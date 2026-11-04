"""Minimal GitHub Issues REST client: the only GitHub-specific module.

A Jira client would implement the same four methods (list_open_tickets,
ensure_labels, create_ticket, add_comment) and return the same Ticket type.
"""

from __future__ import annotations

import logging
import random
import re
import time
from collections.abc import Callable, Collection, Iterator
from dataclasses import dataclass
from typing import Any

import requests

from claims_etl.ingestion.log import log_event
from claims_etl.notifications.config import LabelSpec
from claims_etl.notifications.settings import get_setting

logger = logging.getLogger(__name__)

API_ROOT = "https://api.github.com"
_REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_RETRYABLE_STATUS = {500, 502, 503, 504}


class TicketingError(RuntimeError):
    """A ticketing call failed for good (after any retries). Never contains the token."""


@dataclass(frozen=True)
class Ticket:
    """A ticket as the notifier sees it."""

    number: int
    url: str
    body: str


class GitHubClient:
    """GitHub Issues via REST, with retries on 5xx and rate limits."""

    def __init__(
        self,
        token: str,
        repo: str,
        *,
        session: requests.Session | None = None,
        max_attempts: int = 4,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        timeout: float = 15.0,
        jitter: bool = True,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not _REPO.fullmatch(repo):
            raise ValueError("GITHUB_REPO must look like 'owner/name'")
        self.repo = repo
        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "claims-etl-quarantine-notifier",
            }
        )
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._timeout = timeout
        self._jitter = jitter
        self._sleep = sleep
        self._clock = clock

    @classmethod
    def from_settings(cls) -> GitHubClient:
        """Build from GITHUB_TOKEN and GITHUB_REPO via get_setting()."""
        return cls(get_setting("GITHUB_TOKEN"), get_setting("GITHUB_REPO"))

    def __repr__(self) -> str:  # never expose the session headers (token)
        return f"GitHubClient(repo={self.repo!r})"

    # --- public API ------------------------------------------------------------

    def list_open_tickets(self, label: str) -> Iterator[Ticket]:
        """Open issues with a label, across all pages (pull requests excluded).

        Uses the Issues list endpoint, not search: search is indexed with a
        delay, so a just-created issue could be missed and duplicated.
        """
        url: str | None = f"{API_ROOT}/repos/{self.repo}/issues"
        params: dict[str, Any] | None = {"labels": label, "state": "open", "per_page": 100}
        while url:
            response = self._request("GET", url, params=params)
            for item in response.json():
                if "pull_request" in item:
                    continue
                yield Ticket(
                    number=item["number"], url=item["html_url"], body=item.get("body") or ""
                )
            url = response.links.get("next", {}).get("url")
            params = None  # the "next" URL already carries the query string

    def ensure_labels(self, labels: Collection[LabelSpec]) -> list[str]:
        """Create any labels that do not exist yet; return the names created."""
        existing = {name.lower() for name in self._list_label_names()}
        created = []
        for spec in labels:
            if spec.name.lower() in existing:
                continue
            response = self._request(
                "POST",
                f"{API_ROOT}/repos/{self.repo}/labels",
                json={"name": spec.name, "color": spec.color, "description": spec.description},
                allowed_status={422},  # 422 = created concurrently by someone else
            )
            if response.status_code != 422:
                created.append(spec.name)
                log_event(logger, "label_created", repo=self.repo, label=spec.name)
        return created

    def create_ticket(self, title: str, body: str, labels: list[str]) -> Ticket:
        """Open a new issue."""
        response = self._request(
            "POST",
            f"{API_ROOT}/repos/{self.repo}/issues",
            json={"title": title, "body": body, "labels": labels},
        )
        data = response.json()
        return Ticket(number=data["number"], url=data["html_url"], body=data.get("body") or "")

    def add_comment(self, ticket: Ticket, body: str) -> str:
        """Comment on an issue; return the comment URL."""
        response = self._request(
            "POST",
            f"{API_ROOT}/repos/{self.repo}/issues/{ticket.number}/comments",
            json={"body": body},
        )
        return response.json()["html_url"]

    # --- internals -------------------------------------------------------------------

    def _list_label_names(self) -> Iterator[str]:
        url: str | None = f"{API_ROOT}/repos/{self.repo}/labels"
        params: dict[str, Any] | None = {"per_page": 100}
        while url:
            response = self._request("GET", url, params=params)
            yield from (label["name"] for label in response.json())
            url = response.links.get("next", {}).get("url")
            params = None

    def _request(
        self, method: str, url: str, *, allowed_status: Collection[int] = (), **kwargs: Any
    ) -> requests.Response:
        """Send a request, retrying 5xx, 429 and rate-limited 403 responses.

        Raises TicketingError on a non-retryable error or once attempts run
        out. Messages include method, path and status, never headers.
        """
        path = url.removeprefix(API_ROOT).split("?")[0]
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._session.request(method, url, timeout=self._timeout, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == self._max_attempts:
                    raise TicketingError(
                        f"{method} {path}: {type(exc).__name__} after {attempt} attempts"
                    ) from None
                self._wait(method, path, None, attempt, self._backoff(attempt))
                continue

            if response.ok or response.status_code in allowed_status:
                return response
            if self._is_retryable(response):
                delay = self._retry_delay(response, attempt)
                if delay > self._max_delay:
                    raise TicketingError(
                        f"{method} {path}: rate limited for {delay:.0f}s; retry later"
                    )
                if attempt < self._max_attempts:
                    self._wait(method, path, response.status_code, attempt, delay)
                    continue
            raise TicketingError(
                f"{method} {path} failed: HTTP {response.status_code} "
                f"{_github_message(response)} (attempt {attempt}/{self._max_attempts})"
            )
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _is_retryable(response: requests.Response) -> bool:
        if response.status_code in _RETRYABLE_STATUS or response.status_code == 429:
            return True
        # GitHub signals primary and secondary rate limits with 403 plus these headers.
        return response.status_code == 403 and (
            response.headers.get("x-ratelimit-remaining") == "0"
            or "retry-after" in response.headers
        )

    def _retry_delay(self, response: requests.Response, attempt: int) -> float:
        """Honor Retry-After, then x-ratelimit-reset, else exponential backoff."""
        retry_after = response.headers.get("retry-after")
        if retry_after and retry_after.strip().isdigit():
            return float(retry_after)
        if response.headers.get("x-ratelimit-remaining") == "0":
            reset = response.headers.get("x-ratelimit-reset", "")
            if reset.isdigit():
                return max(0.0, float(reset) - self._clock()) + 1.0
        return self._backoff(attempt)

    def _backoff(self, attempt: int) -> float:
        delay = min(self._max_delay, self._base_delay * 2 ** (attempt - 1))
        return delay * (0.5 + random.random() / 2) if self._jitter else delay

    def _wait(self, method: str, path: str, status: int | None, attempt: int, delay: float) -> None:
        log_event(
            logger,
            "github_retry",
            level=logging.WARNING,
            method=method,
            path=path,
            status=status,
            attempt=attempt,
            delay_seconds=round(delay, 2),
        )
        self._sleep(delay)


def _github_message(response: requests.Response) -> str:
    """GitHub's error 'message' field, if the body is JSON; bounded length."""
    try:
        message = response.json().get("message", "")
    except (ValueError, AttributeError):
        message = ""
    return " ".join(str(message).split())[:200]
