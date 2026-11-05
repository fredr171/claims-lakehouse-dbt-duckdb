"""GitHubClient against a mocked API (responses): pagination, labels, retries, secrecy."""

from __future__ import annotations

import logging

import pytest
import responses

from claims_etl.notifications.config import LabelSpec
from claims_etl.notifications.github_client import (
    API_ROOT,
    GitHubClient,
    Ticket,
    TicketingError,
)

TOKEN = "github_pat_TEST_SECRET_DO_NOT_LOG_123"
REPO = "owner/repo"
ISSUES = f"{API_ROOT}/repos/{REPO}/issues"
LABELS = f"{API_ROOT}/repos/{REPO}/labels"


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def client(sleeps: list[float]) -> GitHubClient:
    return GitHubClient(TOKEN, REPO, jitter=False, sleep=sleeps.append, clock=lambda: 1_000.0)


@pytest.fixture
def api():
    with responses.RequestsMock() as rsps:  # unmatched requests raise: no real network
        yield rsps


def test_sends_auth_and_api_version_headers(client, api) -> None:
    api.get(ISSUES, json=[])
    list(client.list_open_tickets("quarantine"))
    sent = api.calls[0].request
    assert sent.headers["Authorization"] == f"Bearer {TOKEN}"
    assert sent.headers["X-GitHub-Api-Version"] == "2022-11-28"
    assert "labels=quarantine" in sent.url and "state=open" in sent.url


def test_list_follows_pagination_and_skips_pull_requests(client, api) -> None:
    page2 = f"{ISSUES}?labels=quarantine&state=open&per_page=100&page=2"
    api.get(
        ISSUES,
        json=[
            {"number": 1, "html_url": "u1", "body": "a"},
            {"number": 2, "html_url": "u2", "pull_request": {}},
        ],
        headers={"Link": f'<{page2}>; rel="next"'},
        match=[
            responses.matchers.query_param_matcher(
                {"labels": "quarantine", "state": "open", "per_page": "100"}
            )
        ],
    )
    api.get(page2, json=[{"number": 3, "html_url": "u3", "body": None}])
    tickets = list(client.list_open_tickets("quarantine"))
    assert [t.number for t in tickets] == [1, 3]
    assert tickets[1].body == ""


def test_ensure_labels_creates_only_missing(client, api) -> None:
    api.get(LABELS, json=[{"name": "Quarantine"}])  # case-insensitive match
    api.post(LABELS, status=201, json={})
    api.post(LABELS, status=201, json={})
    created = client.ensure_labels(
        [
            LabelSpec("quarantine", "d73a4a", "q"),
            LabelSpec("data-quality", "fbca04", "dq"),
            LabelSpec("claims_header", "c5def5", "ft"),
        ]
    )
    assert created == ["data-quality", "claims_header"]
    posted = [call.request.body for call in api.calls if call.request.method == "POST"]
    assert b'"color": "fbca04"' in posted[0]


def test_ensure_labels_tolerates_concurrent_creation(client, api) -> None:
    api.get(LABELS, json=[])
    api.post(LABELS, status=422, json={"message": "Validation Failed"})
    assert client.ensure_labels([LabelSpec("quarantine", "d73a4a", "q")]) == []


def test_5xx_is_retried_with_backoff_then_succeeds(client, api, sleeps) -> None:
    api.post(ISSUES, status=502)
    api.post(ISSUES, status=503)
    api.post(ISSUES, status=201, json={"number": 9, "html_url": "https://x/9", "body": "b"})
    ticket = client.create_ticket("t", "b", ["quarantine"])
    assert ticket == Ticket(number=9, url="https://x/9", body="b")
    assert sleeps == [1.0, 2.0]  # exponential backoff (jitter off)


def test_5xx_gives_up_after_max_attempts(client, api, sleeps) -> None:
    for _ in range(4):
        api.post(ISSUES, status=503, json={"message": "Service Unavailable"})
    with pytest.raises(TicketingError, match="HTTP 503.*attempt 4/4"):
        client.create_ticket("t", "b", [])
    assert sleeps == [1.0, 2.0, 4.0]


def test_retry_after_header_is_honored(client, api, sleeps) -> None:
    api.post(ISSUES, status=429, headers={"Retry-After": "7"})
    api.post(ISSUES, status=201, json={"number": 1, "html_url": "u", "body": ""})
    client.create_ticket("t", "b", [])
    assert sleeps == [7.0]


def test_rate_limit_reset_is_honored(client, api, sleeps) -> None:
    headers = {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1010"}  # clock = 1000
    api.post(ISSUES, status=403, headers=headers)
    api.post(ISSUES, status=201, json={"number": 1, "html_url": "u", "body": ""})
    client.create_ticket("t", "b", [])
    assert sleeps == [11.0]


def test_long_rate_limit_fails_fast_for_later_retry(client, api, sleeps) -> None:
    api.post(
        ISSUES, status=403, headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "4600"}
    )
    with pytest.raises(TicketingError, match="rate limited .* retry later"):
        client.create_ticket("t", "b", [])
    assert sleeps == []


def test_auth_error_is_not_retried_and_token_never_leaks(client, api, sleeps, caplog) -> None:
    caplog.set_level(logging.DEBUG)
    api.post(ISSUES, status=401, json={"message": "Bad credentials"})
    with pytest.raises(TicketingError) as excinfo:
        client.create_ticket("t", "b", [])
    assert "401" in str(excinfo.value) and "Bad credentials" in str(excinfo.value)
    assert sleeps == []
    assert TOKEN not in str(excinfo.value)
    assert TOKEN not in caplog.text
    assert TOKEN not in repr(client)


def test_plain_403_without_rate_limit_headers_is_not_retried(client, api, sleeps) -> None:
    api.post(
        ISSUES, status=403, json={"message": "Resource not accessible by personal access token"}
    )
    with pytest.raises(TicketingError, match="403"):
        client.create_ticket("t", "b", [])
    assert sleeps == []


def test_repo_must_be_owner_slash_name() -> None:
    with pytest.raises(ValueError, match="owner/name"):
        GitHubClient(TOKEN, "not-a-repo")
