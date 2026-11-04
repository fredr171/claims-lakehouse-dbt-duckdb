"""Ticket title/body/comment text. Pure functions with no tracker or network dependency.

The repo is public, so check details are sanitized rather than trusted:
- quoted values inside CSV check details are kept only if they look like
  column identifiers. A file without a header row would otherwise publish its
  first data row as "unexpected columns", and csv_utf8 would publish raw bytes.
- every value is rendered inside a code span, so text such as "@someone",
  links, or markdown in file or column names cannot ping users or inject markup.
- long details are truncated.
"""

from __future__ import annotations

import ast
import re
import string
from urllib.parse import quote

from claims_etl.notifications.config import NotificationConfig
from claims_etl.notifications.report import QuarantineReport

CHECKSUM_LINE = re.compile(r"^Checksum: `([0-9a-f]{64})`$", re.MULTILINE)
REDACTED = "<redacted>"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_PUNCTUATION_TOKEN = re.compile(r"^[^A-Za-z0-9]{1,2}$")  # e.g. ',' or '"' in csv errors
# A Python repr'd string or bytes literal: 'x', "x", b'x', b"x" (with escapes).
_QUOTED_LITERAL = re.compile(r"""b?'(?:[^'\\]|\\.)*'|b?"(?:[^"\\]|\\.)*\"""")


def checksum_line(checksum: str) -> str:
    """The exact body line used to find an existing ticket for a checksum."""
    return f"Checksum: `{checksum}`"


def find_checksum(body: str | None) -> str | None:
    """The checksum recorded in a ticket body, if any."""
    match = CHECKSUM_LINE.search(body or "")
    return match.group(1) if match else None


def code(value: object, *, in_table: bool = False) -> str:
    """Render a value as an inline code span that cannot break out of itself."""
    text = " ".join(str(value).split())  # collapse newlines and tabs
    text = text.replace("`", "'")
    if in_table:
        text = text.replace("|", "\\|")
    return f"`{text}`" if text else "`-`"


def _redact_quoted(text: str) -> tuple[str, int]:
    """Replace quoted literals that are not identifiers or punctuation; return (text, count)."""
    redacted = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal redacted
        literal = match.group(0)
        try:
            value = ast.literal_eval(literal)
        except (ValueError, SyntaxError):
            value = None
        if isinstance(value, str) and (
            _IDENTIFIER.fullmatch(value) or _PUNCTUATION_TOKEN.fullmatch(value)
        ):
            return literal
        redacted += 1
        return REDACTED

    return _QUOTED_LITERAL.sub(replace, text), redacted


def truncate(text: str, max_chars: int) -> str:
    """Cut text to max_chars, saying so."""
    if len(text) <= max_chars:
        return text
    suffix = f"... [truncated, {len(text)} chars total]"
    return text[: max(0, max_chars - len(suffix))] + suffix


def sanitize_details(check: str | None, details: str, max_chars: int) -> str:
    """Make check details safe for a public ticket: no row values, bounded length.

    CSV checks are the ones whose details can echo file *content*, so their
    quoted literals are filtered. File and archive checks only mention file
    and entry names (metadata), which are kept.
    """
    text = details or ""
    if (check or "").startswith("csv_"):
        text, count = _redact_quoted(text)
        if count:
            text += f" ({count} non-identifier value(s) redacted)"
    return truncate(text, max_chars)


class _BlankMissing(dict):
    """format_map helper: missing or None fields render as '?'."""

    def __missing__(self, key: str) -> str:
        return "?"


def build_title(report: QuarantineReport, config: NotificationConfig) -> str:
    """Title from the config template; falls back when the name gave no year/sample."""
    has_partition = bool(report.year and report.sample)
    template = config.title_template if has_partition else config.title_template_no_partition
    fields = {
        "file_type": report.file_type or "unknown",
        "year": report.year,
        "sample": report.sample,
        "zip_name": report.zip_name,
        "failed_check": report.failed_check or "unknown_check",
    }
    values = _BlankMissing({k: v for k, v in fields.items() if v is not None})
    title = string.Formatter().vformat(template, (), values)
    return truncate(" ".join(title.split()), config.max_title_chars)


def s3_console_url(s3_uri: str, region: str) -> str:
    """AWS console link for an object (only works for someone signed in to the account)."""
    bucket, _, key = s3_uri.removeprefix("s3://").partition("/")
    return (
        f"https://{region}.console.aws.amazon.com/s3/object/{bucket}"
        f"?region={region}&prefix={quote(key, safe='')}"
    )


def error_json_uri(report: QuarantineReport, config: NotificationConfig) -> str:
    """s3:// URI of the report itself."""
    return f"s3://{config.landing_bucket}/{report.error_key}"


def build_body(report: QuarantineReport, config: NotificationConfig) -> str:
    """Markdown body for a new ticket."""
    file_type = report.file_type or "unknown"
    details = sanitize_details(
        report.failed_check, report.failure_details, config.max_details_chars
    )
    report_uri = error_json_uri(report, config)
    check_lines = "\n".join(
        f"- {'PASS' if c.passed else '**FAIL**'} {code(c.name)}" for c in report.checks
    )
    rows = [
        ("File type", code(file_type, in_table=True)),
        ("Zip", code(report.zip_name, in_table=True)),
        ("Source URI", code(report.source_uri, in_table=True)),
        ("Quarantine location", code(report.quarantine_uri or "-", in_table=True)),
        (
            "Error report",
            f"{code(report_uri, in_table=True)} "
            f"([S3 console]({s3_console_url(report_uri, config.region)}))",
        ),
        ("Batch ID", code(report.batch_id, in_table=True)),
        ("Quarantined at (UTC)", code(report.quarantined_at or "-", in_table=True)),
    ]
    table = "\n".join(f"| {label} | {value} |" for label, value in rows)
    return "\n".join(
        [
            "## Quarantined file",
            "",
            "| | |",
            "|---|---|",
            table,
            "",
            checksum_line(report.checksum),
            "",
            f"## Failed check: {code(report.failed_check or 'unknown')}",
            "",
            code(details) if details else "_No details recorded._",
            "",
            "## Suggested next steps",
            "",
            config.next_step_for(report.failed_check, file_type),
            "",
            "<details><summary>All checks (names only)</summary>",
            "",
            check_lines or "_none recorded_",
            "",
            "</details>",
            "",
            "---",
            "_Opened automatically by the claims-etl quarantine notifier. "
            "Row-level data values are never included in tickets._",
        ]
    )


def build_comment(report: QuarantineReport, config: NotificationConfig) -> str:
    """Markdown comment for a repeat quarantine of the same checksum."""
    details = sanitize_details(
        report.failed_check, report.failure_details, config.max_details_chars
    )
    report_uri = error_json_uri(report, config)
    rows = [
        ("Quarantined at (UTC)", code(report.quarantined_at or "-", in_table=True)),
        ("Batch ID", code(report.batch_id, in_table=True)),
        ("Zip", code(report.zip_name, in_table=True)),
        ("Failed check", code(report.failed_check or "unknown", in_table=True)),
        ("Details", code(details or "-", in_table=True)),
        ("Error report", code(report_uri, in_table=True)),
    ]
    table = "\n".join(f"| {label} | {value} |" for label, value in rows)
    return "\n".join(
        [
            "### Quarantined again",
            "",
            "The same file (same checksum) was delivered and quarantined again.",
            "",
            "| | |",
            "|---|---|",
            table,
        ]
    )
