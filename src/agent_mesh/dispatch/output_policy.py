"""Safe stdout-to-response extraction policy for dispatch launches.

This module is deliberately pure: it does not append RES messages, mutate events, or persist output.
It only classifies whether a completed process result contains an explicitly fenced response body that
future posting code may hand to the normal RES append path.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from .types import AgentLaunchResult

RESPONSE_BEGIN = "AGENT_MESH_RESPONSE_BEGIN"
RESPONSE_END = "AGENT_MESH_RESPONSE_END"
DEFAULT_MAX_BODY_CHARS = 20_000
_OSC_RE = re.compile(r"(?:\x1b\]|\x9d).*?(?:\x07|\x1b\\|\x9c)", re.DOTALL)
_UNTERMINATED_OSC_LINE_RE = re.compile(r"(?:\x1b\]|\x9d)[^\n]*(?=\n|$)")
_STRING_CONTROL_RE = re.compile(r"(?:\x1b[P_X^]|[\x90\x98\x9e\x9f]).*?(?:\x1b\\|\x9c)", re.DOTALL)
_UNTERMINATED_STRING_CONTROL_LINE_RE = re.compile(r"(?:\x1b[P_X^]|[\x90\x98\x9e\x9f])[^\n]*(?=\n|$)")
_CSI_RE = re.compile(r"(?:\x1b\[|\x9b)[0-?]*[ -/]*[@-~]")
_ESC_RE = re.compile(r"\x1b[@-Z\\-_]|\x1b[ -/]*[@-~]")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")


@dataclass(frozen=True)
class ResponseCandidateDecision:
    status: str
    reason: str
    body: str = ""
    summary: str = ""
    safe_detail: str = ""


_RESPONSE_RE = re.compile(
    rf"^{re.escape(RESPONSE_BEGIN)}[ \t]*\n(?P<body>.*?)\n?{re.escape(RESPONSE_END)}[ \t]*$",
    re.DOTALL | re.MULTILINE,
)


def extract_response_candidate(
    result: AgentLaunchResult, *, max_body_chars: int = DEFAULT_MAX_BODY_CHARS
) -> ResponseCandidateDecision:
    """Return a safe, explicit response candidate from process stdout.

    Policy:
    - only successful completed launches are eligible;
    - stdout must contain exactly one begin/end fenced response block;
    - stderr, metadata, raw prompt, and unfenced logs are never copied;
    - ANSI escape sequences are stripped and CRLF is normalized;
    - empty or over-limit bodies are rejected without echoing body content.
    """

    if result.status != "completed" or result.exit_code != 0:
        return ResponseCandidateDecision(
            status="rejected",
            reason="launch_not_completed",
            safe_detail=f"status={result.status} exit_code={result.exit_code}",
        )

    stdout_raw = result.stdout.replace("\r\n", "\n").replace("\r", "\n")
    if _has_unterminated_terminal_string(stdout_raw):
        return ResponseCandidateDecision(
            status="rejected",
            reason="unterminated_terminal_control",
            safe_detail="unterminated terminal string control detected",
        )
    stdout = _normalize_stdout(stdout_raw)
    begin_count = stdout.count(RESPONSE_BEGIN)
    end_count = stdout.count(RESPONSE_END)
    if begin_count == 0 and end_count == 0:
        return ResponseCandidateDecision(
            status="rejected",
            reason="missing_response_markers",
            safe_detail="stdout did not contain explicit response markers",
        )
    if begin_count != 1 or end_count != 1:
        return ResponseCandidateDecision(
            status="rejected",
            reason="ambiguous_response_markers",
            safe_detail=f"begin_markers={begin_count} end_markers={end_count}",
        )

    match = _RESPONSE_RE.search(stdout)
    if match is None:
        return ResponseCandidateDecision(
            status="rejected",
            reason="ambiguous_response_markers",
            safe_detail="response markers were malformed",
        )

    body = match.group("body").strip()
    if not body:
        return ResponseCandidateDecision(
            status="rejected",
            reason="empty_response_body",
            safe_detail="response body was empty",
        )
    if len(body) > max_body_chars:
        return ResponseCandidateDecision(
            status="rejected",
            reason="response_body_too_large",
            safe_detail=f"body_chars={len(body)} max_body_chars={max_body_chars}",
        )

    return ResponseCandidateDecision(status="accepted", reason="ok", body=body, summary=_summary(body))


def _has_unterminated_terminal_string(stdout: str) -> bool:
    without_terminated = _STRING_CONTROL_RE.sub("", _OSC_RE.sub("", stdout))
    return bool(re.search(r"(?:\x1b\]|\x9d|\x1b[P_X^]|[\x90\x98\x9e\x9f])", without_terminated))


def _normalize_stdout(stdout: str) -> str:
    normalized = stdout.replace("\r\n", "\n").replace("\r", "\n")
    without_osc = _OSC_RE.sub("", normalized)
    without_unterminated_osc = _UNTERMINATED_OSC_LINE_RE.sub("", without_osc)
    without_strings = _STRING_CONTROL_RE.sub("", without_unterminated_osc)
    without_unterminated_strings = _UNTERMINATED_STRING_CONTROL_LINE_RE.sub("", without_strings)
    without_csi = _CSI_RE.sub("", without_unterminated_strings)
    without_esc = _ESC_RE.sub("", without_csi)
    return _CONTROL_RE.sub("", without_esc)


def _summary(body: str) -> str:
    for line in body.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:120]
    return ""
