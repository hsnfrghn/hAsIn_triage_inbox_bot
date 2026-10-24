import hashlib
import html
import logging
from importlib.resources import files
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ai_service.gen.triage.v1 import triage_pb2 as pb
from ai_service.providers.base import Prompt

log = logging.getLogger(__name__)

IntentLabel = Literal["billing", "bug", "how_to", "cancellation", "feedback", "other"]
UrgencyLabel = Literal["low", "normal", "high"]

_FILE = "v1"
_TEMPLATE = (files("ai_service") / "prompts" / "classify" / f"{_FILE}.md").read_text(encoding="utf-8")
VERSION = f"classify-{_FILE}-{hashlib.sha256(_TEMPLATE.encode()).hexdigest()[:8]}"

_AUTHOR = {pb.ThreadMessage.CUSTOMER: "customer", pb.ThreadMessage.AGENT: "agent"}

class ClassificationOut(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)

    intent: IntentLabel
    urgency: UrgencyLabel
    sentiment: Annotated[float, Field(ge=-1, le=1)]
    confidence: Annotated[float, Field(ge=0, le=1)]

class InvalidRequest(ValueError):
    """The caller sent something we can't classify -> INVALID_ARGUMENT."""

INTENT_TO_PB: dict[IntentLabel, pb.ClassifyResponse.Intent] = {
    "billing": pb.ClassifyResponse.BILLING,
    "bug": pb.ClassifyResponse.BUG,
    "cancellation": pb.ClassifyResponse.CANCELLATION,
    "feedback": pb.ClassifyResponse.FEEDBACK,
    "how_to": pb.ClassifyResponse.HOW_TO,
    "other": pb.ClassifyResponse.OTHER,
}

URGENCY_TO_PB: dict[UrgencyLabel, pb.ClassifyResponse.Urgency] = {
    "low": pb.ClassifyResponse.LOW,
    "normal": pb.ClassifyResponse.NORMAL,
    "high": pb.ClassifyResponse.HIGH
}


def to_response(raw: str, *, prompt_version: str, usage: pb.Usage) -> pb.ClassifyResponse:
    try:
        out = ClassificationOut.model_validate_json(raw)
    except ValidationError as e:
        log.warning(
            "classification output rejected",
            extra={"errors": [(err["type"], err["loc"]) for err in e.errors(include_input=False)],
                   "raw_len": len(raw)},
        )
        return pb.ClassifyResponse(
            needs_human=True, prompt_version=prompt_version, usage=usage,
        )
    return pb.ClassifyResponse(
        intent=INTENT_TO_PB[out.intent],
        urgency=URGENCY_TO_PB[out.urgency],
        sentiment=out.sentiment,
        confidence=out.confidence,
        prompt_version=prompt_version,
        usage=usage,
        needs_human=False,
    )

def _render(m: pb.ThreadMessage) -> str:
    author = _AUTHOR.get(m.author)
    if author is None:                                   # AUTHOR_UNSPECIFIED = caller bug
        raise InvalidRequest("thread message has no author")
    return f'<message author="{author}">\n{html.escape(m.text, quote=False)}\n</message>'


def build(request: pb.ClassifyRequest) -> Prompt:
    if not request.thread:
        raise InvalidRequest("thread is empty")
    *history, target = request.thread                    # proto: thread ends at the message to classify
    if target.author != pb.ThreadMessage.CUSTOMER or not target.text.strip():
        raise InvalidRequest("last message must be a non-empty customer message")
    parts = []
    if history:
        parts.append("<thread>\n" + "\n".join(_render(m) for m in history) + "\n</thread>")
    parts.append("<message_to_classify>\n" + _render(target) + "\n</message_to_classify>")
    return Prompt(system=_TEMPLATE, user="\n\n".join(parts))
