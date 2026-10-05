import logging
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ai_service.gen.triage.v1 import triage_pb2 as pb

log = logging.getLogger(__name__)

IntentLabel = Literal["billing", "bug", "how_to", "cancellation", "feedback", "other"]
UrgencyLabel = Literal["low", "normal", "high"]


class ClassificationOut(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)

    intent: IntentLabel
    urgency: UrgencyLabel
    sentiment: Annotated[float, Field(ge=-1, le=1)]
    confidence: Annotated[float, Field(ge=0, le=1)]


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
