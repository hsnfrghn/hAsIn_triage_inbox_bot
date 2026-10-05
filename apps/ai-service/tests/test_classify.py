import itertools
import json
import logging
from typing import Any, get_args

import pytest

from ai_service.classify import (
    INTENT_TO_PB,
    URGENCY_TO_PB,
    IntentLabel,
    UrgencyLabel,
    to_response,
)
from ai_service.gen.triage.v1 import triage_pb2 as pb

PROMPT_VERSION = "test-v1"
USAGE = pb.Usage(input_tokens=100, output_tokens=20, latency_ms=300)
VALID: dict[str, Any] = {
    "intent": "billing",
    "urgency": "high",
    "sentiment": -0.4,
    "confidence": 0.9,
}


def call(raw: str) -> pb.ClassifyResponse:
    return to_response(raw, prompt_version=PROMPT_VERSION, usage=USAGE)


def valid_with(**overrides: Any) -> str:
    return json.dumps({**VALID, **overrides})


def valid_without(field: str) -> str:
    return json.dumps({k: v for k, v in VALID.items() if k != field})


def test_valid_output_is_mapped() -> None:
    r = call(json.dumps(VALID))

    assert not r.needs_human
    assert r.intent == pb.ClassifyResponse.BILLING
    assert r.urgency == pb.ClassifyResponse.HIGH
    # proto float is 32-bit: -0.4 comes back as -0.4000000059604645
    assert r.sentiment == pytest.approx(-0.4)
    assert r.confidence == pytest.approx(0.9)
    assert r.prompt_version == PROMPT_VERSION
    assert r.usage == USAGE


@pytest.mark.parametrize(
    ("sentiment", "confidence"),
    [(-1, 0), (1, 1), (-1.0, 1.0), (0, 0.5)],
)
def test_boundary_values_are_accepted(sentiment: float, confidence: float) -> None:
    r = call(valid_with(sentiment=sentiment, confidence=confidence))

    assert not r.needs_human
    assert r.sentiment == pytest.approx(sentiment)
    assert r.confidence == pytest.approx(confidence)


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("", id="empty"),
        pytest.param("I'm sorry, I can't help with that.", id="refusal-prose"),
        pytest.param("not json", id="not-json"),
        pytest.param("```json\n" + json.dumps(VALID) + "\n```", id="markdown-fenced"),
        pytest.param('{"intent": "billing"', id="truncated"),
        pytest.param("[]", id="json-array"),
        pytest.param("null", id="json-null"),
        pytest.param(valid_without("intent"), id="missing-intent"),
        pytest.param(valid_without("confidence"), id="missing-confidence"),
        pytest.param(valid_with(reason="looks like billing"), id="extra-field"),
        pytest.param(valid_with(intent="Billing"), id="wrong-case-intent"),
        pytest.param(valid_with(intent="refund"), id="unknown-intent"),
        pytest.param(valid_with(urgency="critical"), id="unknown-urgency"),
        pytest.param(valid_with(intent=1), id="intent-as-number"),
        pytest.param(valid_with(sentiment="0.5"), id="sentiment-as-string"),
        pytest.param(valid_with(sentiment=1.0001), id="sentiment-above-range"),
        pytest.param(valid_with(sentiment=-1.0001), id="sentiment-below-range"),
        pytest.param(valid_with(confidence=1.5), id="confidence-above-range"),
        pytest.param(valid_with(confidence=-0.1), id="confidence-below-range"),
        pytest.param(valid_with(sentiment=None), id="sentiment-null"),
        pytest.param(
            '{"intent": "billing", "urgency": "high", "sentiment": NaN, "confidence": 0.9}',
            id="sentiment-nan",
        ),
        pytest.param(
            '{"intent": "billing", "urgency": "high", "sentiment": 0, "confidence": Infinity}',
            id="confidence-infinity",
        ),
    ],
)
def test_unusable_output_fails_closed(raw: str) -> None:
    r = call(raw)

    assert r.needs_human
    assert r.intent == pb.ClassifyResponse.INTENT_UNSPECIFIED
    assert r.urgency == pb.ClassifyResponse.URGENCY_UNSPECIFIED
    assert r.prompt_version == PROMPT_VERSION
    assert r.usage == USAGE


def test_intent_mapping_is_complete_both_ways() -> None:
    assert set(INTENT_TO_PB) == set(get_args(IntentLabel))
    proto_values = {
        v.number for v in pb.ClassifyResponse.Intent.DESCRIPTOR.values if v.number != 0
    }
    assert set(INTENT_TO_PB.values()) == proto_values


def test_urgency_mapping_is_complete_both_ways() -> None:
    assert set(URGENCY_TO_PB) == set(get_args(UrgencyLabel))
    proto_values = {
        v.number for v in pb.ClassifyResponse.Urgency.DESCRIPTOR.values if v.number != 0
    }
    assert set(URGENCY_TO_PB.values()) == proto_values


@pytest.mark.parametrize(
    ("intent", "urgency"),
    list(itertools.product(get_args(IntentLabel), get_args(UrgencyLabel))),
)
def test_valid_output_never_leaves_enums_unspecified(intent: str, urgency: str) -> None:
    """PLAN §3: needs_human == false implies intent and urgency are set."""
    r = call(valid_with(intent=intent, urgency=urgency))

    assert not r.needs_human
    assert r.intent != pb.ClassifyResponse.INTENT_UNSPECIFIED
    assert r.urgency != pb.ClassifyResponse.URGENCY_UNSPECIFIED


def test_rejection_is_logged_without_model_output(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="ai_service.classify")
    secret = "jane.doe@example.com"

    r = call(valid_with(intent=secret))

    assert r.needs_human
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING
    # check the message and every structured field, not just the formatted text
    assert secret not in caplog.text
    assert secret not in repr(record.__dict__)
