"""OpenAIProvider against a fake HTTP transport: no network, no quota.

The SDK (openai 3.x) runs on httpx2, so httpx2.MockTransport stands in for the
API. Each test scripts the HTTP responses and checks what our adapter turns
them into. One opt-in test at the end talks to the real endpoint in .env.
"""

import asyncio
import json
import os
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx2
import pytest
from openai import AsyncOpenAI

from ai_service import classify
from ai_service.gen.triage.v1 import triage_pb2 as pb
from ai_service.providers.base import (
    Prompt,
    ProviderError,
    ProviderRateLimited,
    ProviderTimeout,
    ProviderUnavailable,
    Usage,
)
from ai_service.providers.openai_provider import OpenAIProvider

PROMPT = Prompt(system="system text", user="<message_to_classify>hi</message_to_classify>")
SCHEMA = classify.ClassificationOut
VALID = '{"intent":"bug","urgency":"high","sentiment":-0.5,"confidence":0.9}'


def completion(content: str | None = VALID, *, refusal: str | None = None) -> httpx2.Response:
    return httpx2.Response(
        200,
        json={
            "id": "c-1",
            "object": "chat.completion",
            "created": 0,
            "model": "m",
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "stop",
                    "message": {"role": "assistant", "content": content, "refusal": refusal},
                }
            ],
            "usage": {"prompt_tokens": 800, "completion_tokens": 60, "total_tokens": 860},
        },
    )


def api_error(status: int, code: str | None, **extra: Any) -> httpx2.Response:
    return httpx2.Response(
        status, json={"error": {"message": "upstream message", "type": "t", "code": code, **extra}}
    )


Step = httpx2.Response | Exception


class Api:
    """Plays back scripted steps (the last one repeats) and records each request."""

    def __init__(self, *steps: Step) -> None:
        self.steps = steps
        self.requests: list[dict[str, Any]] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        step = self.steps[min(len(self.requests), len(self.steps) - 1)]
        self.requests.append(json.loads(request.content))
        if isinstance(step, Exception):
            raise step
        return step


def provider(
    handler: Callable[[httpx2.Request], Any], *, max_retries: int = 2, **kwargs: Any
) -> OpenAIProvider:
    client = AsyncOpenAI(
        api_key="test-key",
        base_url="http://api.test/v1",
        max_retries=max_retries,
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)),
    )
    return OpenAIProvider(client, model=kwargs.pop("model", "test-model"), **kwargs)


@pytest.fixture
def backoff(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[float]]:
    """Make retry sleeps instant, and record how long each one would have been.

    This patches asyncio.sleep for the whole test, which is safe here: with the
    SDK's retries off, the adapter's backoff is the only sleep on these paths.
    """
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    yield delays


async def complete(p: OpenAIProvider, timeout: float | None = 5) -> tuple[str, Usage]:
    return await p.complete_structured(PROMPT, SCHEMA, timeout=timeout)


# --- the request we send ---------------------------------------------------------------


async def test_request_uses_strict_json_schema_and_both_roles() -> None:
    api = Api(completion())
    await complete(provider(api, model="openai/gpt-oss-20b"))

    [body] = api.requests
    assert body["model"] == "openai/gpt-oss-20b"
    assert body["messages"] == [
        {"role": "system", "content": PROMPT.system},
        {"role": "user", "content": PROMPT.user},
    ]
    assert body["response_format"] == {
        "type": "json_schema",
        "json_schema": {
            "name": "ClassificationOut",
            "schema": SCHEMA.model_json_schema(),
            "strict": True,
        },
    }


async def test_reasoning_effort_is_omitted_unless_set() -> None:
    unset, low = Api(completion()), Api(completion())
    await complete(provider(unset))
    await complete(provider(low, reasoning_effort="low"))

    assert "reasoning_effort" not in unset.requests[0]
    assert low.requests[0]["reasoning_effort"] == "low"


# --- answers -------------------------------------------------------------------------


async def test_answer_returns_raw_text_and_usage() -> None:
    assert await complete(provider(Api(completion()))) == (VALID, Usage(800, 60))


@pytest.mark.parametrize(
    "response",
    [
        pytest.param(completion(None, refusal="I can't help with that."), id="refusal"),
        pytest.param(completion(None), id="null-content"),
        pytest.param(completion(""), id="empty-content"),
    ],
)
async def test_no_content_becomes_empty_raw(response: httpx2.Response) -> None:
    raw, usage = await complete(provider(Api(response)))

    assert raw == ""
    assert usage == Usage(800, 60)


async def test_schema_violation_returns_the_failed_generation() -> None:
    """Groq rejects off-schema output with a 400; it's a bad answer, not an outage."""
    bad = '{"intent":"how_to","urgency":"normal","sentiment":0.0,"confidence":"0.9"}'
    api = Api(api_error(400, "json_validate_failed", failed_generation=bad))

    assert await complete(provider(api)) == (bad, Usage(0, 0))
    assert len(api.requests) == 1


async def test_schema_violation_without_failed_generation_is_empty_raw() -> None:
    assert await complete(provider(Api(api_error(400, "json_validate_failed")))) == (
        "",
        Usage(0, 0),
    )


@pytest.mark.parametrize(
    "response",
    [
        pytest.param(completion(None, refusal="no"), id="refusal"),
        pytest.param(
            api_error(400, "json_validate_failed", failed_generation='{"confidence":"0.9"}'),
            id="schema-violation",
        ),
    ],
)
async def test_unusable_answers_fail_closed_through_the_classify_gate(
    response: httpx2.Response,
) -> None:
    raw, _ = await complete(provider(Api(response)))

    r = classify.to_response(raw, prompt_version=classify.VERSION, usage=pb.Usage())
    assert r.needs_human


# --- errors that must not be retried -------------------------------------------------


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        pytest.param(api_error(429, "rate_limit_exceeded"), ProviderRateLimited, id="rate-limit"),
        pytest.param(api_error(429, "insufficient_quota"), ProviderError, id="out-of-credit"),
        pytest.param(api_error(400, "invalid_value"), ProviderError, id="other-400"),
        pytest.param(api_error(401, "invalid_api_key"), ProviderError, id="bad-key"),
        pytest.param(api_error(403, None), ProviderError, id="forbidden"),
        pytest.param(api_error(404, "model_not_found"), ProviderError, id="unknown-model"),
    ],
)
async def test_non_retryable_errors_map_and_are_tried_once(
    response: httpx2.Response, expected: type[ProviderError], backoff: list[float]
) -> None:
    api = Api(response)
    with pytest.raises(ProviderError) as info:
        await complete(provider(api))

    # exact type: out-of-credit must NOT become ProviderRateLimited (the worker
    # would back off and retry forever), and nothing may become Unavailable
    assert type(info.value) is expected
    assert len(api.requests) == 1
    assert backoff == []


async def test_provider_message_is_not_in_our_error() -> None:
    with pytest.raises(ProviderError) as info:
        await complete(provider(Api(api_error(429, "rate_limit_exceeded"))))

    assert "upstream message" not in str(info.value)


# --- transient errors: our retry layer -----------------------------------------------


@pytest.mark.parametrize(
    "first",
    [
        pytest.param(api_error(500, None), id="500"),
        pytest.param(api_error(503, None), id="503"),
        pytest.param(httpx2.ConnectError("connection reset"), id="connection-error"),
    ],
)
async def test_transient_error_is_retried(first: Step, backoff: list[float]) -> None:
    api = Api(first, completion())

    assert await complete(provider(api)) == (VALID, Usage(800, 60))
    assert len(api.requests) == 2
    assert backoff == [0.25]


@pytest.mark.parametrize(
    "step",
    [
        pytest.param(api_error(502, None), id="5xx"),
        pytest.param(httpx2.ConnectError("down"), id="connection-error"),
    ],
)
async def test_persistent_transient_error_gives_up_as_unavailable(
    step: Step, backoff: list[float]
) -> None:
    api = Api(step)
    with pytest.raises(ProviderUnavailable):
        await complete(provider(api))

    assert len(api.requests) == 3
    assert backoff == [0.25, 0.5]


async def test_sdk_retries_are_off_whatever_the_client_says(backoff: list[float]) -> None:
    """One owner per retry layer: 3 attempts total, not 3 x (1 + SDK retries)."""
    api = Api(api_error(500, None))
    with pytest.raises(ProviderUnavailable):
        await complete(provider(api, max_retries=5))

    assert len(api.requests) == 3


async def test_max_attempts_one_means_no_retry(backoff: list[float]) -> None:
    api = Api(api_error(500, None))
    with pytest.raises(ProviderUnavailable):
        await complete(provider(api, max_attempts=1))

    assert len(api.requests) == 1
    assert backoff == []


# --- the caller's deadline -------------------------------------------------------------


async def test_slow_api_hits_the_deadline() -> None:
    async def slow(request: httpx2.Request) -> httpx2.Response:
        await asyncio.sleep(10)
        return completion()

    started = time.monotonic()
    with pytest.raises(ProviderTimeout):
        # outer limit: if the adapter ignored the deadline, fail fast instead of hanging
        await asyncio.wait_for(complete(provider(slow), timeout=0.2), timeout=3)

    assert time.monotonic() - started < 1


async def test_retries_never_outlast_the_deadline() -> None:
    """Real backoff sleeps (0.25, 0.5, 1, ...) against a 0.6 s deadline."""
    api = Api(api_error(500, None))
    started = time.monotonic()
    with pytest.raises(ProviderTimeout):
        await asyncio.wait_for(complete(provider(api, max_attempts=10), timeout=0.6), timeout=3)

    assert time.monotonic() - started < 1
    assert 1 < len(api.requests) < 10


# --- opt-in: the real endpoint from .env -------------------------------------------------

ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


def env_settings() -> dict[str, str]:
    pairs = (
        line.split("=", 1)
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    )
    return {k.strip(): v.strip() for k, v in pairs}


@pytest.mark.skipif(
    os.environ.get("AI_LIVE_TESTS") != "1" or not ENV_FILE.exists(),
    reason="live API test: run with AI_LIVE_TESTS=1 (uses the key in .env)",
)
async def test_live_endpoint_classifies_a_clear_bug_report() -> None:
    env = env_settings()
    client = AsyncOpenAI(api_key=env["OPENAI_API_KEY"], base_url=env.get("OPENAI_BASE_URL"))
    p = OpenAIProvider(client, model=env["OPENAI_MODEL"])
    request = pb.ClassifyRequest(
        message_id="live-1",
        thread=[
            pb.ThreadMessage(
                author=pb.ThreadMessage.CUSTOMER,
                text="Every time I click Save on the invoice page I get 'Error 500' "
                "and my changes are lost. This started after yesterday's update.",
            )
        ],
    )

    raw, usage = await p.complete_structured(classify.build(request), SCHEMA, timeout=30)
    r = classify.to_response(raw, prompt_version=classify.VERSION, usage=pb.Usage())

    assert not r.needs_human, raw
    assert r.intent == pb.ClassifyResponse.BUG
    assert usage.input_tokens > 0 and usage.output_tokens > 0
