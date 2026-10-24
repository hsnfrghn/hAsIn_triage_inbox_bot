"""In-process gRPC tests: a real grpc.aio server and a real client stub.

These go through the wire on purpose. They catch what unit tests and mypy can't:
a misspelled servicer method (the generated base class answers UNIMPLEMENTED),
status codes as the client actually sees them, and deadline propagation.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import grpc
import pytest
from pydantic import BaseModel

from ai_service import classify
from ai_service.gen.triage.v1 import triage_pb2 as pb
from ai_service.gen.triage.v1 import triage_pb2_grpc as pb_grpc
from ai_service.providers import LLMProvider
from ai_service.providers.base import (
    Prompt,
    ProviderError,
    ProviderRateLimited,
    ProviderTimeout,
    ProviderUnavailable,
    Usage,
)
from ai_service.providers.fake import FakeProvider
from ai_service.servicer import TriageServicer

CUSTOMER = pb.ThreadMessage.CUSTOMER


def classify_request(text: str = "I was charged twice this month") -> pb.ClassifyRequest:
    return pb.ClassifyRequest(
        message_id="m-1",
        org_id="o-1",
        thread=[pb.ThreadMessage(author=CUSTOMER, text=text)],
    )


@asynccontextmanager
async def serve(llm: LLMProvider) -> AsyncIterator[Any]:
    """Start the servicer on a free local port and yield a connected stub."""
    server = grpc.aio.server()
    pb_grpc.add_TriageServiceServicer_to_server(TriageServicer(llm), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            yield pb_grpc.TriageServiceStub(channel)
    finally:
        await server.stop(None)


class SpyProvider(FakeProvider):
    """A FakeProvider that also records the timeout it got and when it stopped."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.timeouts: list[float | None] = []
        self.exited = asyncio.Event()

    async def complete_structured(
        self, prompt: Prompt, schema: type[BaseModel], timeout: float | None
    ) -> tuple[str, Usage]:
        self.timeouts.append(timeout)
        try:
            return await super().complete_structured(prompt, schema, timeout)
        finally:
            self.exited.set()


async def rpc_error(call: Any) -> grpc.aio.AioRpcError:
    with pytest.raises(grpc.aio.AioRpcError) as info:
        await call
    return info.value


# --- GetInfo -------------------------------------------------------------------


async def test_get_info_reports_version_and_loaded_provider() -> None:
    async with serve(FakeProvider()) as stub:
        info = await stub.GetInfo(pb.GetInfoRequest(), timeout=5)

    assert info.classify_prompt_version == classify.VERSION
    assert info.provider == "fake"
    assert info.draft_prompt_version == ""  # draft.py arrives in M2


# --- Classify: success paths -------------------------------------------------------


async def test_classify_returns_mapped_result() -> None:
    fake = FakeProvider(
        raw='{"intent": "billing", "urgency": "high", "sentiment": -0.5, "confidence": 0.9}',
        usage=Usage(input_tokens=321, output_tokens=12),
    )
    async with serve(fake) as stub:
        r = await stub.Classify(classify_request(), timeout=5)

    assert not r.needs_human
    assert r.intent == pb.ClassifyResponse.BILLING
    assert r.urgency == pb.ClassifyResponse.HIGH
    assert r.sentiment == pytest.approx(-0.5)
    assert r.confidence == pytest.approx(0.9)
    assert r.prompt_version == classify.VERSION
    assert (r.usage.input_tokens, r.usage.output_tokens) == (321, 12)


async def test_provider_receives_the_built_prompt() -> None:
    fake = FakeProvider()
    async with serve(fake) as stub:
        await stub.Classify(classify_request("where is my invoice?"), timeout=5)

    req = classify_request("where is my invoice?")
    assert fake.calls == [classify.build(req)]


async def test_invalid_model_output_fails_closed_with_ok_status() -> None:
    """Bad model output is an answer (needs_human), not an RPC error."""
    async with serve(FakeProvider(raw="Sorry, I can't help with that.")) as stub:
        r = await stub.Classify(classify_request(), timeout=5)

    assert r.needs_human
    assert r.intent == pb.ClassifyResponse.INTENT_UNSPECIFIED
    assert r.prompt_version == classify.VERSION


async def test_latency_covers_the_provider_call() -> None:
    async with serve(FakeProvider(delay=0.1)) as stub:
        r = await stub.Classify(classify_request(), timeout=5)

    assert 100 <= r.usage.latency_ms < 2000


# --- Classify: errors as status codes ------------------------------------------------


async def test_invalid_request_is_invalid_argument_and_skips_the_provider() -> None:
    fake = FakeProvider()
    async with serve(fake) as stub:
        err = await rpc_error(stub.Classify(pb.ClassifyRequest(message_id="m-1"), timeout=5))

    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert err.details() == "thread is empty"
    assert fake.calls == []


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (ProviderRateLimited, grpc.StatusCode.RESOURCE_EXHAUSTED),
        (ProviderTimeout, grpc.StatusCode.DEADLINE_EXCEEDED),
        (ProviderUnavailable, grpc.StatusCode.UNAVAILABLE),
        (ProviderError, grpc.StatusCode.INTERNAL),  # e.g. bad key, insufficient_quota
    ],
)
async def test_provider_errors_map_to_status_codes(
    error: type[ProviderError], code: grpc.StatusCode
) -> None:
    upstream_detail = "upstream said: org-123 key sk-abc is over quota"
    async with serve(FakeProvider(error=error(upstream_detail))) as stub:
        err = await rpc_error(stub.Classify(classify_request(), timeout=5))

    assert err.code() == code
    # the client learns the kind of failure, never the provider's own message
    assert err.details() == error.__name__
    assert upstream_detail not in (err.details() or "")


async def test_provider_failure_is_logged_by_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="ai_service.servicer")
    secret = "jane.doe@example.com"
    async with serve(FakeProvider(error=ProviderRateLimited(secret))) as stub:
        await rpc_error(stub.Classify(classify_request(secret), timeout=5))

    [record] = [r for r in caplog.records if r.name == "ai_service.servicer"]
    assert record.__dict__["message_id"] == "m-1"
    assert record.__dict__["error"] == "ProviderRateLimited"
    assert secret not in caplog.text
    assert secret not in repr(record.__dict__)


async def test_unexpected_exception_is_not_swallowed() -> None:
    """A bug must surface as UNKNOWN, not be dressed up as a provider status."""
    async with serve(FakeProvider(error=RuntimeError("bug"))) as stub:  # type: ignore[arg-type]
        err = await rpc_error(stub.Classify(classify_request(), timeout=5))

    assert err.code() == grpc.StatusCode.UNKNOWN


# --- deadlines ------------------------------------------------------------------------


async def test_provider_gets_the_callers_remaining_deadline() -> None:
    spy = SpyProvider()
    async with serve(spy) as stub:
        await stub.Classify(classify_request(), timeout=5)

    [timeout] = spy.timeouts
    assert timeout is not None
    assert 4 < timeout <= 5


async def test_no_client_deadline_means_no_provider_timeout() -> None:
    spy = SpyProvider()
    async with serve(spy) as stub:
        await stub.Classify(classify_request())

    assert spy.timeouts == [None]


async def test_slow_provider_hits_the_deadline_and_stops_working() -> None:
    spy = SpyProvider(delay=30)
    async with serve(spy) as stub:
        err = await rpc_error(stub.Classify(classify_request(), timeout=0.3))
        # the server must give up too, not keep waiting on (and paying for) the model
        await asyncio.wait_for(spy.exited.wait(), timeout=2)

    assert err.code() == grpc.StatusCode.DEADLINE_EXCEEDED


# --- DraftReply -------------------------------------------------------------------------


async def test_draft_reply_is_unimplemented_until_m2() -> None:
    async with serve(FakeProvider()) as stub:
        call = stub.DraftReply(pb.DraftReplyRequest(), timeout=5)
        with pytest.raises(grpc.aio.AioRpcError) as info:
            async for _ in call:
                pass

    assert info.value.code() == grpc.StatusCode.UNIMPLEMENTED
