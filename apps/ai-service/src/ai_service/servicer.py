import logging
import time

import grpc

from ai_service import classify
from ai_service.gen.triage.v1 import triage_pb2 as pb
from ai_service.gen.triage.v1 import triage_pb2_grpc as pb_grpc
from ai_service.providers import LLMProvider
from ai_service.providers.base import (
    ProviderError,
    ProviderRateLimited,
    ProviderTimeout,
    ProviderUnavailable,
    Usage,
)

log = logging.getLogger(__name__)

_STATUS: dict[type[ProviderError], grpc.StatusCode] = {
    ProviderRateLimited: grpc.StatusCode.RESOURCE_EXHAUSTED,
    ProviderTimeout: grpc.StatusCode.DEADLINE_EXCEEDED,
    ProviderUnavailable: grpc.StatusCode.UNAVAILABLE,
}


def to_pb_usage(usage: Usage, started: float) -> pb.Usage:
    return pb.Usage(
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        latency_ms=round((time.monotonic() - started) * 1000),
    )


class TriageServicer(pb_grpc.TriageServiceServicer):
    def __init__(self, llm: LLMProvider) -> None:
        self.llm = llm

    async def GetInfo(
        self,
        request: pb.GetInfoRequest,
        context: grpc.aio.ServicerContext[pb.GetInfoRequest, pb.GetInfoResponse],
    ) -> pb.GetInfoResponse:
        return pb.GetInfoResponse(
            classify_prompt_version=classify.VERSION, provider=self.llm.name
        )

    async def Classify(
        self,
        request: pb.ClassifyRequest,
        context: grpc.aio.ServicerContext[pb.ClassifyRequest, pb.ClassifyResponse],
    ) -> pb.ClassifyResponse:
        try:
            prompt = classify.build(request)
        except classify.InvalidRequest as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        started = time.monotonic()
        try:
            raw, usage = await self.llm.complete_structured(
                prompt, classify.ClassificationOut, timeout=context.time_remaining()
            )
        except ProviderError as e:
            code = _STATUS.get(type(e), grpc.StatusCode.INTERNAL)
            log.warning(
                "provider call failed",
                extra={"message_id": request.message_id, "error": type(e).__name__},
            )
            await context.abort(code, type(e).__name__)
        return classify.to_response(
            raw, prompt_version=classify.VERSION, usage=to_pb_usage(usage, started)
        )