import asyncio
import logging

import openai
from openai import AsyncOpenAI
from openai.types.shared import ReasoningEffort
from pydantic import BaseModel

from ai_service.providers.base import (
    Prompt,
    ProviderError,
    ProviderRateLimited,
    ProviderTimeout,
    ProviderUnavailable,
    Usage,
)

log = logging.getLogger(__name__)


class OpenAIProvider:
    """Chat Completions with strict JSON-schema output.

    Works against OpenAI and OpenAI-compatible endpoints (Groq in dev). The SDK's
    own retries are off: this class retries only 5xx and connection errors, inside
    the caller's deadline; the worker owns backoff on rate limits (PLAN §5).
    """

    name = "openai"

    def __init__(
        self,
        client: AsyncOpenAI,
        *,
        model: str,
        reasoning_effort: ReasoningEffort | None = None,
        max_attempts: int = 3,
    ) -> None:
        self._client = client.with_options(max_retries=0)
        self._model = model
        self._reasoning_effort = reasoning_effort
        self._max_attempts = max_attempts

    async def complete_structured(
        self, prompt: Prompt, schema: type[BaseModel], timeout: float | None
    ) -> tuple[str, Usage]:
        try:
            async with asyncio.timeout(timeout):
                return await self._with_retries(prompt, schema)
        except TimeoutError as e:
            raise ProviderTimeout(f"no answer within {timeout}s") from e

    async def _with_retries(
        self, prompt: Prompt, schema: type[BaseModel]
    ) -> tuple[str, Usage]:
        attempt = 1
        while True:
            try:
                return await self._call(prompt, schema)
            except (openai.APIConnectionError, openai.InternalServerError) as e:
                if attempt >= self._max_attempts:
                    raise ProviderUnavailable(type(e).__name__) from e
                log.warning(
                    "provider call failed, retrying",
                    extra={"attempt": attempt, "error": type(e).__name__},
                )
            await asyncio.sleep(0.25 * 2 ** (attempt - 1))
            attempt += 1

    async def _call(self, prompt: Prompt, schema: type[BaseModel]) -> tuple[str, Usage]:
        try:
            r = await self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": prompt.system},
                    {"role": "user", "content": prompt.user},
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": schema.__name__,
                        "schema": schema.model_json_schema(),
                        "strict": True,
                    },
                },
                reasoning_effort=self._reasoning_effort or openai.omit,
            )
        except openai.BadRequestError as e:
            if e.code == "json_validate_failed":  # Groq: output broke the schema
                failed = (
                    e.body.get("failed_generation")
                    if isinstance(e.body, dict)
                    else None
                )
                return (failed if isinstance(failed, str) else ""), Usage(0, 0)
            raise ProviderError(f"bad request ({e.code})") from e
        except openai.RateLimitError as e:
            if e.code == "insufficient_quota":  # out of credit: retrying won't help
                raise ProviderError("insufficient_quota") from e
            raise ProviderRateLimited(e.code or "rate limited") from e
        except openai.InternalServerError:
            raise  # 5xx: retried by _with_retries
        except openai.APIStatusError as e:
            raise ProviderError(f"status {e.status_code}") from e
        except openai.APITimeoutError as e:
            raise ProviderTimeout("SDK timeout") from e

        content = r.choices[0].message.content if r.choices else None
        usage = (
            Usage(r.usage.prompt_tokens, r.usage.completion_tokens)
            if r.usage
            else Usage(0, 0)
        )
        return content or "", usage  # refusal / empty -> "" -> fails closed in classify
