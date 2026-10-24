import asyncio

from pydantic import BaseModel

from ai_service.providers.base import Prompt, ProviderError, ProviderTimeout, Usage

DEFAULT_RAW = '{"intent": "other", "urgency": "normal", "sentiment": 0.0, "confidence": 0.5}'
DEFAULT_USAGE = Usage(input_tokens=10, output_tokens=5)


class FakeProvider:
    name = "fake"

    def __init__(
        self,
        *,
        raw: str = DEFAULT_RAW,
        usage: Usage = DEFAULT_USAGE,
        delay: float = 0.0,
        error: ProviderError | None = None,
    ) -> None:
        self.raw = raw
        self.usage = usage
        self.delay = delay
        self.error = error
        self.calls: list[Prompt] = []   # tests assert on what was sent

    async def complete_structured(
        self, prompt: Prompt, schema: type[BaseModel], timeout: float | None
    ) -> tuple[str, Usage]:
        self.calls.append(prompt)
        try:
            async with asyncio.timeout(timeout):     # None = no limit
                await asyncio.sleep(self.delay)
        except TimeoutError as e:
            raise ProviderTimeout(f"fake provider exceeded {timeout}s") from e
        if self.error is not None:
            raise self.error
        return self.raw, self.usage
