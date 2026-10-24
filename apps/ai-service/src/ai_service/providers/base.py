from dataclasses import dataclass
from typing import Protocol

from pydantic import BaseModel


@dataclass(frozen=True, slots=True)
class Prompt:
    system: str   # our instructions (trusted)
    user: str     # rendered thread, customer text delimited inside (untrusted)


@dataclass(frozen=True, slots=True)
class Usage:
    input_tokens: int
    output_tokens: int


class ProviderError(Exception):
    """Base for every provider failure the servicer knows how to map."""

class ProviderRateLimited(ProviderError): ...
class ProviderTimeout(ProviderError): ...
class ProviderUnavailable(ProviderError): ...


class LLMProvider(Protocol):
    @property
    def name(self) -> str: ...      # GetInfo reports this ("fake" | "openai")

    async def complete_structured(
        self, prompt: Prompt, schema: type[BaseModel], timeout: float | None
    ) -> tuple[str, Usage]: ...
