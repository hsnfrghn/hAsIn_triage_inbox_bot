from ai_service.providers.base import LLMProvider
from ai_service.providers.fake import FakeProvider


def make_provider(name: str) -> LLMProvider:
    match name:
        case "fake":
            return FakeProvider()
        # case "openai": step 8, with a lazy import inside this branch,
        # so fake mode never imports the OpenAI SDK or needs an API key
        case _:
            raise ValueError(f"unknown AI_PROVIDER {name!r}")

__all__ = ["FakeProvider", "LLMProvider", "make_provider"]