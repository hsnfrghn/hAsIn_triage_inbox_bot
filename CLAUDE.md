# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

AI Triage Inbox is a local support inbox that the user is building to learn **AI engineering and NestJS**. Customer emails arrive over IMAP and an AI classifies them in the background. Agents request reply drafts, which stream in token by token, then review them and send over SMTP. **`PLAN.md` is the source of truth** for the design, the decisions and their reasons, and the milestones (M0–M8). Read it before suggesting anything architectural.

Status: M0 is done. M1 (ai-service: Classify + GetInfo) is in progress, with steps 1–7 of 9 done: codegen inside the package, tooling, the `classify.py` output gate, the `LLMProvider` protocol with the fake provider, the classify prompt and `build()`, the servicer, and in-process gRPC tests. Next is step 8: the OpenAI SDK adapter, developed against Groq's free tier (`apps/ai-service/.env` sets `OPENAI_API_KEY`, `OPENAI_BASE_URL`, `OPENAI_MODEL`; it's git-ignored, so never print or commit it).

## Collaboration rules

- **The user writes the code.** Claude's default role is to discuss, review, explain, and point out bugs or trade-offs. Don't create or edit source files unless the user explicitly asks for that specific change.
- For `PLAN.md` and `CLAUDE.md`: discuss proposed changes first, then edit them once agreed.
- Keep the project small (it's a local tool), but never remove an educational element of the plan to save effort, especially on the AI and NestJS side.

## Architecture invariants

The full reasoning is in PLAN.md §1–§7. Flag any code or suggestion that breaks one of these.

- **Two AI paths stay separate.** Classification is async (outbox → Redpanda/Kafka → worker → unary gRPC). Drafts are interactive (WS → gateway → server-streaming gRPC). Never mix them.
- **`ai-service` (Python) is stateless and has no DB credentials.** It owns prompts, PII redaction, provider calls, output validation, and token usage. NestJS sends it all the context it needs.
- **`gateway` is strictly user-scoped.** Every query runs through `withUserTx`, which does `SET LOCAL ROLE app_user` and sets the JWT claims, so RLS applies. **`worker` is the only privileged (BYPASSRLS) actor**, and it hosts ingestion, the outbox relay, the classify consumer, and the mail sender.
- **Dual writes go through the outbox**: incoming messages (`inbox.message.received`) and outgoing replies (`inbox.reply.send`). The one deliberate exception is the best-effort `inbox.message.classified` notification.
- **The contract lives in `packages/proto/triage/v1/triage.proto`**, with codegen by ts-proto via buf for Nest, and by grpcio-tools (the ai-service's own `generate` script) for Python. buf.build is unreachable from this network, so never use remote buf plugins.
  - Every enum has `*_UNSPECIFIED = 0`.
  - Failing closed means `needs_human = true`, never a reused UNSPECIFIED.
  - Errors travel as gRPC status codes, not inside the stream.
  - Every result carries `prompt_version`.
- **Classification idempotency key** is `(message_id, prompt_version)`, and Kafka topics are keyed by `conversation_id`.
- **WS broadcasts carry IDs only.** Clients refetch through RLS-checked REST.
- **The model gets no tools, and a human sends every reply.** Customer text is untrusted input.

## Layout (planned)

`apps/{gateway,worker,ai-service,web}`, `packages/{proto,db}`, `infra/docker-compose.yml` (Postgres, Redpanda, GreenMail, Jaeger), `scripts/`. The Python ai-service uses **uv**, with a thin `package.json` so Turborepo includes it.

## Commands

Run from the repo root unless noted.

- **Install:** `pnpm i`. Python deps are managed by uv in `apps/ai-service`, and `uv run` syncs them on demand (`uv sync` to do it explicitly).
- **Infra:** `pnpm infra:up` starts Postgres, Redpanda, GreenMail and Jaeger and waits until healthy. `pnpm infra:down` stops them and keeps data (`docker compose -f infra/docker-compose.yml down -v` wipes it). Redpanda Console is optional: add `--profile tools` to `up`.
- **Codegen:** `pnpm generate`. buf runs ts-proto into `packages/proto/gen/ts`, and grpcio-tools writes `apps/ai-service/src/ai_service/gen` (imported as `ai_service.gen.triage.v1`, via protoc's `-Iai_service/gen=...` virtual path). Re-run after every `.proto` change.
- **Proto:** `pnpm --filter @inbox/proto lint`, `pnpm --filter @inbox/proto format`, `pnpm proto:breaking` (compares with `main`).
- **One package:** `pnpm --filter <package-name> <script>`.
- **ai-service checks** (from `apps/ai-service`): `uv run pytest -q`; a single test: `uv run pytest tests/test_classify.py::test_valid_output_is_mapped`; `uv run mypy src`; `uv run ruff check .`. They aren't wired into `pnpm test` / `pnpm lint` until the package gets those scripts (M1 step 9).
- **Not set up yet** (add them when their milestone does): dev, test (all, one package, one test), lint, evals (M7), db migrate/seed (M3).

### Local endpoints

| Service | Address |
|---|---|
| Postgres | `localhost:5432` (db `inbox`) |
| Kafka API (Redpanda) | `localhost:19092` from the host, `redpanda:9092` inside compose |
| GreenMail | SMTP `3025`, IMAP `3143`, REST API `8083` |
| Jaeger | UI `16686`, OTLP gRPC `4317`, OTLP HTTP `4318` |
| Redpanda Console | `8088` (with `--profile tools`) |
