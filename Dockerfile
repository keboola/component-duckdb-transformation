FROM python:3.13-slim AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ARG VENV_BASE=/code/.venvs
WORKDIR /code/
COPY pyproject.toml uv.lock ./

# Current version venv — runtime deps + duckdb 1.5.2, no dev tools
RUN UV_PROJECT_ENVIRONMENT=$VENV_BASE/duckdb-1.5.2 \
    uv sync --no-group dev --frozen

# LTS venv — runtime deps + duckdb 1.4.4, no dev tools
RUN UV_PROJECT_ENVIRONMENT=$VENV_BASE/duckdb-1.4.4 \
    uv sync --group duckdb-1.4.4 --no-group dev --no-group duckdb-1.5.2 --frozen

COPY src/ src/


# ── test ──────────────────────────────────────────────────────────────────────

FROM base AS test

ARG VENV_BASE=/code/.venvs

# Extend the primary venv with dev tools (pytest, ruff, ty, pre-commit)
RUN UV_PROJECT_ENVIRONMENT=$VENV_BASE/duckdb-1.5.2 \
    uv sync --frozen

COPY scripts/ scripts/
COPY tests/ tests/

CMD ["uv", "run", "python", "-m", "pytest", "tests/", "--tb=short", "-q"]


# ── production ────────────────────────────────────────────────────────────────

FROM base AS production

ARG VENV_BASE=/code/.venvs
ENV UV_PROJECT_ENVIRONMENT=$VENV_BASE/duckdb-1.5.2
ENV UV_NO_CACHE=1

CMD ["uv", "run", "--no-sync", "python", "-u", "/code/src/launcher.py"]
