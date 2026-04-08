FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /code/
COPY pyproject.toml uv.lock ./

# Primary venv: all deps + duckdb 1.5.1 (from lockfile)
RUN uv venv /opt/venvs/duckdb-1.5.1 && \
    UV_PROJECT_ENVIRONMENT=/opt/venvs/duckdb-1.5.1 uv sync --all-groups --frozen

# Legacy venv: clone primary, swap duckdb to 1.4.4
RUN cp -a /opt/venvs/duckdb-1.5.1 /opt/venvs/duckdb-1.4.4 && \
    uv pip install --python /opt/venvs/duckdb-1.4.4/bin/python duckdb==1.4.4

COPY src/ src/
COPY tests/ tests/
COPY flake8.cfg .

# System Python runs the launcher, which execs into the correct venv
CMD ["python", "-u", "/code/src/launcher.py"]
