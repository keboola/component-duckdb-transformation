# Single source of truth for supported DuckDB versions at runtime.
#
# When adding a new version:
#   1. Add a [dependency-groups] entry in pyproject.toml: "duckdb-X.Y.Z" = ["duckdb==X.Y.Z"]
#   2. Add it to the conflicts list in [tool.uv] in pyproject.toml
#   3. Add a RUN block in Dockerfile (base stage)
#   4. Add the version string here in SUPPORTED_VERSIONS
#   5. Run `uv lock` to update the lockfile
#   6. CI pre-check will validate consistency automatically
#
# VENV_BASE must match ARG VENV_BASE default in Dockerfile.

from packaging.version import Version

VENV_BASE = "/code/.venvs"

# Order does not matter — DEFAULT_VERSION is always derived as the
# semantically highest version using packaging.Version comparison.
SUPPORTED_VERSIONS: set[str] = {
    "1.5.1",
    "1.4.4",  # LTS
}

DEFAULT_VERSION = max(SUPPORTED_VERSIONS, key=Version)

# "latest" is a UI alias that always resolves to DEFAULT_VERSION at runtime.
# It is not in SUPPORTED_VERSIONS and is not tracked in the lockfile.
LATEST_ALIAS = "latest"


def venv_name(version: str) -> str:
    """Returns the venv directory name for a given version string."""
    return f"duckdb-{version}"


def resolve_version(version: str) -> str:
    """Resolves the 'latest' UI alias to DEFAULT_VERSION; passes real versions through."""
    return DEFAULT_VERSION if version == LATEST_ALIAS else version
