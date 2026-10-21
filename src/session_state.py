"""Session state (variables and settings) shared across DuckDB cursors."""

import logging
from dataclasses import dataclass, field

from duckdb import DuckDBPyConnection

SETTINGS_QUERY = "SELECT name, value FROM duckdb_settings()"
VARIABLES_QUERY = "SELECT name, type FROM duckdb_variables() ORDER BY name"


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class Variable:
    name: str
    type: str
    value: str | None  # VARCHAR rendering of the value, None for NULL


@dataclass(frozen=True)
class SessionState:
    """
    Snapshot of the session-scoped state set by `SET`, `SET VARIABLE` and `RESET` statements.

    - DuckDB keeps this state per connection, and every query runs on its own cursor (connection).
    - The snapshot is captured from the cursor that ran the session statement and applied to every new cursor.
    """

    settings: dict[str, str] = field(default_factory=dict)
    variables: tuple[Variable, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.settings and not self.variables

    def apply(self, cursor: DuckDBPyConnection) -> None:
        """Apply settings first, so values that depend on them (e.g. TIMESTAMPTZ and TimeZone) are cast correctly."""
        for name, value in self.settings.items():
            cursor.execute(f"SET SESSION {_quote_identifier(name)} = ?", [value])
        for variable in self.variables:
            cursor.execute(
                f"SET VARIABLE {_quote_identifier(variable.name)} = CAST(? AS {variable.type})",
                [variable.value],
            )

    @staticmethod
    def capture(cursor: DuckDBPyConnection, connection: DuckDBPyConnection) -> "SessionState":
        """Capture the variables of `cursor` and its settings that differ from a fresh cursor of `connection`."""
        current_settings = dict(cursor.execute(SETTINGS_QUERY).fetchall())
        fresh_cursor = connection.cursor()
        try:
            default_settings = dict(fresh_cursor.execute(SETTINGS_QUERY).fetchall())
        finally:
            fresh_cursor.close()
        settings = {
            name: value
            for name, value in current_settings.items()
            if value is not None and value != default_settings.get(name)
        }

        variables = []
        for name, variable_type in cursor.execute(VARIABLES_QUERY).fetchall():
            [(value,)] = cursor.execute(f"SELECT getvariable({_quote_literal(name)})::VARCHAR").fetchall()
            variables.append(Variable(name=name, type=variable_type, value=value))

        state = SessionState(settings=settings, variables=tuple(variables))
        logging.info(
            f"Session state applied to all following queries: settings {sorted(state.settings)}, "
            f"variables {[variable.name for variable in state.variables]}"
        )
        return state
