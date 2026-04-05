"""
Database connection module.
Tries Lakebase (Postgres) first, falls back to Databricks SQL warehouse.
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Backend selection: Lakebase (Postgres) or Databricks SQL
# ---------------------------------------------------------------------------
_backend = None  # "postgres" or "databricks"
_pg_conn = None
_ws_client = None
_warehouse_id = None
_pg_host = None
_pg_dbname = None
_pg_user = None


def _get_workspace_client():
    """Get or create a WorkspaceClient."""
    global _ws_client
    if _ws_client is None:
        from databricks.sdk import WorkspaceClient
        _ws_client = WorkspaceClient()
    return _ws_client


def _try_postgres():
    """Try to connect to Lakebase using SDK-generated credentials."""
    global _pg_conn, _backend, _pg_host, _pg_dbname, _pg_user, _pg_error
    host = os.environ.get("PGHOST_OVERRIDE") or os.environ.get("PGHOST", "")
    if not host:
        return False

    dbname = os.environ.get("PGDATABASE_OVERRIDE") or os.environ.get("PGDATABASE", "artemis_app")
    user = os.environ.get("PGUSER", "")
    password = os.environ.get("PGPASSWORD", "")

    # If no password from resource injection, generate via SDK
    if not password:
        try:
            instance = os.environ.get("LAKEBASE_INSTANCE", "artemis-tracker-lb")
            w = _get_workspace_client()
            if not user:
                user = w.current_user.me().user_name
            cred = w.database.generate_database_credential(
                instance_names=[instance]
            )
            password = cred.token
            logger.info("Generated Lakebase credential for %s via instance %s", user, instance)
        except Exception as e:
            _pg_error = f"credential: {e}"
            logger.warning("Failed to generate Lakebase credential: %s", e)
            return False

    port_str = os.environ.get("PGPORT", "5432")
    try:
        port = int(port_str)
    except (ValueError, TypeError):
        port = 5432

    try:
        import psycopg2
        import psycopg2.extras
        _pg_conn = psycopg2.connect(
            host=host, port=port, dbname=dbname,
            user=user, password=password,
            sslmode="require", connect_timeout=10,
        )
        _pg_conn.autocommit = True
        _backend = "postgres"
        _pg_host = host
        _pg_dbname = dbname
        _pg_user = user
        logger.info("Connected to Lakebase at %s db=%s", host, dbname)
        return True
    except Exception as e:
        _pg_error = f"connect: {e}"
        logger.warning("Lakebase connect failed: %s", e)
        return False


def _try_databricks():
    """Try Databricks SQL warehouse."""
    global _ws_client, _warehouse_id, _backend
    try:
        w = _get_workspace_client()
        wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
        if not wh_id:
            warehouses = list(w.warehouses.list())
            for wh in warehouses:
                if wh.state and wh.state.value == "RUNNING":
                    wh_id = wh.id
                    break
            if not wh_id and warehouses:
                wh_id = warehouses[0].id
        if wh_id:
            _warehouse_id = wh_id
            _backend = "databricks"
            logger.info("Using Databricks SQL warehouse: %s", wh_id)
            return True
    except Exception as e:
        logger.warning("Databricks SQL connection failed: %s", e)
    return False


def _init_backend():
    global _backend
    if _backend is not None:
        return
    if not _try_postgres():
        if not _try_databricks():
            logger.error("No database backend available!")
            _backend = "none"


# ---------------------------------------------------------------------------
# Public query helpers
# ---------------------------------------------------------------------------

UC_SCHEMA = os.environ.get("UC_SCHEMA", "oil_pump_monitor_catalog.artemis_tracker")

def get_backend() -> str:
    """Return current backend: 'postgres', 'databricks', or 'none'."""
    _init_backend()
    return _backend or "none"


def table(name: str) -> str:
    """Return fully qualified table name for the current backend."""
    _init_backend()
    if _backend == "databricks":
        return f"{UC_SCHEMA}.{name}"
    return name  # Postgres uses unqualified names


_pg_error = None

def get_backend_info() -> dict:
    """Return backend details for health check."""
    _init_backend()
    return {
        "backend": _backend or "none",
        "pg_host": (_pg_host or "")[:40] if _pg_host else "not set",
        "pg_database": _pg_dbname or "not set",
        "pg_user": (_pg_user or "")[:30] if _pg_user else "not set",
        "warehouse_id": _warehouse_id or "not set",
        "pg_error": str(_pg_error)[:120] if _pg_error else None,
    }


def execute_query(sql: str) -> list[dict]:
    """Execute a SQL query and return all rows as a list of dicts."""
    _init_backend()

    if _backend == "postgres":
        return _pg_query(sql)
    elif _backend == "databricks":
        return _dbx_query(sql)
    else:
        raise RuntimeError("No database backend available")


def execute_query_single(sql: str) -> dict:
    """Execute a SQL query and return the first row as a dict, or empty dict."""
    rows = execute_query(sql)
    return rows[0] if rows else {}


def _pg_query(sql: str) -> list[dict]:
    global _pg_conn
    try:
        import psycopg2.extras
        if _pg_conn is None or _pg_conn.closed:
            _try_postgres()
        with _pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            if cur.description is None:
                return []
            return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        logger.error("Postgres query failed: %s", e)
        _pg_conn = None
        raise


def _dbx_query(sql: str) -> list[dict]:
    from databricks.sdk.service.sql import StatementState
    try:
        result = _ws_client.statement_execution.execute_statement(
            warehouse_id=_warehouse_id, statement=sql, wait_timeout="30s",
        )
        if result.status and result.status.state == StatementState.FAILED:
            raise RuntimeError(f"Query failed: {result.status.error}")
        if not result.manifest or not result.result or not result.result.data_array:
            return []
        columns = [c.name for c in result.manifest.schema.columns]
        return [{columns[i]: v for i, v in enumerate(row)} for row in result.result.data_array]
    except Exception as e:
        logger.error("Databricks query failed: %s", e)
        raise
