# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

A trigger-based SQL Server CDC (Change Data Capture) replicator. It installs INSERT/UPDATE/DELETE triggers on a source SQL Server database that write to a `dbo.sync_audit_log` table, then polls that log to replicate changes to a destination SQL Server in near real-time.

## Development Commands

**Install dependencies:**
```powershell
cd replicator
poetry install
```

**Run the replicator (development):**
```powershell
cd replicator
poetry run python -m src.replicator
```

**Manual sync (reconcile missing rows):**
```powershell
poetry run python -m src.replicator --sync-missing
poetry run python -m src.replicator --sync-missing --table TableName
```

**Initial trigger setup on source DB:**
```powershell
poetry run python -m src.setup_triggers
```

**Build standalone Windows EXE:**
```powershell
cd d:\toolSQL\tool_trigger
python build_exe.py
# Output: replicator/dist/CDC_Replicator.exe (also copied to project root)
```

**Docker:**
```powershell
docker-compose up --build
```

There is no test suite or linter configured.

## Configuration

The app reads from a `.env` file in the working directory (or the EXE's directory in production):

```
KINGDOM_SQLSERVER_HOST / PORT / USER / PASS / DB      # Source DB
KINGDOM_DST_SQLSERVER_HOST / PORT / USER / PASS / DB  # Destination DB
KINGDOM_BATCH_SIZE=5000          # rows per poll batch (default: 500)
KINGDOM_SCHEMA_CHECK_INTERVAL    # seconds (default: 5.0)
KINGDOM_TABLE_SCAN_INTERVAL      # seconds (default: 300.0)
KINGDOM_SYNC_TABLES              # optional comma-separated table whitelist
```

## Architecture

```
Source SQL Server
  └─ User tables
       └─ trig_cdc_TABLE_{INS,UPD,DEL}  ← created by setup_triggers.py
            └─ writes pk_value + operation → dbo.sync_audit_log

CDC Replicator (replicator.py event loop)
  ├─ Table discovery loop (every TABLE_SCAN_INTERVAL)
  │    └─ finds new tables → calls setup_triggers.py to add triggers
  ├─ Schema sync loop (every SCHEMA_CHECK_INTERVAL per table)
  │    └─ detects new columns → ALTER TABLE ADD COLUMN on destination
  └─ Change processing loop (every POLL_INTERVAL)
       ├─ SELECT TOP N from sync_audit_log
       ├─ group by table + operation (I/U/D)
       ├─ fetch full row data from source (2000 PK chunks)
       ├─ UPSERT / DELETE on destination
       └─ DELETE processed rows from audit_log

Destination SQL Server
  └─ Replicated tables (auto-created if missing)
```

## Key Module Responsibilities

- **`replicator/src/replicator.py`** — Main event loop and CLI entry point (`--sync-missing`, `--table`). Orchestrates the three concurrent loops above.
- **`replicator/src/db_utils.py`** — All database I/O: `upsert_data_odbc()`, `delete_data_odbc()`, `sync_schema_direct()`, `ensure_table_exists()`, `get_primary_key()`. Contains the adaptive batch logic (fast `executemany` → row-by-row fallback with `FAST_EXEC_FAIL_CACHE`).
- **`replicator/src/setup_triggers.py`** — Creates `sync_audit_log` and the three DDL triggers per table. Also handles `auto_discover_new_tables()`.
- **`replicator/src/manual_sync.py`** — Recovery path: loads all target PKs, diffs against source, injects missing rows as `'I'` records into the audit log for normal processing.
- **`replicator/src/logger.py`** — Colored console logging with level-specific colors; no external log framework.

## Notable Patterns

**Primary key resolution** (`db_utils.get_primary_key`): tries PK constraint → unique index → column named `id`/`ID` → first column (with warning). Code that touches PK-dependent logic must account for all four cases.

**UPSERT safety**: `upsert_data_odbc` auto-detects IDENTITY columns and wraps inserts with `SET IDENTITY_INSERT ON/OFF`. It also normalizes bytes→UTF-8, datetime strings, Decimal→float, and UUID types before sending to ODBC.

**Type casting in DDL**: `to_sql_type()` maps pyodbc column metadata to SQL Server DDL. `NVARCHAR`/`VARCHAR` columns larger than 4000 chars are promoted to `MAX`.

**Docker vs EXE**: Docker mounts `replicator/src` for live code reload. The EXE bundles all hidden imports (dotenv, pyodbc, db_utils, setup_triggers, logger) via PyInstaller spec at `replicator/CDC_Replicator.spec`.
