import duckdb
import os
import glob
from datetime import datetime, timezone
from pathlib import Path


def main():

    # -----------------------------
    # Resolve project root correctly
    # -----------------------------
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent

    # -----------------------------
    # Paths (clean & idiot-proof)
    # -----------------------------
    DB_PATH = project_root / "databases" / "currency_rates.duckdb"
    JSON_FOLDER = project_root / "jsons" / "fx_jsons"
    SCHEMA_NAME = "raw"
    TABLE_NAME = "raw_ingested_data"

    print("Project root resolved to:", project_root)
    print("Loading JSON files from:", JSON_FOLDER)
    print("Writing to DuckDB file:", DB_PATH)

    # -----------------------------
    # 1) Connect to DuckDB
    #    (it creates the DB file automatically if missing and the parent folder is exist!)
    # -----------------------------
    # Ensure database directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    print("Connected to DuckDB.")

    # -----------------------------
    # 2) List JSON files
    # -----------------------------
    json_files = glob.glob(str(JSON_FOLDER / "*.json"))
    if not json_files:
        raise FileNotFoundError("No JSON files found in JSON_FOLDER.")

    print(f"{len(json_files)} JSON files found.")

    # -----------------------------
    # 3) Load timestamp (UTC)
    # -----------------------------
    load_ts = datetime.now(timezone.utc).isoformat()
    print("Load timestamp (UTC):", load_ts)

    # -----------------------------
    # 4) Ensure schema exists
    # -----------------------------
    print(f"Ensuring schema exists: {SCHEMA_NAME}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")

    # -----------------------------
    # 5) Drop table → refreshed daily
    # -----------------------------
    print(f"Dropping existing table if exists: {SCHEMA_NAME}.{TABLE_NAME}")
    con.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{TABLE_NAME}")

    # -----------------------------
    # 6) Read all JSON into a single table
    # -----------------------------
    print("Ingesting JSON files into DuckDB…")

    con.execute(f"""
        CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} AS
        SELECT 
            *,
            '{load_ts}' AS loaded_at_utc,
            filename AS file_name
        FROM read_json('{JSON_FOLDER}/*.json', auto_detect=true, filename=true)
    """)

    print("Ingestion completed.")

    # -----------------------------
    # 7) Row count QC
    # -----------------------------
    rowcount = con.execute(
        f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}"
    ).fetchone()[0]

    print("Number of rows ingested:", rowcount)

    # -----------------------------
    # 8) Checkpoint & close
    # -----------------------------
    con.execute("CHECKPOINT;")
    con.close()
    print("DuckDB checkpointed and connection closed.")


if __name__ == "__main__":
    main()
