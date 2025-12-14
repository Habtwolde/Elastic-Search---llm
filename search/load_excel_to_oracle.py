#!/usr/bin/env python3
"""
load_excel_to_oracle.py

Loads rows from an Excel sheet into Oracle table: DOCS

Expected Oracle table schema (already created):
  docs(
    id         varchar2(64) primary key,
    title      varchar2(500),
    body       clob,
    content    clob,
    updated_at timestamp
  )

This script is designed to be run from:
  ...\Oracle-elser_\search>

Example:
  python .\load_excel_to_oracle.py --file "..\incidents.xlsx" --sheet 0
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

import oracledb


# -----------------------------
# .env loading (project root)
# -----------------------------
def load_env() -> Optional[Path]:
    """
    Loads .env from the project root (one level above the 'search' folder).
    Returns the path if loaded, else None.
    """
    here = Path(__file__).resolve()
    # project_root = .../Oracle-elser_
    project_root = here.parents[1]
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
        return env_path
    # fallback: load any .env in current working dir
    load_dotenv(override=True)
    return None


# -----------------------------
# Oracle connection helpers
# -----------------------------
def build_dsn() -> str:
    """
    Use ORACLE_DSN if provided; else build from ORACLE_HOST/PORT/SERVICE.
    For Oracle thin driver, DSN format: host:port/service_name
    """
    dsn = (os.getenv("ORACLE_DSN") or "").strip()
    if dsn:
        return dsn

    host = (os.getenv("ORACLE_HOST") or "").strip()
    port = (os.getenv("ORACLE_PORT") or "").strip()
    service = (os.getenv("ORACLE_SERVICE") or "").strip()

    if not host or not port or not service:
        raise ValueError(
            "Oracle connection env vars missing. Set either:\n"
            "  ORACLE_DSN\n"
            "or all of:\n"
            "  ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE\n"
        )

    return f"{host}:{port}/{service}"


def oracle_conn():
    user = (os.getenv("ORACLE_USER") or "").strip()
    password = os.getenv("ORACLE_PASSWORD")  # keep quotes in .env; dotenv strips them for us
    if not user or not password:
        raise ValueError("ORACLE_USER and ORACLE_PASSWORD must be set")

    dsn = build_dsn()

    # Thin mode is default; no Oracle Client required.
    # If you ever need thick mode, you'd init_oracle_client() here.
    return oracledb.connect(user=user, password=password, dsn=dsn)


# -----------------------------
# Excel -> docs row mapping
# -----------------------------
def pick_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def to_string_safe(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def parse_datetime_safe(v) -> Optional[datetime]:
    if pd.isna(v):
        return None
    # pandas may read Excel dates as Timestamp already
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    # try parse string
    s = str(v).strip()
    if not s:
        return None
    try:
        return pd.to_datetime(s, errors="coerce").to_pydatetime()
    except Exception:
        return None


def dataframe_to_docs(df: pd.DataFrame) -> list[dict]:
    """
    Convert dataframe rows to docs-compatible dicts.
    Tries common column names, but will still work with minimal columns.
    """
    id_col = pick_first_existing_column(df, ["id", "case_id", "doc_id", "incident_id"])
    title_col = pick_first_existing_column(df, ["title", "subject", "summary"])
    body_col = pick_first_existing_column(df, ["body", "description", "details", "content"])
    updated_col = pick_first_existing_column(df, ["updated_at", "opendate", "date", "created_at", "timestamp"])

    docs = []
    for i, row in df.iterrows():
        doc_id = to_string_safe(row[id_col]) if id_col else f"excel_{i+1}"
        title = to_string_safe(row[title_col]) if title_col else (to_string_safe(row[id_col]) if id_col else f"Row {i+1}")
        body = to_string_safe(row[body_col]) if body_col else ""

        updated = parse_datetime_safe(row[updated_col]) if updated_col else None
        if updated is None:
            updated = datetime.utcnow()

        content = f"{title}\n{body}".strip()

        docs.append(
            {
                "id": doc_id[:64],
                "title": title[:500],
                "body": body,
                "content": content,
                "updated_at": updated,
            }
        )
    return docs


# -----------------------------
# Oracle upsert
# -----------------------------
UPSERT_SQL = """
MERGE INTO docs d
USING (
  SELECT :id AS id,
         :title AS title,
         :body AS body,
         :content AS content,
         :updated_at AS updated_at
  FROM dual
) s
ON (d.id = s.id)
WHEN MATCHED THEN UPDATE SET
  d.title = s.title,
  d.body = s.body,
  d.content = s.content,
  d.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (id, title, body, content, updated_at)
VALUES (s.id, s.title, s.body, s.content, s.updated_at)
"""


def upsert_docs(conn, docs: list[dict]) -> Tuple[int, int]:
    """
    Returns (inserted_or_updated_count, error_count).
    """
    cur = conn.cursor()
    ok = 0
    err = 0

    for d in docs:
        try:
            cur.execute(
                UPSERT_SQL,
                id=d["id"],
                title=d["title"],
                body=d["body"],
                content=d["content"],
                updated_at=d["updated_at"],
            )
            ok += 1
        except Exception as e:
            err += 1
            print(f"[ERROR] id={d.get('id')}: {e}")

    conn.commit()
    cur.close()
    return ok, err


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Path to Excel file")
    ap.add_argument("--sheet", default=0, help="Sheet index or sheet name (default: 0)")
    ap.add_argument("--limit", type=int, default=0, help="Optional limit rows (0=all)")
    args = ap.parse_args()

    env_path = load_env()
    if env_path:
        print(f"Loaded .env from: {env_path}")

    xlsx_path = Path(args.file).resolve()
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel file not found: {xlsx_path}")

    print(f"Reading Excel: {xlsx_path} (sheet={args.sheet})")
    df = pd.read_excel(xlsx_path, sheet_name=args.sheet)

    if args.limit and args.limit > 0:
        df = df.head(args.limit)

    print(f"Rows: {len(df)} | Columns: {list(df.columns)}")

    if df.empty:
        print("No rows to load. Exiting.")
        return

    docs = dataframe_to_docs(df)
    print(f"Prepared {len(docs)} docs")

    conn = oracle_conn()
    ok, err = upsert_docs(conn, docs)
    conn.close()

    print(f"Upsert complete. OK={ok} | ERR={err}")


if __name__ == "__main__":
    main()
