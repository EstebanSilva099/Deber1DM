import json
from datetime import timezone

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def upsert_invoices_to_postgres(data, *args, **kwargs):
    """
    Recibe output del transformer:
      data = {"invoices": invoices_df, "lines": lines_df}
    y hace upsert solo a raw.qb_invoices.
    """

    if not data or "invoices" not in data or data["invoices"] is None:
        return {"inserted_or_updated": 0, "note": "No invoices data returned from transformer."}

    invoices_df = data["invoices"]

    if hasattr(invoices_df, "empty") and invoices_df.empty:
        return {"inserted_or_updated": 0, "note": "Invoices dataframe is empty."}

    records = invoices_df.to_dict("records")

    config_path = path.join(get_repo_path(), "io_config.yaml")
    profile = "default"

    rows = []
    for r in records:
        payload = r.get("payload")
        request_payload = r.get("request_payload")

        def to_utc_iso(x):
            if x is None:
                return None
            if isinstance(x, str):
                return x
            return x.astimezone(timezone.utc).isoformat()

        rows.append({
            "id": str(r.get("id")),
            "payload": json.dumps(payload) if isinstance(payload, (dict, list)) else (payload or "{}"),
            "ingested_at_utc": to_utc_iso(r.get("ingested_at_utc")),
            "extract_window_start_utc": to_utc_iso(r.get("extract_window_start_utc")),
            "extract_window_end_utc": to_utc_iso(r.get("extract_window_end_utc")),
            "page_number": int(r.get("page_number") or 0),
            "page_size": int(r.get("page_size") or 0),
            "request_payload": json.dumps(request_payload) if isinstance(request_payload, (dict, list)) else (request_payload or "{}"),
        })

    sql = """
    INSERT INTO raw.qb_invoices (
      id, payload, ingested_at_utc, extract_window_start_utc, extract_window_end_utc,
      page_number, page_size, request_payload
    )
    VALUES (
      %(id)s, %(payload)s::jsonb, %(ingested_at_utc)s::timestamptz,
      %(extract_window_start_utc)s::timestamptz, %(extract_window_end_utc)s::timestamptz,
      %(page_number)s, %(page_size)s, %(request_payload)s::jsonb
    )
    ON CONFLICT (id) DO UPDATE SET
      payload = EXCLUDED.payload,
      ingested_at_utc = EXCLUDED.ingested_at_utc,
      extract_window_start_utc = EXCLUDED.extract_window_start_utc,
      extract_window_end_utc = EXCLUDED.extract_window_end_utc,
      page_number = EXCLUDED.page_number,
      page_size = EXCLUDED.page_size,
      request_payload = EXCLUDED.request_payload;
    """

    with Postgres.with_config(ConfigFileLoader(config_path, profile)) as loader:
        for row in rows:
            loader.execute(sql, **row)

        count = loader.load("SELECT COUNT(*) AS n FROM raw.qb_invoices;")

    return {"inserted_or_updated": len(rows), "raw.qb_invoices_count": count}
