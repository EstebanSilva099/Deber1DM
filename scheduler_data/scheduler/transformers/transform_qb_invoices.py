import pandas as pd

def _safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(k, default)
        else:
            return default
    return cur if cur is not None else default

@transformer
def transform(data, *args, **kwargs):
    """
    data: lo que sale del bloque anterior (upsert_invoices_to_postgres)
    En tu loader tú armaste "out.append({ id, payload, ... })"
    Así que aquí data debería ser una lista de dicts.
    """
    if data is None:
        return pd.DataFrame()

    if isinstance(data, pd.DataFrame):
        rows = data.to_dict("records")
    else:
        rows = data

    invoice_rows = []
    line_rows = []

    for r in rows:
        payload = r.get("payload") or {}
        inv_id = r.get("id") or _safe_get(payload, "Id")

        invoice_rows.append({
            "id": inv_id,
            "doc_number": _safe_get(payload, "DocNumber"),
            "txn_date": _safe_get(payload, "TxnDate"),
            "currency": _safe_get(payload, "CurrencyRef", "value"),
            "customer_id": _safe_get(payload, "CustomerRef", "value"),
            "customer_name": _safe_get(payload, "CustomerRef", "name"),
            "total_amt": _safe_get(payload, "TotalAmt"),
            "balance": _safe_get(payload, "Balance"),
            "status": _safe_get(payload, "TxnStatus"),
            "private_note": _safe_get(payload, "PrivateNote"),
            "create_time": _safe_get(payload, "MetaData", "CreateTime"),
            "last_updated_time": _safe_get(payload, "MetaData", "LastUpdatedTime"),
            "sync_token": _safe_get(payload, "SyncToken"),

            "ingested_at_utc": r.get("ingested_at_utc"),
            "extract_window_start_utc": r.get("extract_window_start_utc"),
            "extract_window_end_utc": r.get("extract_window_end_utc"),
            "page_number": r.get("page_number"),
            "page_size": r.get("page_size"),
        })

        lines = payload.get("Line") or []
        for i, ln in enumerate(lines):
            line_rows.append({
                "invoice_id": inv_id,
                "line_idx": i,
                "line_id": ln.get("Id"),
                "detail_type": ln.get("DetailType"),
                "amount": ln.get("Amount"),
                "description": ln.get("Description"),
                "item_id": _safe_get(ln, "SalesItemLineDetail", "ItemRef", "value"),
                "item_name": _safe_get(ln, "SalesItemLineDetail", "ItemRef", "name"),
                "qty": _safe_get(ln, "SalesItemLineDetail", "Qty"),
                "unit_price": _safe_get(ln, "SalesItemLineDetail", "UnitPrice"),
            })

    invoices_df = pd.DataFrame(invoice_rows)
    lines_df = pd.DataFrame(line_rows)

    return {
        "invoices": invoices_df,
        "lines": lines_df,
    }
