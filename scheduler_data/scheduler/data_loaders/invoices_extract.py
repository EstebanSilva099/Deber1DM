import base64
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Optional


import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def parse_iso_z(s: str) -> datetime:
    return datetime.fromisoformat(s.replace('Z', '+00:00')).astimezone(timezone.utc)


def iter_daily_windows(start_iso: str, end_iso: str):
    start = parse_iso_z(start_iso)
    end = parse_iso_z(end_iso)
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=1), end)
        yield cur, nxt
        cur = nxt


def get_access_token() -> str:
    client_id = get_secret_value('qb_clientid')
    client_secret = get_secret_value('qb_clientsecret')
    refresh_token = get_secret_value('qb_refreshtoken')

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {basic}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

    r = requests.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        headers=headers,
        data=data,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def qbo_base_url() -> str:
    return "https://sandbox-quickbooks.api.intuit.com"


def qbo_query(entity: str, where_clause: Optional[str] = None, page_size: int = 1000):
    realm_id = get_secret_value('qb_realmid')
    url = f"{qbo_base_url()}/v3/company/{realm_id}/query"

    start = 1
    page_number = 1

    where_clause = (where_clause or "").strip()

    while True:
        where_sql = f" where {where_clause}" if where_clause else ""
        query = (
            f"select * from {entity}"
            f"{where_sql} "
            f"startposition {start} "
            f"maxresults {page_size}"
        )

        access_token = get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/text",
        }

        resp = None
        for attempt in range(6):
            resp = requests.post(url, headers=headers, data=query, timeout=60)

            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(60, (2 ** attempt)) + random.random())
                continue

            resp.raise_for_status()
            break

        data = resp.json()

        rows = (
            data.get("QueryResponse", {})
            .get(entity, [])
            or []
        )

        yield page_number, page_size, {"query": query}, rows

        if len(rows) < page_size:
            break

        start += page_size
        page_number += 1


@data_loader
def invoices_extract(*args, **kwargs):

    cfg = kwargs.get("configuration", {}) or {}

    fecha_inicio = cfg.get("fecha_inicio") or kwargs.get("fecha_inicio")
    fecha_fin = cfg.get("fecha_fin") or kwargs.get("fecha_fin")

    if not fecha_inicio or not fecha_fin:
        raise ValueError("Faltan variables: fecha_inicio y fecha_fin (ej: 2024-01-01T00:00:00Z)")

    out = []

    for w_start, w_end in iter_daily_windows(fecha_inicio, fecha_fin):
        where = (
            f"MetaData.LastUpdatedTime >= '{iso_utc(w_start)}' "
            f"and MetaData.LastUpdatedTime < '{iso_utc(w_end)}'"
        )

        for page_number, page_size, request_payload, rows in qbo_query("Invoice", None):
            ingested_at = datetime.now(timezone.utc)

            for r in rows:
                invoice_id = str(r.get("Id"))
                if not invoice_id:
                    continue

                out.append({
                    "id": invoice_id,
                    "payload": r,
                    "ingested_at_utc": ingested_at,
                    "extract_window_start_utc": w_start,
                    "extract_window_end_utc": w_end,
                    "page_number": page_number,
                    "page_size": page_size,
                    "request_payload": request_payload,
                })

            print("TOTAL OUT:", len(out))
            return out


