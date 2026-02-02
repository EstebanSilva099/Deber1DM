from mage_ai.data_preparation.decorators import custom
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path


@custom
def transform_custom(*args, **kwargs):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, profile)) as loader:
        rows = loader.load("SELECT current_database() AS db, current_user AS usr;")
        print("DB/USER:", rows)

        exists = loader.load("SELECT to_regclass('raw.qb_invoices') AS tbl;")
        print("to_regclass:", exists)

        tables = loader.load("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'raw'
            ORDER BY 1,2;
        """)
        print("tables in raw:", tables)

    return {"ok": True}


