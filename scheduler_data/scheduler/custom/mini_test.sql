--SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'raw'
ORDER BY table_name;
 Docs: https://docs.mage.ai/guides/sql-blocks
