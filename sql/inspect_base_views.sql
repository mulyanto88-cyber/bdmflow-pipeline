-- =====================================================================
-- inspect_base_views.sql
-- Diagnostik: cek keberadaan, kolom, dan DDL dari 7 base view yang
-- direferensikan phase_a_views.sql. Pakai untuk validasi sebelum
-- jalankan phase_a_views.sql apa adanya.
--
-- Cara pakai:
--   Paste blok per blok ke MotherDuck SQL console (atau jalankan sekaligus
--   jika konsol mendukung multi-statement).
-- =====================================================================


-- 1) EXISTENCE — apakah 7 view ini ada? (juga cek tipenya: VIEW vs TABLE)
-- =====================================================================
WITH wanted(schema_name, name) AS (
  VALUES
    ('market', 'vw_f_price_history'),
    ('market', 'vw_f_volume_profile'),
    ('market', 'vw_f_foreign_flow_daily'),
    ('main',   'vw_f_broker_category_rolling'),
    ('market', 'vw_s_whale_timing'),
    ('main',   'vw_f_insider_rolling'),
    ('market', 'vw_s_foreign_accumulation')
)
SELECT
  w.schema_name,
  w.name,
  CASE
    WHEN v.view_name  IS NOT NULL THEN 'VIEW'
    WHEN t.table_name IS NOT NULL THEN 'TABLE'
    ELSE 'MISSING'
  END AS object_type
FROM wanted w
LEFT JOIN duckdb_views()  v ON v.schema_name = w.schema_name AND v.view_name  = w.name
LEFT JOIN duckdb_tables() t ON t.schema_name = w.schema_name AND t.table_name = w.name
ORDER BY w.schema_name, w.name;


-- 2) KOLOM — daftar kolom + tipe per view (kalau ada)
-- =====================================================================
SELECT table_schema, table_name, column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE (table_schema, table_name) IN (
  ('market', 'vw_f_price_history'),
  ('market', 'vw_f_volume_profile'),
  ('market', 'vw_f_foreign_flow_daily'),
  ('main',   'vw_f_broker_category_rolling'),
  ('market', 'vw_s_whale_timing'),
  ('main',   'vw_f_insider_rolling'),
  ('market', 'vw_s_foreign_accumulation')
)
ORDER BY table_schema, table_name, ordinal_position;


-- 3) DDL — definisi SQL view (untuk view, bukan table)
-- =====================================================================
SELECT schema_name, view_name, sql
FROM duckdb_views()
WHERE (schema_name, view_name) IN (
  ('market', 'vw_f_price_history'),
  ('market', 'vw_f_volume_profile'),
  ('market', 'vw_f_foreign_flow_daily'),
  ('main',   'vw_f_broker_category_rolling'),
  ('market', 'vw_s_whale_timing'),
  ('main',   'vw_f_insider_rolling'),
  ('market', 'vw_s_foreign_accumulation')
)
ORDER BY schema_name, view_name;


-- 4) ROW COUNT — sanity check populasi
-- =====================================================================
SELECT 'market.vw_f_price_history'         AS view_full, COUNT(*) AS n FROM market.vw_f_price_history
UNION ALL SELECT 'market.vw_f_volume_profile',      COUNT(*) FROM market.vw_f_volume_profile
UNION ALL SELECT 'market.vw_f_foreign_flow_daily',  COUNT(*) FROM market.vw_f_foreign_flow_daily
UNION ALL SELECT 'main.vw_f_broker_category_rolling', COUNT(*) FROM main.vw_f_broker_category_rolling
UNION ALL SELECT 'market.vw_s_whale_timing',        COUNT(*) FROM market.vw_s_whale_timing
UNION ALL SELECT 'main.vw_f_insider_rolling',       COUNT(*) FROM main.vw_f_insider_rolling
UNION ALL SELECT 'market.vw_s_foreign_accumulation', COUNT(*) FROM market.vw_s_foreign_accumulation;
-- Catatan: kalau view tidak ada, UNION ini akan error pada baris view yang
-- hilang. Jalankan blok 1 dulu untuk lihat mana yang missing, lalu hapus
-- baris yang hilang sebelum jalankan blok 4 ini.


-- 5) SAMPLE — 1 baris pertama tiap view (struktur data nyata)
--    Jalankan satu per satu sesuai kebutuhan.
-- =====================================================================
-- SELECT * FROM market.vw_f_price_history          LIMIT 1;
-- SELECT * FROM market.vw_f_volume_profile         LIMIT 1;
-- SELECT * FROM market.vw_f_foreign_flow_daily     LIMIT 1;
-- SELECT * FROM main.vw_f_broker_category_rolling  LIMIT 1;
-- SELECT * FROM market.vw_s_whale_timing           LIMIT 1;
-- SELECT * FROM main.vw_f_insider_rolling          LIMIT 1;
-- SELECT * FROM market.vw_s_foreign_accumulation   LIMIT 1;


-- 6) BONUS — fuzzy search kalau nama view ternyata beda sedikit
--    (mis. vw_price_history tanpa _f, atau di schema lain)
-- =====================================================================
SELECT schema_name, view_name
FROM duckdb_views()
WHERE view_name ILIKE '%price_history%'
   OR view_name ILIKE '%volume_profile%'
   OR view_name ILIKE '%foreign_flow_daily%'
   OR view_name ILIKE '%broker_category_rolling%'
   OR view_name ILIKE '%whale_timing%'
   OR view_name ILIKE '%insider_rolling%'
   OR view_name ILIKE '%foreign_accumulation%'
ORDER BY schema_name, view_name;
