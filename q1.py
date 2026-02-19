import duckdb

con = duckdb.connect("case.duckdb")

query = """
SELECT
  airline,
  COUNT(DISTINCT
    src_city || '|' || src_country || '->' || dst_city || '|' || dst_country
  ) AS unique_city_pairs_directional
FROM routes_enriched
WHERE airline IN ('LH', 'UA')
  AND is_codeshare = false
  AND src_type = 'airport'
  AND dst_type = 'airport'
  AND src_city IS NOT NULL AND src_country IS NOT NULL
  AND dst_city IS NOT NULL AND dst_country IS NOT NULL
GROUP BY airline
ORDER BY airline;
"""

result = con.execute(query).fetchdf()

print("\nQuestion 1 — Unique Directional City-Pairs")
print(result)

validation_query = """
SELECT COUNT(*) 
FROM routes_enriched
WHERE airline IN ('LH','UA')
  AND is_codeshare = false
  AND (src_type != 'airport' OR dst_type != 'airport');
"""

print(con.execute(validation_query).fetchall())


con.close()
