import duckdb

con = duckdb.connect("case.duckdb")

query = """
SELECT DISTINCT
  dst_iata AS iata,
  dst_name AS name,
  dst_city AS city
FROM routes_enriched
WHERE airline = 'LH'
  AND is_codeshare = false
  AND dst_type = 'airport'
  AND dst_country = 'Germany'
  AND dst_iata IS NOT NULL
ORDER BY iata;
"""

df = con.execute(query).fetchdf()

print("\nQ3 — Airports in Germany LH flies to (destination airports)")
print("Count:", len(df))
print(df)

con.close()
