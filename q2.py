import duckdb

con = duckdb.connect("case.duckdb")

q_inclusive = """
SELECT airline, COUNT(*) AS routes_with_333_in_list
FROM routes_enriched
WHERE airline IN ('LH','UA')
  AND is_codeshare = false
  AND src_type = 'airport'
  AND dst_type = 'airport'
  AND equipment IS NOT NULL
  AND regexp_matches(equipment, '(^|\\s)333(\\s|$)')
GROUP BY airline
ORDER BY airline;
"""

q_exclusive = """
SELECT airline, COUNT(*) AS routes_with_only_333
FROM routes_enriched
WHERE airline IN ('LH','UA')
  AND is_codeshare = false
  AND src_type = 'airport'
  AND dst_type = 'airport'
  AND equipment IS NOT NULL
  AND regexp_matches(equipment, '^333$')
GROUP BY airline
ORDER BY airline;
"""

inclusive = con.execute(q_inclusive).fetchdf()
exclusive = con.execute(q_exclusive).fetchdf()

print("\nQ2 — A330-300 (code 333)")
print("\nInclusive (333 appears in equipment list):")
print(inclusive)

print("\nExclusive (equipment exactly '333'):")
print(exclusive)

con.close()
