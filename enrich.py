import duckdb

con = duckdb.connect("case.duckdb")

con.execute("DROP VIEW IF EXISTS routes_enriched;")

con.execute("""
CREATE VIEW routes_enriched AS
SELECT
    r.airline,
    r.airline_id,
    r.stops,
    r.is_codeshare,
    r.equipment,

    -- Source airport info
    r.src_airport_id,
    a_src.iata         AS src_iata,
    a_src.name         AS src_name,
    a_src.city         AS src_city,
    a_src.country      AS src_country,
    a_src.type         AS src_type,

    -- Destination airport info
    r.dst_airport_id,
    a_dst.iata         AS dst_iata,
    a_dst.name         AS dst_name,
    a_dst.city         AS dst_city,
    a_dst.country      AS dst_country,
    a_dst.type         AS dst_type

FROM routes_raw r
LEFT JOIN airports_raw a_src
    ON r.src_airport_id = a_src.airport_id
LEFT JOIN airports_raw a_dst
    ON r.dst_airport_id = a_dst.airport_id
""")

count = con.execute("SELECT COUNT(*) FROM routes_enriched;").fetchone()[0]
print("routes_enriched rows:", count)

print("\nSample enriched rows:")
print(con.execute("""
SELECT airline, src_city, src_country, dst_city, dst_country, equipment
FROM routes_enriched
LIMIT 5
""").fetchdf())

con.close()
