import duckdb
from pathlib import Path

DATA_DIR = Path("data")
AIRPORTS_FILE = DATA_DIR / "airports-extended.dat"
ROUTES_FILE = DATA_DIR / "routes.dat"

con = duckdb.connect("case.duckdb")

con.execute("PRAGMA enable_progress_bar=false;")

con.execute("DROP TABLE IF EXISTS airports_raw;")
con.execute(
    """
    CREATE TABLE airports_raw AS
    SELECT
        airport_id::INTEGER              AS airport_id,
        name::VARCHAR                    AS name,
        city::VARCHAR                    AS city,
        country::VARCHAR                 AS country,
        NULLIF(iata, '\\\\N')::VARCHAR   AS iata,
        NULLIF(icao, '\\\\N')::VARCHAR   AS icao,
        latitude::DOUBLE                 AS latitude,
        longitude::DOUBLE                AS longitude,
        altitude::INTEGER                AS altitude_ft,
        timezone::DOUBLE                 AS timezone_utc_offset,
        dst::VARCHAR                     AS dst,
        tz_db::VARCHAR                   AS tz_db,
        type::VARCHAR                    AS type,
        source::VARCHAR                  AS source
    FROM read_csv(
        ?,
        delim=',',
        quote='\"',
        escape='\"',
        header=false,
        nullstr='\\N',
        columns={
            'airport_id':'VARCHAR',
            'name':'VARCHAR',
            'city':'VARCHAR',
            'country':'VARCHAR',
            'iata':'VARCHAR',
            'icao':'VARCHAR',
            'latitude':'VARCHAR',
            'longitude':'VARCHAR',
            'altitude':'VARCHAR',
            'timezone':'VARCHAR',
            'dst':'VARCHAR',
            'tz_db':'VARCHAR',
            'type':'VARCHAR',
            'source':'VARCHAR'
        }
    );
    """,
    [str(AIRPORTS_FILE)]
)

con.execute("DROP TABLE IF EXISTS routes_raw;")
con.execute(
    """
    CREATE TABLE routes_raw AS
    SELECT
        airline::VARCHAR                               AS airline,        
        NULLIF(airline_id, '\\\\N')::INTEGER          AS airline_id,
        src_airport::VARCHAR                           AS src_airport,    
        NULLIF(src_airport_id, '\\\\N')::INTEGER      AS src_airport_id,  
        dst_airport::VARCHAR                           AS dst_airport,
        NULLIF(dst_airport_id, '\\\\N')::INTEGER      AS dst_airport_id,
        CASE WHEN codeshare = 'Y' THEN true ELSE false END AS is_codeshare,
        stops::INTEGER                                 AS stops,
        NULLIF(equipment, '\\\\N')::VARCHAR           AS equipment       
    FROM read_csv(
        ?,
        delim=',',
        quote='\"',
        escape='\"',
        header=false,
        nullstr='\\N',
        columns={
            'airline':'VARCHAR',
            'airline_id':'VARCHAR',
            'src_airport':'VARCHAR',
            'src_airport_id':'VARCHAR',
            'dst_airport':'VARCHAR',
            'dst_airport_id':'VARCHAR',
            'codeshare':'VARCHAR',
            'stops':'VARCHAR',
            'equipment':'VARCHAR'
        }
    );
    """,
    [str(ROUTES_FILE)]
)

airports_count = con.execute("SELECT COUNT(*) FROM airports_raw;").fetchone()[0]
routes_count = con.execute("SELECT COUNT(*) FROM routes_raw;").fetchone()[0]

print("Loaded successfully")
print("airports_raw rows:", airports_count)
print("routes_raw rows:  ", routes_count)

print("\nSample airports:")
print(con.execute("SELECT airport_id, name, city, country, iata, type FROM airports_raw LIMIT 5;").fetchdf())

print("\nSample routes:")
print(con.execute("SELECT airline, src_airport, dst_airport, src_airport_id, dst_airport_id, equipment FROM routes_raw LIMIT 5;").fetchdf())

con.close()
