# openflights-case
This repository contains my solution to a small data engineering case study based on the OpenFlights routing dataset (https://openflights.org/data.php).

The objective was to analyze airline route data and answer three business questions related to Lufthansa (LH) and United Airlines (UA).

---

## Dataset

Source: OpenFlights (historical snapshot)

Files used:
- routes.dat
- airports-extended.dat

Note:
The dataset represents a historical snapshot and does not reflect current airline schedules.

---

## Tools & Approach

- Python
- DuckDB (embedded analytical SQL engine)

DuckDB was used to:
- Ingest CSV data
- Handle NULL normalization (`\N`)
- Join route and airport dimension data
- Perform aggregations using SQL

---

## How to Run

1. Install dependencies:

```bash
pip install -r requirements.txt

    Create a local data/ folder and place:

routes.dat
airports-extended.dat

    Run scripts in order:

python load.py
python enrich.py
python q1.py
python q2.py
python q3.py