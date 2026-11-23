"""
Weather ETL pipeline: Open-Meteo API -> pandas (validate) -> SQL Server

Steps
  1. Extract   pull daily weather (last 7 days + today) for each city
  2. Validate  reject bad rows (saved to etl.rejected_rows), remove duplicates
  3. Stage     load clean rows into staging.weather_daily
  4. Merge     call dw.usp_merge_weather (incremental upsert)
  Every step is logged to etl.load_log. Any failure exits with code 1.

Config (environment variables, all optional):
  SQL_SERVER    default: localhost\\SQLEXPRESS
  SQL_DATABASE  default: WeatherDW
  SQL_DRIVER    default: ODBC Driver 17 for SQL Server
  SQL_USER / SQL_PASSWORD   if not set, Windows authentication is used
"""
import logging
import os
import sys
import uuid

import pandas as pd
import pyodbc
import requests

API_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum"
PAST_DAYS = 7  # overlapping window on purpose: shows idempotent MERGE upserts

CITIES = {
    "Thiruvananthapuram": (8.5241, 76.9366),
    "Kochi": (9.9312, 76.2673),
    "Kozhikode": (11.2588, 75.7804),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai": (13.0827, 80.2707),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("weather_etl")


# --------------------------------------------------------------------------
# Database helpers
# --------------------------------------------------------------------------
def get_connection():
    server = os.getenv("SQL_SERVER", r"localhost\SQLEXPRESS")
    database = os.getenv("SQL_DATABASE", "WeatherDW")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
    user, password = os.getenv("SQL_USER"), os.getenv("SQL_PASSWORD")

    conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};TrustServerCertificate=yes;"
    if user and password:
        conn_str += f"UID={user};PWD={password};"
    else:
        conn_str += "Trusted_Connection=yes;"
    return pyodbc.connect(conn_str, autocommit=False)


def write_log(conn, batch_id, step, status, rows=None, message=None):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO etl.load_log (batch_id, step, status, rows_affected, message) "
        "VALUES (?, ?, ?, ?, ?)",
        batch_id, step, status, rows, (message or "")[:1000],
    )
    conn.commit()


def to_rows(df, columns):
    """DataFrame -> list of tuples, converting NaN/NaT to None for pyodbc."""
    clean = df[columns].astype(object).where(df[columns].notna(), None)
    return [tuple(r) for r in clean.itertuples(index=False, name=None)]


# --------------------------------------------------------------------------
# 1. Extract
# --------------------------------------------------------------------------
def extract_city(city, lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": DAILY_VARS,
        "timezone": "Asia/Kolkata",
        "past_days": PAST_DAYS,
        "forecast_days": 1,
    }
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    daily = resp.json()["daily"]
    df = pd.DataFrame({
        "obs_date": daily["time"],
        "temp_max_c": daily["temperature_2m_max"],
        "temp_min_c": daily["temperature_2m_min"],
        "precip_mm": daily["precipitation_sum"],
    })
    df["city_name"] = city
    return df


def extract_all():
    frames = []
    for city, (lat, lon) in CITIES.items():
        log.info("Extracting %s", city)
        frames.append(extract_city(city, lat, lon))
    return pd.concat(frames, ignore_index=True)


# --------------------------------------------------------------------------
# 2. Validate
# --------------------------------------------------------------------------
def validate(df):
    """Return (clean_df, rejected_df). Rejected rows carry a 'reason'."""
    df = df.copy()
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce").dt.date
    for col in ("temp_max_c", "temp_min_c", "precip_mm"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    reasons = pd.Series("", index=df.index)
    reasons[df["obs_date"].isna()] += "missing_date;"
    reasons[df["city_name"].isna() | (df["city_name"] == "")] += "missing_city;"
    reasons[df["temp_min_c"] > df["temp_max_c"]] += "min_greater_than_max;"
    reasons[(df["temp_max_c"] > 55) | (df["temp_min_c"] < -10)] += "temp_out_of_range;"
    reasons[df["precip_mm"] < 0] += "negative_precipitation;"

    rejected = df[reasons != ""].assign(reason=reasons[reasons != ""])
    clean = (
        df[reasons == ""]
        .drop_duplicates(subset=["city_name", "obs_date"], keep="last")
        .reset_index(drop=True)
    )
    return clean, rejected


# --------------------------------------------------------------------------
# 3. Stage  &  4. Merge
# --------------------------------------------------------------------------
def load_staging(conn, batch_id, df):
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.execute("TRUNCATE TABLE staging.weather_daily")
    rows = [(batch_id,) + r for r in to_rows(
        df, ["city_name", "obs_date", "temp_max_c", "temp_min_c", "precip_mm"])]
    cur.executemany(
        "INSERT INTO staging.weather_daily "
        "(batch_id, city_name, obs_date, temp_max_c, temp_min_c, precip_mm) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


def save_rejected(conn, batch_id, rejected):
    if rejected.empty:
        return
    cur = conn.cursor()
    rows = [(batch_id,) + r for r in to_rows(
        rejected, ["city_name", "obs_date", "temp_max_c", "temp_min_c", "precip_mm", "reason"])]
    cur.executemany(
        "INSERT INTO etl.rejected_rows "
        "(batch_id, city_name, obs_date, temp_max_c, temp_min_c, precip_mm, reason) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def run_merge(conn, batch_id):
    cur = conn.cursor()
    cur.execute("EXEC dw.usp_merge_weather @batch_id = ?", batch_id)
    conn.commit()


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def main():
    batch_id = str(uuid.uuid4())
    log.info("Starting batch %s", batch_id)
    conn = get_connection()
    step = "extract"
    try:
        raw = extract_all()
        write_log(conn, batch_id, "extract", "SUCCESS", len(raw))

        step = "validate"
        clean, rejected = validate(raw)
        save_rejected(conn, batch_id, rejected)
        write_log(conn, batch_id, "validate", "SUCCESS", len(clean),
                  f"clean={len(clean)}, rejected={len(rejected)}")

        step = "stage"
        staged = load_staging(conn, batch_id, clean)
        write_log(conn, batch_id, "stage", "SUCCESS", staged)

        step = "merge"
        run_merge(conn, batch_id)  # the procedure writes its own log row

        log.info("Batch %s finished OK", batch_id)
    except Exception as exc:  # noqa: BLE001
        log.exception("Batch %s failed at step '%s'", batch_id, step)
        try:
            conn.rollback()
            if step != "merge":  # merge procedure already logs its own failure
                write_log(conn, batch_id, step, "FAILED", None, str(exc))
        finally:
            conn.close()
        sys.exit(1)
    conn.close()


if __name__ == "__main__":
    main()
