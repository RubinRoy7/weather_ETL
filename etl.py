"""
Weather and air-quality ETL: Open-Meteo APIs -> pandas -> SQL Server

Weather and air quality run independently through extract, validate, stage,
and merge. A failure in one source does not prevent the other source from
running. Every step is logged to etl.load_log.

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
AIR_QUALITY_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum"
AIR_QUALITY_VARS = "pm10,pm2_5,us_aqi"
PAST_DAYS = 7  # overlapping window on purpose: shows idempotent MERGE upserts
TIMEZONE = "Asia/Kolkata"

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
        "timezone": TIMEZONE,
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


def extract_air_quality_city(city, lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": AIR_QUALITY_VARS,
        "timezone": TIMEZONE,
        "past_days": PAST_DAYS,
        "forecast_days": 1,
    }
    resp = requests.get(AIR_QUALITY_API_URL, params=params, timeout=30)
    resp.raise_for_status()
    hourly = resp.json()["hourly"]
    df = pd.DataFrame({
        "obs_time": hourly["time"],
        "pm10": hourly["pm10"],
        "pm2_5": hourly["pm2_5"],
        "us_aqi": hourly["us_aqi"],
    })
    df["city_name"] = city
    return df


def extract_all_air_quality():
    frames = []
    for city, (lat, lon) in CITIES.items():
        log.info("Extracting air quality for %s", city)
        frames.append(extract_air_quality_city(city, lat, lon))
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


def validate_air_quality(df):
    """Return clean and rejected hourly pollutant readings."""
    df = df.copy()
    df["obs_time"] = pd.to_datetime(df["obs_time"], errors="coerce")
    for col in ("pm10", "pm2_5", "us_aqi"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    reasons = pd.Series("", index=df.index)
    reasons[df["obs_time"].isna()] += "missing_time;"
    reasons[df["city_name"].isna() | (df["city_name"] == "")] += "missing_city;"
    reasons[df["pm10"].isna()] += "missing_pm10;"
    reasons[df["pm2_5"].isna()] += "missing_pm2_5;"
    reasons[(df["pm10"] < 0) | (df["pm2_5"] < 0)] += "negative_pm;"
    reasons[(df["pm10"] > 2000) | (df["pm2_5"] > 2000)] += "pm_out_of_range;"
    reasons[(df["us_aqi"] < 0) | (df["us_aqi"] > 500)] += "aqi_out_of_range;"

    rejected = df[reasons != ""].assign(reason=reasons[reasons != ""])
    rejected["obs_date"] = rejected["obs_time"].dt.date
    rejected["obs_time"] = rejected["obs_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    clean = (
        df[reasons == ""]
        .drop_duplicates(subset=["city_name", "obs_time"], keep="last")
        .reset_index(drop=True)
    )
    clean["obs_time"] = clean["obs_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    clean["us_aqi"] = clean["us_aqi"].round().astype("Int64")
    return clean, rejected


# --------------------------------------------------------------------------
# 3. Stage  &  4. Merge
# --------------------------------------------------------------------------
def load_staging(conn, batch_id, df):
    rows = [(batch_id,) + r for r in to_rows(
        df, ["city_name", "obs_date", "temp_max_c", "temp_min_c", "precip_mm"])]
    if not rows:
        raise ValueError("No valid rows to stage: all rows were rejected by validation")

    cur = conn.cursor()
    cur.fast_executemany = True
    cur.execute("TRUNCATE TABLE staging.weather_daily")
    cur.executemany(
        "INSERT INTO staging.weather_daily "
        "(batch_id, city_name, obs_date, temp_max_c, temp_min_c, precip_mm) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


def load_air_quality_staging(conn, batch_id, df):
    rows = [(batch_id,) + r for r in to_rows(
        df, ["city_name", "obs_time", "pm10", "pm2_5", "us_aqi"])]
    if not rows:
        raise ValueError("No valid air-quality rows to stage: all rows were rejected")

    cur = conn.cursor()
    cur.fast_executemany = True
    cur.execute("TRUNCATE TABLE staging.air_quality_hourly")
    cur.executemany(
        "INSERT INTO staging.air_quality_hourly "
        "(batch_id, city_name, obs_time, pm10, pm2_5, us_aqi) "
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


def save_rejected_air_quality(conn, batch_id, rejected):
    if rejected.empty:
        return
    rows = [(batch_id,) + r for r in to_rows(
        rejected, ["city_name", "obs_time", "obs_date", "pm10", "pm2_5", "us_aqi", "reason"])]
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO etl.rejected_air_quality_rows "
        "(batch_id, city_name, obs_time, obs_date, pm10, pm2_5, us_aqi, reason) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def run_merge(conn, batch_id):
    cur = conn.cursor()
    cur.execute("EXEC dw.usp_merge_weather @batch_id = ?", batch_id)
    conn.commit()


def run_air_quality_merge(conn, batch_id):
    cur = conn.cursor()
    cur.execute("EXEC dw.usp_merge_air_quality @batch_id = ?", batch_id)
    conn.commit()


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def run_source(conn, batch_id, name, suffix, extract, validate, stage, merge):
    step = "extract" + suffix
    try:
        raw = extract()
        write_log(conn, batch_id, step, "SUCCESS", len(raw))

        step = "validate" + suffix
        clean, rejected = validate(raw)
        if suffix == "_air":
            save_rejected_air_quality(conn, batch_id, rejected)
        else:
            save_rejected(conn, batch_id, rejected)
        write_log(
            conn, batch_id, step, "SUCCESS", len(clean),
            f"clean={len(clean)}, rejected={len(rejected)}",
        )

        step = "stage" + suffix
        staged = stage(conn, batch_id, clean)
        write_log(conn, batch_id, step, "SUCCESS", staged)

        step = "merge" + suffix
        merge(conn, batch_id)
        log.info("Source '%s' finished OK", name)
        return True
    except Exception as exc:  # noqa: BLE001
        log.exception("Source '%s' failed at step '%s'", name, step)
        try:
            conn.rollback()
            if not step.startswith("merge"):
                write_log(conn, batch_id, step, "FAILED", None, str(exc))
        except Exception:
            log.exception("Could not record failure for source '%s'", name)
        return False


def main():
    batch_id = str(uuid.uuid4())
    log.info("Starting batch %s", batch_id)
    conn = get_connection()
    try:
        weather_ok = run_source(
            conn, batch_id, "weather", "", extract_all, validate, load_staging, run_merge,
        )
        air_quality_ok = run_source(
            conn, batch_id, "air_quality", "_air",
            extract_all_air_quality, validate_air_quality,
            load_air_quality_staging, run_air_quality_merge,
        )
    finally:
        conn.close()

    if not weather_ok or not air_quality_ok:
        failures = []
        if not weather_ok:
            failures.append("weather")
        if not air_quality_ok:
            failures.append("air_quality")
        log.error("Batch %s finished with failures: %s", batch_id, ", ".join(failures))
        sys.exit(1)
    log.info("Batch %s finished OK", batch_id)


if __name__ == "__main__":
    main()
