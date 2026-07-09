# Building-a-Serverless-Macro-Risk-Data-Lake-Automating-NSE-Ingestion-Quant-Metrics-with-AWS-Tableau
Most financial market dashboards rely on static CSV downloads or rigid third-party data layers. To track live institutional trends and volatility metrics dynamically, I wanted to build a production-grade, automated pipeline from scratch. 
"""
lambda_function.py
-------------------
AWS Lambda handler that runs daily (post NSE market close, ~18:30 IST) to:
  1. Pull the day's NSE equity bhavcopy (OHLCV for every listed stock)
  2. Pull the day's FII/DII net trading activity
  3. Land both as partitioned CSVs in an S3 data lake bucket

S3 layout (Hive-style partitioning so Glue/Athena can auto-discover it):
  s3://<bucket>/nse_equity/year=YYYY/month=MM/day=DD/bhavcopy.csv
  s3://<bucket>/fii_dii/year=YYYY/month=MM/day=DD/flows.csv

Trigger: EventBridge (CloudWatch Events) cron rule, e.g.
  cron(0 13 ? * MON-FRI *)   # 13:00 UTC = 18:30 IST, weekdays only

Env vars expected:
  BUCKET_NAME   - target S3 bucket
  TARGET_DATE   - optional, "YYYY-MM-DD". Defaults to "today" (IST) if unset.
"""

import io
import os
import csv
import json
import zipfile
from datetime import datetime, timedelta, timezone

import boto3
import requests

s3 = boto3.client("s3")

IST = timezone(timedelta(hours=5, minutes=30))
NSE_HOME = "https://www.nseindia.com"
NSE_FIIDII_API = "https://www.nseindia.com/api/fiidiiTradeReact"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def get_nse_session() -> requests.Session:
    """NSE blocks bare requests; you first need to hit the homepage to
    pick up cookies (nsit, ak_bmsc, bm_sv, etc.) before hitting the API."""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.get(NSE_HOME, timeout=10)  # warms up cookies
    return session


def get_target_date() -> datetime:
    override = os.environ.get("TARGET_DATE")
    if override:
        return datetime.strptime(override, "%Y-%m-%d")
    return datetime.now(IST)


def fetch_bhavcopy(session: requests.Session, date: datetime) -> bytes:
    """Fetch the daily equity bhavcopy CSV (unzipped) as raw bytes."""
    mmm = date.strftime("%b").upper()
    url = (
        f"https://nsearchives.nseindia.com/content/historical/EQUITIES/"
        f"{date.year}/{mmm}/cm{date.strftime('%d')}{mmm}{date.year}bhav.csv.zip"
    )
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        inner_name = zf.namelist()[0]
        return zf.read(inner_name)


def fetch_fii_dii(session: requests.Session) -> list[dict]:
    """Fetch latest FII/DII net buy/sell activity (cash market, in Rs cr)."""
    resp = session.get(NSE_FIIDII_API, timeout=15)
    resp.raise_for_status()
    return resp.json()


def upload_csv_bytes(bucket: str, key: str, data: bytes):
    s3.put_object(Bucket=bucket, Key=key, Body=data)


def upload_json_as_csv(bucket: str, key: str, rows: list[dict]):
    if not rows:
        return
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))


def lambda_handler(event, context):
    bucket = os.environ["BUCKET_NAME"]
    date = get_target_date()
    y, m, d = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")

    session = get_nse_session()
    results = {"date": date.strftime("%Y-%m-%d")}

    # --- 1. Equity bhavcopy ---
    try:
        bhav_bytes = fetch_bhavcopy(session, date)
        key = f"nse_equity/year={y}/month={m}/day={d}/bhavcopy.csv"
        upload_csv_bytes(bucket, key, bhav_bytes)
        results["bhavcopy"] = f"s3://{bucket}/{key}"
    except Exception as e:
        # Common on holidays/weekends when there's no bhavcopy - don't fail the whole run
        results["bhavcopy_error"] = str(e)

    # --- 2. FII/DII flows ---
    try:
        flows = fetch_fii_dii(session)
        key = f"fii_dii/year={y}/month={m}/day={d}/flows.csv"
        upload_json_as_csv(bucket, key, flows)
        results["fii_dii"] = f"s3://{bucket}/{key}"
    except Exception as e:
        results["fii_dii_error"] = str(e)

    print(json.dumps(results))
    return {"statusCode": 200, "body": json.dumps(results)}


if __name__ == "__main__":
    # Local test: set BUCKET_NAME env var and run `python lambda_function.py`
    os.environ.setdefault("BUCKET_NAME", "test-bucket")
    lambda_handler({}, None)
