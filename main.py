import requests
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import os
import json
from flask import Flask, jsonify, request
from google.cloud import storage
from google.cloud import bigquery

# GCS env
GCS_BUCKET = os.getenv("GCS_BUCKET")  # 必填
GCS_PREFIX = os.getenv("GCS_PREFIX", "google_trends_rss/tw")

# BQ env
BQ_PROJECT = os.getenv("BQ_PROJECT")  # 預設用Cloud Run所在專案
BQ_DATASET = os.getenv("BQ_DATASET", "trends")
BQ_TABLE = os.getenv("BQ_TABLE", "google_trends_tw")


url = "https://trends.google.com/trending/rss?geo=TW"
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'}
ns = {'ht': 'https://trends.google.com/trending/rss'}


def fetch_data() -> list[dict]:
    r_text = requests.get(url, headers=header, timeout=30).text
    
    root = ET.fromstring(r_text)
    items = root.findall('./channel/item') # root以rss下做為起點，去抓底下<channel>的所有<item>
    tz_tw = timezone(timedelta(hours=8))
    fetch_times = datetime.now(tz_tw).isoformat()

    data = []
    for i in items:
        title = i.findtext("title")
        approx_traffic = i.findtext("ht:approx_traffic", namespaces=ns) #<ht:approx_traffic>200+</ht:approx_traffic>,'ht:'namespace前綴，在xml中的完整名稱：{namespace_url}tag_name
        pubdate = i.findtext("pubDate")
        news = i.findall('ht:news_item', namespaces=ns)
        news_list = []
        for n in news:
            news_title = n.findtext("ht:news_item_title", namespaces=ns)
            news_src = n.findtext("ht:news_item_source", namespaces=ns)
            news_list.append((news_title, news_src))
        # print(title, amount, pubdate, news_list)
        data.append(
            {
                'geo': 'TW',
                'keyword': title,
                'approx_traffic': approx_traffic,
                'pubdate': pubdate,
                'news': news_list,
                'fetch_times': fetch_times
            }
        )
    return data

def save_to_gcs(data: list[dict]) -> tuple[str, str]:
    if not GCS_BUCKET:
        raise ValueError("Missing GCS_BUCKET!")

    tz_tw = timezone(timedelta(hours=8))
    stamp = datetime.now(tz_tw).strftime("%Y%m%d_%H%M")
    object_name = f"{GCS_PREFIX}/trends_tw_{stamp}.jsonl"

    jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"

    client = storage.Client()
    blob = client.bucket(GCS_BUCKET).blob(object_name)
    blob.upload_from_string(jsonl, content_type="application/json")

    gcs_uri = f"gs://{GCS_BUCKET}/{object_name}"
    return gcs_uri, object_name
    



def load_to_bigquery(gcs_uri: str) -> str:
    client = bigquery.Client(project=BQ_PROJECT or None)
    table_id = f"{client.project}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )
    load_job.result()  # 等待完成(失敗會丟 exception)

    return f"loaded_to={table_id}, job_id={load_job.job_id}"



@app.get("/")
def health():
    return "ok", 200


@app.post("/run")
def run():
    expected = os.getenv("RUN_KEY")
    if expected and request.headers.get("X-Run-Key") != expected:
        return jsonify({"error": "unauthorized"}), 401

    rows = fetch_data()
    gcs_uri, object_name = save_to_gcs(rows)
    bq_result = load_to_bigquery(gcs_uri)

    return jsonify(
        {
            "count": len(rows),
            "gcs_uri": gcs_uri,
            "bq": bq_result,
        }
    ), 200

