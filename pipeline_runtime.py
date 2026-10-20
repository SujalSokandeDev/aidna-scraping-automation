"""Shared, replay-safe delivery primitives for the two scraping pipelines."""

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone, timedelta

MAX_STAGED_RECORD_BYTES = 90_000_000


def content_hash(value):
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def canonical_json(value):
    # Existing formatters emit JSON strings; the load must store JSON objects, not strings of JSON.
    return json.loads(value) if isinstance(value, str) else value


def read_tracking(client, table, key, **filters):
    rows, offset = {}, 0
    while True:
        query = client.table(table).select("*").order(key)
        for field, value in filters.items():
            query = query.eq(field, value)
        page = query.range(offset, offset + 499).execute().data
        if page is None:
            raise RuntimeError("State storage returned no tracking response")
        rows.update((row[key], row) for row in page)
        if len(page) < 500:
            return rows
        offset += 500


def confirm_tracking(client, table, key, rows):
    """Persist only confirmed delivery state, retaining existing first-seen/history fields."""
    for offset in range(0, len(rows), 100):
        batch = [dict(row) for row in rows[offset:offset + 100]]
        for row in batch:
            row.setdefault('first_seen', utc_now())
        # PostgREST requires matching keys across a bulk upsert, including new and existing rows.
        fields = set().union(*(row.keys() for row in batch))
        batch = [{field: row.get(field) for field in fields} for row in batch]
        client.table(table).upsert(batch, on_conflict=key).execute()
        found = client.table(table).select(f"{key},scrape_status,content_hash,last_scraped,bigquery_id").in_(key, [row[key] for row in batch]).execute().data
        def progress(row):
            return row.get('scrape_status'), row.get('content_hash'), timestamp(row.get('last_scraped')), row.get('bigquery_id')
        actual = {row[key]: progress(row) for row in found or []}
        if any(actual.get(row[key]) != progress(row) for row in batch):
            raise RuntimeError("State write/readback failed; delivery will be retried safely")


def check_state_store(client, table, key, pipeline):
    client.table(table).select(key).limit(1).execute()
    crawl_id = "aidna-readiness-" + uuid.uuid4().hex
    try:
        client.table("aidna_pipeline_checkpoints").insert({"crawl_id": crawl_id, "pipeline": pipeline,
            "source": "_readiness_probe", "mode": "incremental", "status": "running"}).execute()
        client.table("aidna_pipeline_checkpoints").update({"status": "completed", "end_time": utc_now()}).eq("crawl_id", crawl_id).execute()
        result = client.table("aidna_pipeline_checkpoints").select("status").eq("crawl_id", crawl_id).execute().data
        if not result or result[0]["status"] != "completed":
            raise RuntimeError("State readiness write/readback was not confirmed")
    finally:
        client.table("aidna_pipeline_checkpoints").delete().eq("crawl_id", crawl_id).execute()
        remaining = client.table('aidna_pipeline_checkpoints').select('crawl_id').eq('crawl_id', crawl_id).execute().data
        if remaining is None or remaining:
            raise RuntimeError('Temporary readiness checkpoint cleanup was not confirmed')


def merge_statement(table_id, source, columns):
    identity = " AND ".join(f"T.{column}=S.{column}" for column in ("id", "source", "content_type"))
    assignments = ", ".join(
        "external_images=CASE WHEN S.external_images IS NULL OR JSON_TYPE(S.external_images)='null' THEN T.external_images ELSE S.external_images END"
        if column == 'external_images' else f"{column}=S.{column}"
        for column in columns if column not in ("id", "source", "content_type"))
    return (f"""MERGE `{table_id}` T USING ({source}) S ON {identity}
      WHEN MATCHED THEN UPDATE SET {assignments}
      WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join('S.' + column for column in columns)})""", identity)


def merge_large_record(client, table_id, row, payload):
    """Load large JSON through a short-lived staging table, not a query parameter."""
    if len(payload.encode('utf-8')) > MAX_STAGED_RECORD_BYTES:
        raise ValueError('Single source record exceeds the safe BigQuery row-size limit; no content was truncated')
    from google.cloud import bigquery
    table = client.get_table(table_id)
    stage_id = table_id.rsplit('.', 1)[0] + '.aidna_stage_' + uuid.uuid4().hex
    stage = bigquery.Table(stage_id, schema=table.schema)
    stage.expires = datetime.now(timezone.utc) + timedelta(hours=1)
    stage.labels = {'application': 'aidna-scraping', 'purpose': 'temporary-delivery'}
    client.create_table(stage)
    try:
        load = client.load_table_from_json([row], stage_id, location=table.location,
            job_config=bigquery.LoadJobConfig(schema=table.schema,
                create_disposition='CREATE_NEVER', write_disposition='WRITE_EMPTY'))
        load.result(timeout=180)
        if load.output_rows != 1:
            raise RuntimeError('Large-record staging load was not confirmed')
        source = f'SELECT * FROM `{stage_id}`'
        query, identity = merge_statement(table_id, source, list(row))
        config = bigquery.QueryJobConfig(maximum_bytes_billed=int(os.getenv('BIGQUERY_MAX_BYTES_BILLED', '10737418240')),
            labels={'application': 'aidna-scraping'})
        job = client.query(query, location=table.location, job_config=config)
        job.result(timeout=180)
        verification = f'''SELECT S.id,S.source,S.content_type FROM `{stage_id}` S
          WHERE EXISTS(SELECT 1 FROM `{table_id}` T WHERE {identity}
          AND T.processing_timestamp=S.processing_timestamp)'''
        actual = {(r.id, r.source, r.content_type) for r in client.query(
            verification, location=table.location, job_config=config).result(timeout=180)}
        if actual != {(row['id'], row['source'], row['content_type'])}:
            raise RuntimeError('Large-record BigQuery readback failed; tracking remains retryable')
        print(f'Confirmed staged BigQuery delivery: {row["source"]}/{row["content_type"]}/{row["id"]}')
        return 1
    finally:
        # Only this invocation's generated, expiring staging table is ever removed.
        client.delete_table(stage_id, not_found_ok=True)


def merge_records(client, table_id, records):
    """Atomically upsert a batch. Return only after BigQuery confirms the job and readback.

    Existing duplicate rows are not deleted. Identity includes the existing source and
    content type, because CMS numeric IDs are reused across collections and sites.
    """
    if not records:
        return 0
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", table_id):
        raise ValueError("Invalid BigQuery destination identifier")
    if os.getenv('EXPECTED_GCP_PROJECT_ID') and table_id.split('.')[0] != os.environ['EXPECTED_GCP_PROJECT_ID']:
        raise ValueError('Configured destination does not match the approved project')
    from google.cloud import bigquery

    columns = ["id", "source", "content_type", "attributes", "external_images", "title", "slug", "locale",
               "published_at", "updated_at", "created_at", "processing_timestamp"]
    rows, seen = [], set()
    for record in records:
        row = {column: record.get(column) for column in columns}
        key = tuple(row[column] for column in ("id", "source", "content_type"))
        if not all(isinstance(value, str) and value for value in key) or key in seen:
            raise ValueError("Missing or duplicate source/type/ID in delivery batch")
        seen.add(key)
        for column in ("attributes", "external_images"):
            row[column] = canonical_json(row[column])
        for column in ("published_at", "updated_at", "created_at", "processing_timestamp"):
            row[column] = timestamp(row[column]).isoformat() if row[column] else None
        if row["processing_timestamp"] is None:
            raise ValueError("A delivery timestamp is required")
        rows.append(row)
    payload = json.dumps(rows, ensure_ascii=False)
    # Leave room for JSON request escaping, query text and client metadata under API limits.
    parameter_limit = min(3_000_000, max(100, int(os.getenv('BIGQUERY_PARAMETER_LIMIT_BYTES', '3000000'))))
    if len(payload.encode("utf-8")) > parameter_limit:
        if len(rows) == 1:
            return merge_large_record(client, table_id, rows[0], payload)
        middle = len(rows) // 2
        return merge_records(client, table_id, rows[:middle]) + merge_records(client, table_id, rows[middle:])

    table = client.get_table(table_id)
    selections = []
    for column in columns:
        expression = f"JSON_QUERY(item, '$.{column}')" if column in ("attributes", "external_images") else f"JSON_VALUE(item, '$.{column}')"
        if column in ("published_at", "updated_at", "created_at", "processing_timestamp"):
            expression = f"TIMESTAMP({expression})"
        selections.append(f"{expression} AS {column}")
    source = "SELECT " + ", ".join(selections) + " FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@payload))) item"
    query, identity = merge_statement(table_id, source, columns)
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("payload", "STRING", payload)],
        maximum_bytes_billed=int(os.getenv("BIGQUERY_MAX_BYTES_BILLED", "10737418240")),
        labels={"application": "aidna-scraping"},
    )
    # Batch payload determines a stable retry job ID. An uncertain response is not an invitation to append again.
    job_id = "aidna_merge_" + hashlib.sha256((table_id + payload).encode()).hexdigest()
    from google.api_core.exceptions import Conflict
    try:
        job = client.query(query, job_config=config, location=table.location, job_id=job_id)
    except Conflict:
        job = client.get_job(job_id, location=table.location)
    job.result(timeout=180)
    verification = f"""SELECT S.id,S.source,S.content_type FROM ({source}) S
      WHERE EXISTS(SELECT 1 FROM `{table_id}` T WHERE {identity}
        AND T.processing_timestamp=S.processing_timestamp)"""
    verified = list(client.query(verification, job_config=config, location=table.location).result(timeout=180))
    actual = {(row.id, row.source, row.content_type) for row in verified}
    if actual != seen:
        raise RuntimeError("BigQuery readback did not confirm every delivered record; tracking remains retryable")
    print(f'Confirmed BigQuery job {job_id}: {len(rows)} source/type/ID records verified')
    return len(rows)
