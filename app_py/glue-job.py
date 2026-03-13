import sys
import json
import hashlib
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote_plus

import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


BUCKET = "analytics-hitdata-s3"
PROCESSED_PREFIX = "processed/"
METADATA_PREFIX = "metadata/"
LOG_PREFIX = "logs/"
CHECKSUM_PREFIX = "checksums/"
CONFIG_FILE = "metadata_config.json"

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_key"])
INPUT_KEY = args["input_key"]

obj = s3.get_object(Bucket=BUCKET, Key=f"{METADATA_PREFIX}{CONFIG_FILE}")
config = json.loads(obj["Body"].read().decode("utf-8"))

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")


def log(msg):
    print(f"{datetime.utcnow().isoformat()} {msg}")


def upload_json(key, payload):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json"
    )


def upload_text(key, text):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain"
    )


def compute_checksum(bucket, key):
    try:
        hasher = hashlib.sha256()
        obj = s3.get_object(Bucket=bucket, Key=key)

        while True:
            chunk = obj["Body"].read(8 * 1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)

        return hasher.hexdigest()

    except ClientError as e:
        raise ValueError(f"S3 source file not found: s3://{bucket}/{key}") from e

def checksum_exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def move_single_part_file(temp_prefix, final_key):
    paginator = s3.get_paginator("list_objects_v2")
    objs = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=temp_prefix):
        objs.extend(page.get("Contents", []))

    part_key = None
    for obj in objs:
        key = obj["Key"]
        if "part-" in key:
            part_key = key
            break

    if not part_key:
        raise ValueError(f"No part file found under s3://{BUCKET}/{temp_prefix}")

    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": part_key},
        Key=final_key
    )

    delete_items = [{"Key": obj["Key"]} for obj in objs]
    if delete_items:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": delete_items})


class MetadataProfiler:
    def __init__(self, expected_columns):
        self.expected_columns = expected_columns or []

    def profile(self, df, source_key):
        columns = df.columns
        total_rows = df.count()

        missing = [c for c in self.expected_columns if c not in columns]
        extra = [c for c in columns if c not in self.expected_columns]

        mismatch_details = []
        if missing:
            mismatch_details.append({"missing_columns": missing})
        if extra:
            mismatch_details.append({"extra_columns": extra})
        if len(columns) != len(self.expected_columns):
            mismatch_details.append({
                "column_count_mismatch": {
                    "expected": len(self.expected_columns),
                    "actual": len(columns)
                }
            })

        profiles = []
        for c in columns:
            stats = df.agg(
                F.min(F.col(c)).alias("min_value"),
                F.max(F.col(c)).alias("max_value"),
                F.sum(
                    F.when(
                        F.col(c).isNull() | (F.trim(F.col(c)) == ""),
                        1
                    ).otherwise(0)
                ).alias("total_nulls"),
                F.countDistinct(F.col(c)).alias("total_unique_values")
            ).collect()[0]

            profiles.append({
                "column_name": c,
                "data_type": "string",
                "min_value": stats["min_value"],
                "max_value": stats["max_value"],
                "total_nulls": int(stats["total_nulls"]),
                "total_unique_values": int(stats["total_unique_values"])
            })

        return {
            "source_key": source_key,
            "profiled_at_utc": datetime.utcnow().isoformat(),
            "number_of_columns": len(columns),
            "column_names": columns,
            "total_rows": total_rows,
            "metadata_mismatch": len(mismatch_details) > 0,
            "metadata_mismatch_details": mismatch_details,
            "columns": profiles
        }


class SearchKeywordRevenueProcessor:
    @staticmethod
    def transform(df):
        required = ["ip", "referrer", "event_list", "product_list", "date_time"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns for business processing: {missing}")

        @F.udf(T.StringType())
        def search_engine(referrer):
            if not referrer:
                return None
            try:
                domain = (urlparse(referrer).netloc or "").lower()
            except Exception:
                return None

            if "google." in domain:
                return domain
            if "yahoo." in domain or "search.yahoo." in domain:
                return domain
            if "bing." in domain or "msn." in domain or "search.msn." in domain:
                return domain
            return None

        @F.udf(T.StringType())
        def keyword(referrer):
            if not referrer:
                return None
            try:
                parsed = urlparse(referrer)
                domain = (parsed.netloc or "").lower()
                query = parse_qs(parsed.query)

                if "google." in domain or "bing." in domain or "msn." in domain or "search.msn." in domain:
                    vals = query.get("q")
                    if vals and vals[0].strip():
                        return unquote_plus(vals[0].strip()).lower()

                if "yahoo." in domain or "search.yahoo." in domain:
                    vals = query.get("p")
                    if vals and vals[0].strip():
                        return unquote_plus(vals[0].strip()).lower()
            except Exception:
                return None
            return None

        @F.udf(T.BooleanType())
        def purchase(event_list):
            if not event_list:
                return False
            return "1" in [x.strip() for x in event_list.split(",") if x.strip()]

        @F.udf(T.DoubleType())
        def revenue(product_list):
            if not product_list:
                return 0.0
            total = 0.0
            for p in [x.strip() for x in product_list.split(",") if x.strip()]:
                parts = p.split(";")
                if len(parts) >= 4:
                    try:
                        total += float(parts[3].strip())
                    except Exception:
                        pass
            return total

        enriched = (
            df.withColumn("engine", search_engine(F.col("referrer")))
              .withColumn("keyword", keyword(F.col("referrer")))
              .withColumn("is_purchase", purchase(F.col("event_list")))
              .withColumn("revenue", revenue(F.col("product_list")))
        )

        searches = (
            enriched.filter(F.col("engine").isNotNull() & F.col("keyword").isNotNull())
                    .select(
                        F.col("ip").alias("s_ip"),
                        F.col("date_time").alias("s_time"),
                        F.col("engine"),
                        F.col("keyword")
                    )
        )

        purchases = (
            enriched.filter(F.col("is_purchase") & (F.col("revenue") > 0))
                    .select(
                        F.col("ip").alias("p_ip"),
                        F.col("date_time").alias("p_time"),
                        F.col("revenue")
                    )
        )

        joined = purchases.join(
            searches,
            (purchases["p_ip"] == searches["s_ip"]) &
            (searches["s_time"] <= purchases["p_time"]),
            "left"
        )

        w = Window.partitionBy("p_ip", "p_time", "revenue").orderBy(F.col("s_time").desc())

        result = (
            joined.withColumn("rn", F.row_number().over(w))
                  .filter(F.col("rn") == 1)
                  .filter(F.col("engine").isNotNull())
                  .groupBy(
                      F.col("engine").alias("Search Engine Domain"),
                      F.col("keyword").alias("Search Keyword")
                  )
                  .agg(F.round(F.sum("revenue"), 2).alias("Revenue"))
                  .orderBy(F.desc("Revenue"))
        )

        return result


try:
    source_path = f"s3://{BUCKET}/{INPUT_KEY}"
    log(f"Processing {source_path}")

    key_parts = INPUT_KEY.split("/")
    timestamp_path = "/".join(key_parts[1:-1]) if len(key_parts) >= 6 else ""

    summary_key = f"{LOG_PREFIX}{timestamp_path}/run-summary.json"
    pipeline_log_key = f"{LOG_PREFIX}{timestamp_path}/pipeline.log"
    metadata_key = f"{METADATA_PREFIX}{timestamp_path}/metadata-{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}.json"
    final_output_key = f"{PROCESSED_PREFIX}{timestamp_path}/{datetime.utcnow().strftime('%Y-%m-%d')}_SearchKeywordPerformance.tab"

    checksum = compute_checksum(BUCKET, INPUT_KEY)
    checksum_key = f"{CHECKSUM_PREFIX}{checksum}.json"

    if checksum_exists(checksum_key):
        upload_json(summary_key, {
            "status": "SKIPPED_NO_CHANGE",
            "source_key": INPUT_KEY,
            "checksum": checksum,
            "processed_at_utc": datetime.utcnow().isoformat()
        })
        upload_text(pipeline_log_key, "Pipeline skipped because source data has not changed.")
        log("File already processed. Skipping.")
        job.commit()
        sys.exit(0)

    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(source_path)
    )

    for c in df.columns:
        df = df.withColumn(c, F.col(c).cast("string"))

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)

    profiler = MetadataProfiler(config["expected_columns"])
    metadata = profiler.profile(df, INPUT_KEY)
    upload_json(metadata_key, metadata)

    processor = SearchKeywordRevenueProcessor()
    result = processor.transform(df)

    temp_prefix = f"{PROCESSED_PREFIX}{timestamp_path}/_tmp_output/"
    temp_s3_path = f"s3://{BUCKET}/{temp_prefix}"

    (
        result.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("sep", "\t")
        .csv(temp_s3_path)
    )

    move_single_part_file(temp_prefix, final_output_key)

    upload_json(checksum_key, {
        "checksum": checksum,
        "source_key": INPUT_KEY,
        "output_key": final_output_key,
        "processed_at_utc": datetime.utcnow().isoformat()
    })

    upload_json(summary_key, {
        "status": "SUCCESS",
        "source_key": INPUT_KEY,
        "checksum": checksum,
        "records_processed": df.count(),
        "output_rows": result.count(),
        "processed_at_utc": datetime.utcnow().isoformat()
    })

    upload_text(pipeline_log_key, "Pipeline completed successfully.")
    log("Pipeline finished successfully")
    job.commit()

except Exception as e:
    error_text = str(e)
    log(error_text)
    if "summary_key" in locals():
        upload_json(summary_key, {
            "status": "FAILED",
            "source_key": INPUT_KEY,
            "error": error_text,
            "processed_at_utc": datetime.utcnow().isoformat()
        })
    if "pipeline_log_key" in locals():
        upload_text(pipeline_log_key, error_text)
    raise
