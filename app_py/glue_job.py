import sys
import json
import hashlib
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ============================================================
# FIXED CONFIGURATION
# Only one runtime argument is accepted: --input_key
#
# Deployment note:
#   Pass metadata_config.json via --extra-files in the Glue job
#   definition so it is available on the local driver filesystem.
#   Example CLI:
#     --extra-files s3://analytics-hitdata-s3/config/metadata_config.json
# ============================================================
BUCKET = "analytics-hitdata-s3"
PROCESSED_PREFIX = "processed/"
METADATA_PREFIX = "metadata/"
LOGS_PREFIX = "logs/"
CHECKSUMS_PREFIX = "checksums/"
CONFIG_FILE = "metadata_config.json"

# Search engines that should be detected from the referrer.
# Add entries here to extend coverage without touching business logic.
SEARCH_ENGINE_PATTERNS = [
    "google.",
    "bing.",
    "search.yahoo.",
    "yahoo.",
    "search.msn.",
    "msn.",
    "duckduckgo.",
    "ask.",
]

# Query-string parameter names used by each engine family to carry the keyword.
# Checked in order; first match wins.
KEYWORD_PARAMS = {
    "yahoo.": "p",     # Yahoo uses p=
    "default": "q",    # Google, Bing, MSN, DDG, Ask all use q=
}


# ============================================================
# JOB ARGUMENTS
# Required runtime argument:
#   --input_key   raw/YYYY/MM/DD/HH-MM-SS/input_file.tab
# Optional runtime arguments:
#   --force       Set to "true" to bypass the checksum idempotency guard
#                 and always reprocess the file. Useful when re-running a
#                 job after a partial failure or during development/testing.
# ============================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_key"])
JOB_NAME  = args["JOB_NAME"]
INPUT_KEY = args["input_key"]

# --force is optional; getResolvedOptions raises if a key is missing so
# we inspect sys.argv directly.  Accepted forms:
#   --force          (presence alone means true)
#   --force true
#   --force True
def _flag(argv, name):
    if name not in argv:
        return False
    idx = argv.index(name)
    # If the next token exists and is not another flag, treat it as the value
    if idx + 1 < len(argv) and not argv[idx + 1].startswith("--"):
        return argv[idx + 1].strip().lower() == "true"
    return True  # bare flag with no value

FORCE_REPROCESS = _flag(sys.argv, "--force")


# ============================================================
# GLUE / SPARK SETUP
# ============================================================
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(JOB_NAME, args)

s3 = boto3.client("s3")

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "200")


# ============================================================
# SIMPLE PIPELINE LOGGER
# Logs go to CloudWatch via print() and also to S3 pipeline.log
# ============================================================
PIPELINE_LOG_LINES = []


def log(message: str) -> None:
    line = f"{datetime.utcnow().isoformat()} INFO {message}"
    PIPELINE_LOG_LINES.append(line)
    print(line)


def upload_json(key: str, payload: dict) -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json"
    )


def upload_text(key: str, text: str) -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/plain"
    )


# ============================================================
# HELPERS
# ============================================================
def load_metadata_config(file_name: str) -> dict:
    """
    Read the JSON config from the local Glue runtime filesystem.
    Glue copies --extra-files to the working directory, so a plain
    open() is sufficient. To read from S3 instead, swap for:

        obj = s3.get_object(Bucket=BUCKET, Key=<s3_key>)
        return json.loads(obj["Body"].read())
    """
    with open(file_name, "r", encoding="utf-8") as f:
        return json.load(f)


def compute_checksum(bucket: str, key: str) -> str:
    """
    Stream the S3 object in 8 MB chunks and return its SHA-256 hex digest.
    Streaming avoids loading the entire file into driver memory, making
    this safe even for files well beyond 10 GB.
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        raise ValueError(f"S3 source file not found: s3://{bucket}/{key}") from e

    hasher = hashlib.sha256()
    for chunk in iter(lambda: obj["Body"].read(8 * 1024 * 1024), b""):
        hasher.update(chunk)
    return hasher.hexdigest()


def checksum_exists(checksum_key: str) -> bool:
    """Return True if a checksum record already exists in S3 (idempotency guard)."""
    try:
        s3.head_object(Bucket=BUCKET, Key=checksum_key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def consolidate_output(temp_prefix: str, final_key: str) -> None:
    """
    Spark writes output as a directory of part files.  For the required
    single-file naming convention this helper copies the sole part file
    to the named destination and removes the temp directory.

    Scalability note:
        coalesce(1) forces all data through one executor before writing,
        which is acceptable for files up to ~1-2 GB.  For 10 GB+ files
        the recommended approach is to write partitioned output and use
        an S3 Batch Operation (or a post-processing Lambda) to rename/
        merge the parts, avoiding the single-executor bottleneck entirely.
    """
    paginator = s3.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(Bucket=BUCKET, Prefix=temp_prefix):
        objects.extend(page.get("Contents", []))

    part_key = next(
        (obj["Key"] for obj in objects if "part-" in obj["Key"]),
        None
    )

    if not part_key:
        raise ValueError(f"No Spark part file found under s3://{BUCKET}/{temp_prefix}")

    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": part_key},
        Key=final_key
    )

    delete_items = [{"Key": obj["Key"]} for obj in objects]
    if delete_items:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": delete_items})


def build_s3_keys(input_key: str) -> dict:
    """
    Derive all output S3 keys for this pipeline run.

    Output folder structure
    -----------------------
    The processed output always lands under a date-partitioned path built
    from the current UTC run date so the processed/ folder is always
    populated regardless of how the input key is structured:

        processed/YYYY/MM/DD/YYYY-MM-DD_SearchKeywordPerformance.tab

    Logs and metadata follow the same date partition so all artifacts for
    a given day are co-located:

        logs/YYYY/MM/DD/run-summary-YYYY-MM-DD-HH-MM-SS.json
        logs/YYYY/MM/DD/pipeline-YYYY-MM-DD-HH-MM-SS.log
        metadata/YYYY/MM/DD/metadata-log-YYYY-MM-DD-HH-MM-SS.json

    Every file carries the full run timestamp so multiple executions on
    the same day never overwrite each other.
    """
    now = datetime.utcnow()
    current_date = now.strftime("%Y-%m-%d")
    current_ts   = now.strftime("%Y-%m-%d-%H-%M-%S")

    # Date-partitioned folder: YYYY/MM/DD
    date_path = now.strftime("%Y/%m/%d")

    return {
        "date_path":          date_path,
        "metadata_key":       f"{METADATA_PREFIX}{date_path}/metadata-log-{current_ts}.json",
        "summary_key":        f"{LOGS_PREFIX}{date_path}/run-summary-{current_ts}.json",
        "pipeline_log_key":   f"{LOGS_PREFIX}{date_path}/pipeline-{current_ts}.log",
        "output_key":         f"{PROCESSED_PREFIX}{date_path}/{current_date}_SearchKeywordPerformance.tab",
        "temp_output_prefix": f"{PROCESSED_PREFIX}{date_path}/_tmp_output_{current_ts}/",
    }


# ============================================================
# METADATA PROFILER CLASS
# ============================================================
class MetadataProfiler:
    """
    Profiles delimited text source metadata using Spark aggregations.

    Supported expected_columns config formats:
        1) ["col1", "col2", "col3"]
        2) [{"name": "col1", "type": "int"}, {"name": "col2", "type": "datetime"}]

    Because the source file is read as raw text, all actual values are
    treated as strings.  Expected types come from config only and are
    reported alongside the profiled type for downstream validation.

    Performance:
        - Min/max/null stats are computed in a single distributed agg pass.
        - Distinct counts are computed in a second pass (Spark cannot fuse
          countDistinct with other agg expressions on very large datasets).
        - Both passes use collect()[0] which is safe because aggregation
          reduces the result to a single row regardless of source size.
        - No Python UDFs are used; all logic runs natively in Spark.
    """

    def __init__(self, expected_columns: list):
        self.expected_columns = expected_columns or []
        self.expected_names: list = []
        self.expected_defs: dict = {}

        for col in self.expected_columns:
            if isinstance(col, str):
                self.expected_names.append(col)
                self.expected_defs[col] = {"type": "string"}
            elif isinstance(col, dict) and col.get("name"):
                name = col["name"]
                self.expected_names.append(name)
                self.expected_defs[name] = {"type": col.get("type", "string")}

    def _detect_mismatches(self, columns: list) -> list:
        """Compare actual columns against the expected schema definition."""
        if not self.expected_names:
            return []

        mismatches = []
        missing = [c for c in self.expected_names if c not in columns]
        extra = [c for c in columns if c not in self.expected_names]

        if missing:
            mismatches.append({"type": "missing_columns", "columns": missing})
        if extra:
            mismatches.append({"type": "extra_columns", "columns": extra})
        if len(columns) != len(self.expected_names):
            mismatches.append({
                "type": "column_count_mismatch",
                "expected": len(self.expected_names),
                "actual": len(columns),
            })
        if list(columns) != self.expected_names:
            mismatches.append({
                "type": "column_order_mismatch",
                "expected_order": self.expected_names,
                "actual_order": list(columns),
            })

        return mismatches

    def profile(self, df, source_key: str) -> dict:
        """
        Run distributed aggregations over df and return a metadata report dict.
        The caller is responsible for caching df before calling this method
        if the DataFrame will be reused afterward.
        """
        columns = df.columns
        total_rows = df.count()

        mismatches = self._detect_mismatches(columns)

        # --- Pass 1: min / max / null counts (single action) ---
        agg_exprs = []
        for col_name in columns:
            agg_exprs.extend([
                F.min(F.col(col_name)).alias(f"{col_name}__min"),
                F.max(F.col(col_name)).alias(f"{col_name}__max"),
                F.sum(
                    F.when(
                        F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
                        1
                    ).otherwise(0)
                ).alias(f"{col_name}__nulls"),
            ])

        stats_row = df.agg(*agg_exprs).collect()[0].asDict()

        # --- Pass 2: distinct counts (separate pass required by Spark) ---
        distinct_exprs = [
            F.countDistinct(F.col(col_name)).alias(col_name)
            for col_name in columns
        ]
        distinct_row = df.agg(*distinct_exprs).collect()[0].asDict()

        profiles = [
            {
                "column_name": col_name,
                "actual_profiled_data_type": "string",
                "expected_data_type": self.expected_defs.get(col_name, {}).get("type", "string"),
                "min_value": stats_row.get(f"{col_name}__min"),
                "max_value": stats_row.get(f"{col_name}__max"),
                "total_nulls": int(stats_row.get(f"{col_name}__nulls", 0)),
                "total_unique_values": int(distinct_row.get(col_name, 0)),
            }
            for col_name in columns
        ]

        return {
            "source_bucket": BUCKET,
            "source_key": source_key,
            "profiled_at_utc": datetime.utcnow().isoformat(),
            "file_type": "delimited_text",
            "delimiter": "\t",
            "number_of_columns": len(columns),
            "column_names": list(columns),
            "total_rows": total_rows,
            "metadata_mismatch": len(mismatches) > 0,
            "metadata_mismatch_details": mismatches,
            "columns": profiles,
        }


# ============================================================
# BUSINESS PROCESSOR CLASS
# ============================================================
class SearchKeywordRevenueProcessor:
    """
    Answers the client business question:

        How much revenue is the client getting from external Search Engines
        (Google, Yahoo, Bing/MSN, etc.) and which keywords are performing
        the best based on revenue?

    Attribution logic (last-touch, per-session):
        1. Identify rows where the referrer is a known external search engine
           and extract the search keyword from the query string.
        2. Identify rows where event_list contains event code "1" (Purchase)
           and extract revenue from product_list (4th semicolon-delimited
           field per product, summed across all products in the row).
        3. For each purchase, attribute it to the most recent search engine
           referral from the same IP address that occurred before the purchase.
        4. Aggregate total revenue by (search_engine_domain, search_keyword)
           and return results sorted descending by revenue.

    Performance notes:
        - All transformations use native Spark/SQL functions (no Python UDFs)
          so the full computation runs distributed across the cluster.
        - Revenue extraction uses Spark's aggregate() higher-order function,
          which avoids explode()+groupBy overhead for the product_list column.
        - The Window function for last-touch attribution partitions by
          (ip, purchase_time, revenue) which limits shuffle size.
        - df is expected to already be cached by the caller.
    """

    # Supported search engine domain substrings, in match-priority order.
    SEARCH_ENGINE_PATTERNS = SEARCH_ENGINE_PATTERNS

    def __init__(self):
        pass

    @staticmethod
    def _build_search_engine_column(df):
        """
        Identify external search engine domain from the lowercased referrer.
        Returns df with a new 'search_engine_domain' column (null if not a
        recognised search engine).

        The domain patterns list is driven by the module-level constant so
        new engines can be added without touching business logic.
        """
        condition = None
        for pattern in SEARCH_ENGINE_PATTERNS:
            clause = F.when(
                F.col("referrer_domain").contains(pattern),
                F.col("referrer_domain")
            )
            condition = clause if condition is None else condition.when(
                F.col("referrer_domain").contains(pattern),
                F.col("referrer_domain")
            )

        # Build the chain using F.when properly
        expr = F.when(F.col("referrer_domain").contains(SEARCH_ENGINE_PATTERNS[0]),
                      F.col("referrer_domain"))
        for pattern in SEARCH_ENGINE_PATTERNS[1:]:
            expr = expr.when(F.col("referrer_domain").contains(pattern),
                             F.col("referrer_domain"))

        return df.withColumn("search_engine_domain", expr)

    @staticmethod
    def _build_keyword_column(df):
        """
        Extract the search keyword from the referrer query string.
        Yahoo uses p=; all other engines use q=.
        URL-encoded '+' characters are normalised to spaces.
        """
        keyword_raw = (
            F.when(
                F.col("referrer_domain").contains("yahoo."),
                F.regexp_extract(F.col("referrer_lc"), r"[?&]p=([^&]+)", 1)
            ).otherwise(
                F.regexp_extract(F.col("referrer_lc"), r"[?&]q=([^&]+)", 1)
            )
        )

        keyword = F.when(
            F.trim(keyword_raw) != "",
            F.lower(F.regexp_replace(keyword_raw, r"\+", " "))
        )

        return df.withColumn("search_keyword", keyword)

    @staticmethod
    def _build_revenue_column(df):
        """
        Sum revenue across all products in product_list.

        product_list format (comma-delimited products, semicolon-delimited fields):
            category;product_name;qty;revenue;custom_events;merch_evar,...

        Revenue is the 4th semicolon-delimited token (1-indexed → element_at pos 4).
        Revenue is only meaningful when event code "1" (Purchase) is present in
        event_list; filtering to purchase rows is handled by the caller.
        """
        return df.withColumn(
            "revenue",
            F.expr("""
                aggregate(
                    filter(
                        split(coalesce(product_list, ''), ','),
                        x -> x is not null and trim(x) <> ''
                    ),
                    cast(0.0 as double),
                    (acc, x) -> acc + coalesce(
                        try_cast(element_at(split(x, ';'), 4) as double),
                        0.0
                    )
                )
            """)
        )

    def transform(self, df):
        """
        Execute the full search keyword revenue attribution pipeline.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Raw source DataFrame.  Expected to be cached by the caller.

        Returns
        -------
        pyspark.sql.DataFrame
            Aggregated result with columns:
                Search Engine Domain | Search Keyword | Revenue
            Sorted descending by Revenue.
        """
        required_cols = ["ip", "referrer", "event_list", "product_list", "date_time"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"Missing required columns for business processing: {missing}"
            )

        # ---- Base enrichment ------------------------------------------------
        base_df = (
            df
            # Parse timestamp for chronological ordering (not lexicographic)
            .withColumn("event_ts", F.to_timestamp(F.col("date_time")))
            # Lowercase + trim referrer once; reused for all downstream extractions
            .withColumn("referrer_lc", F.lower(F.trim(F.col("referrer"))))
            # Strip whitespace from event_list so "1" matching is reliable
            .withColumn(
                "event_list_clean",
                F.regexp_replace(F.coalesce(F.col("event_list"), F.lit("")), r"\s+", "")
            )
        )

        # Extract host from referrer (handles http://, https://, and bare domains)
        base_df = base_df.withColumn(
            "referrer_domain",
            F.lower(
                F.regexp_extract(F.col("referrer_lc"), r"^(?:https?://)?([^/?#]+)", 1)
            )
        )

        # Identify search engine, extract keyword, compute revenue
        base_df = self._build_search_engine_column(base_df)
        base_df = self._build_keyword_column(base_df)

        # Purchase = event code "1".  Split by comma so "1" does not match "10"/"100".
        base_df = base_df.withColumn(
            "is_purchase",
            F.array_contains(F.split(F.col("event_list_clean"), ","), "1")
        )

        base_df = self._build_revenue_column(base_df)

        # Cache the enriched base so both branches below share it
        enriched = base_df.cache()

        try:
            # ---- Search touchpoints -----------------------------------------
            # All rows that originated from a recognised external search engine
            search_touchpoints = (
                enriched
                .filter(
                    F.col("search_engine_domain").isNotNull()
                    & F.col("search_keyword").isNotNull()
                    & F.col("event_ts").isNotNull()
                )
                .select(
                    F.col("ip").alias("s_ip"),
                    F.col("event_ts").alias("s_time"),
                    F.col("search_engine_domain"),
                    F.col("search_keyword"),
                )
            )

            # ---- Purchase rows ----------------------------------------------
            purchases = (
                enriched
                .filter(
                    F.col("is_purchase")
                    & (F.col("revenue") > 0)
                    & F.col("event_ts").isNotNull()
                )
                .select(
                    F.col("ip").alias("p_ip"),
                    F.col("event_ts").alias("p_time"),
                    F.col("revenue"),
                )
            )

            # ---- Last-touch attribution via Window --------------------------
            # Join purchases to all preceding search touchpoints for the same IP,
            # then pick the most recent touchpoint per purchase (last-touch).
            joined = purchases.join(
                search_touchpoints,
                (purchases["p_ip"] == search_touchpoints["s_ip"])
                & (search_touchpoints["s_time"] <= purchases["p_time"]),
                "left",
            )

            window_spec = (
                Window
                .partitionBy("p_ip", "p_time", "revenue")
                .orderBy(F.col("s_time").desc())
            )

            result = (
                joined
                .withColumn("rn", F.row_number().over(window_spec))
                .filter(F.col("rn") == 1)
                .filter(F.col("search_engine_domain").isNotNull())
                .groupBy(
                    F.col("search_engine_domain").alias("Search Engine Domain"),
                    F.col("search_keyword").alias("Search Keyword"),
                )
                .agg(F.round(F.sum("revenue"), 2).alias("Revenue"))
                .orderBy(F.desc("Revenue"))
            )

        finally:
            # Always release the enriched cache, even if an exception occurs
            enriched.unpersist()

        return result


# ============================================================
# MAIN PIPELINE
# ============================================================
def main(keys: dict):
    log(f"Starting Glue job — input: s3://{BUCKET}/{INPUT_KEY}")
    log(f"Config file: {CONFIG_FILE}")

    source_path = f"s3://{BUCKET}/{INPUT_KEY}"

    # ------------------------------------------------------------------
    # Step 0: Checksum — skip unchanged source files (idempotency guard)
    #
    # The SHA-256 of the source file is compared against a registry in S3.
    # If a matching entry exists the file was already processed and we skip
    # to avoid duplicate output.
    #
    # Override: pass --force true as a Glue job argument to bypass this
    # check and force reprocessing (useful after partial failures or during
    # development / testing).
    # ------------------------------------------------------------------
    checksum = compute_checksum(BUCKET, INPUT_KEY)
    checksum_key = f"{CHECKSUMS_PREFIX}{checksum}.json"
    log(f"Source SHA-256 checksum: {checksum}")
    log(f"Force-reprocess flag   : {FORCE_REPROCESS}")

    already_processed = checksum_exists(checksum_key)

    if already_processed and not FORCE_REPROCESS:
        skipped_summary = {
            "status": "SKIPPED_NO_CHANGE",
            "source_bucket": BUCKET,
            "source_key": INPUT_KEY,
            "checksum": checksum,
            "checksum_registry_key": checksum_key,
            "hint": "Pass --force true to reprocess this file regardless.",
            "processed_at_utc": datetime.utcnow().isoformat(),
        }
        upload_json(keys["summary_key"], skipped_summary)
        log(
            f"Pipeline skipped — checksum already registered at "
            f"s3://{BUCKET}/{checksum_key}. Pass --force true to reprocess."
        )
        upload_text(keys["pipeline_log_key"], "\n".join(PIPELINE_LOG_LINES))
        job.commit()
        return

    if already_processed and FORCE_REPROCESS:
        log(
            f"WARNING: Checksum already registered at s3://{BUCKET}/{checksum_key} "
            f"but --force is set — reprocessing anyway."
        )

    # ------------------------------------------------------------------
    # Step 1: Read source file as tab-delimited text
    #
    # Key reader options:
    #   sep=\t         — tab-delimited as per spec
    #   quote="        — handle quoted fields that contain commas (e.g. user_agent)
    #   multiLine=true — allow quoted fields that span multiple lines
    #   escape="       — doubled-quote escape inside quoted fields
    #   encoding=UTF-8 — explicit charset
    # ------------------------------------------------------------------
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("encoding", "UTF-8")
        .csv(source_path)
    )

    # Cast all columns to string for consistent metadata profiling
    for col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast("string"))

    # Cache once; shared by profiler and processor
    df = df.cache()
    source_row_count = df.count()

    log(f"Source file read successfully. Columns: {len(df.columns)}, Rows: {source_row_count}")

    # ------------------------------------------------------------------
    # Step 2: Metadata profiling
    # ------------------------------------------------------------------
    config = load_metadata_config(CONFIG_FILE)
    profiler = MetadataProfiler(config.get("expected_columns", []))
    metadata_report = profiler.profile(df, INPUT_KEY)
    upload_json(keys["metadata_key"], metadata_report)
    log(f"Metadata report written → s3://{BUCKET}/{keys['metadata_key']}")

    if metadata_report["metadata_mismatch"]:
        log(f"WARNING: Schema mismatches detected: {metadata_report['metadata_mismatch_details']}")

    # ------------------------------------------------------------------
    # Step 3: Business transformation
    # ------------------------------------------------------------------
    processor = SearchKeywordRevenueProcessor()
    result_df = processor.transform(df).cache()
    output_row_count = result_df.count()
    log(f"Business transformation complete. Output rows: {output_row_count}")

    # Release source cache — no longer needed after transform
    df.unpersist()

    # ------------------------------------------------------------------
    # Step 4: Write output
    #
    # Scalability note:
    #   coalesce(1) is used to satisfy the single-file naming requirement.
    #   For files > ~2 GB this becomes a bottleneck because all data is
    #   funnelled through one executor before writing.  At 10 GB+ scale,
    #   consider writing partitioned output (no coalesce) and using an
    #   S3 Batch Operation or a post-job Lambda to merge/rename the parts.
    # ------------------------------------------------------------------
    temp_output_s3 = f"s3://{BUCKET}/{keys['temp_output_prefix']}"

    (
        result_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("sep", "\t")
        .csv(temp_output_s3)
    )

    result_df.unpersist()

    consolidate_output(keys["temp_output_prefix"], keys["output_key"])
    log(f"Final output written → s3://{BUCKET}/{keys['output_key']}")

    # ------------------------------------------------------------------
    # Step 5: Register checksum (prevents reprocessing same file twice)
    # ------------------------------------------------------------------
    upload_json(checksum_key, {
        "checksum": checksum,
        "source_bucket": BUCKET,
        "source_key": INPUT_KEY,
        "output_key": keys["output_key"],
        "metadata_key": keys["metadata_key"],
        "first_processed_at_utc": datetime.utcnow().isoformat(),
    })

    # ------------------------------------------------------------------
    # Step 6: Write run summary
    # ------------------------------------------------------------------
    summary = {
        "status": "SUCCESS",
        "source_bucket": BUCKET,
        "source_key": INPUT_KEY,
        "checksum": checksum,
        "records_processed": source_row_count,
        "output_rows": output_row_count,
        "processed_at_utc": datetime.utcnow().isoformat(),
    }
    upload_json(keys["summary_key"], summary)

    # ------------------------------------------------------------------
    # Step 7: Flush pipeline log to S3
    # ------------------------------------------------------------------
    log("Glue pipeline completed successfully.")
    upload_text(keys["pipeline_log_key"], "\n".join(PIPELINE_LOG_LINES))

    job.commit()


# ============================================================
# ENTRY POINT
# S3 keys are built once here so the error handler reuses the
# exact same timestamped paths that main() used — not a second
# set of keys generated at a later timestamp.
# ============================================================
_run_keys = build_s3_keys(INPUT_KEY)

try:
    main(_run_keys)
except Exception as exc:
    error_text = str(exc)
    log(f"Pipeline FAILED: {error_text}")

    try:
        upload_json(_run_keys["summary_key"], {
            "status": "FAILED",
            "source_bucket": BUCKET,
            "source_key": INPUT_KEY,
            "error": error_text,
            "processed_at_utc": datetime.utcnow().isoformat(),
        })
        upload_text(_run_keys["pipeline_log_key"], "\n".join(PIPELINE_LOG_LINES))
    except Exception:
        pass  # Best-effort log upload; do not mask the original error

    raise
