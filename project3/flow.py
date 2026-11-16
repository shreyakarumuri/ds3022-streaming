"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python bluesky_flow.py
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))       # "raw" or "sample"
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"bluesky{TAG}.duckdb"                     
CHART = f"charts/reply_percentage_by_language{TAG}.png"  

@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_posts
            WHERE action <> 'malformed'
              AND kind = 'commit'                   -- not identity or account events
              AND did IS NOT NULL AND rkey IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_posts WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of bluesky.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]

@task
def entities():
    """Kind 2: one row per post (did, rkey), with its latest operation."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT did, rkey, operation, lang, text,
                   make_timestamp(time_us) AS event_time,  -- microseconds -> UTC
                   (raw::JSON ->> '$.data.commit.record.reply.parent.uri')
                       IS NOT NULL AS is_reply  -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY did, rkey
                                       ORDER BY time_us DESC, kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM posts").fetchone()[0]


@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("""
            SELECT lang, reply_percentage, total_posts
            FROM language_comparison
            ORDER BY reply_percentage DESC
        """).fetchall()

    os.makedirs("charts", exist_ok=True)

        # Show the 10 languages with the most posts
    rows = sorted(rows, key=lambda r: r[2], reverse=True)[:10]

    # Sort those languages by reply percentage for the chart
    rows = sorted(rows, key=lambda r: r[1])

    languages = [r[0] for r in rows]
    percentages = [r[1] for r in rows]
    counts = [r[2] for r in rows]

    plt.figure(figsize=(10, 7))
    bars = plt.barh(languages, percentages)

    plt.title("Reply Rates Differ Across Languages")
    plt.xlabel("Proportion of posts that are replies (%)")
    plt.ylabel("Language")

    for bar, percentage, count in zip(bars, percentages, counts):
        plt.text(
        bar.get_width() + 0.3,
        bar.get_y() + bar.get_height() / 2,
        f"{percentage:.1f}%  (n={count:,})",
        va="center",
        fontsize=9
    )

    plt.xlim(0, max(percentages) * 1.25)
    plt.tight_layout()
    plt.savefig(CHART, dpi=150, bbox_inches="tight")    
    plt.close()
    return CHART


@task
def marts():
    with duckdb.connect(DB) as con: 
        con.execute("""
            CREATE OR REPLACE TABLE language_comparison AS
            SELECT
                lang,
                COUNT(*) AS total_posts,
                COUNT(*) FILTER (WHERE is_reply) AS replies,
                COUNT(*) FILTER (WHERE NOT is_reply) AS original_posts,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE is_reply)
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS reply_percentage
            FROM posts
            WHERE lang IS NOT NULL
            GROUP BY lang
            HAVING COUNT(*) >= 100
            ORDER BY reply_percentage DESC
        """)

@flow(name="bluesky-pipeline")
def pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    things = entities()
    marts()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique Bluesky Posts:             %d", things)
    log.info("Chart: %s", path)
# at the end of the file: if __name__ == "__main__": pipeline()

if __name__ == "__main__":
    pipeline()
