#!/usr/bin/env python3
import argparse
import statistics
import time
from pathlib import Path

import psycopg2


def read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def timed_query(cur, sql: str) -> float:
    start = time.perf_counter()
    cur.execute(sql)
    try:
        cur.fetchall()
    except psycopg2.ProgrammingError:
        pass
    return (time.perf_counter() - start) * 1000.0


def run_case(conn, sql: str, runs: int) -> list[float]:
    timings = []
    with conn.cursor() as cur:
        for _ in range(runs):
            timings.append(timed_query(cur, sql))
            conn.rollback()
    return timings


def summarize(label: str, timings: list[float]) -> dict:
    return {
        "label": label,
        "runs": len(timings),
        "avg_ms": statistics.mean(timings),
        "min_ms": min(timings),
        "max_ms": max(timings),
        "median_ms": statistics.median(timings),
    }


def markdown_report(baseline: dict, indexed: dict) -> str:
    improvement = baseline["avg_ms"] - indexed["avg_ms"]
    speedup = baseline["avg_ms"] / indexed["avg_ms"] if indexed["avg_ms"] else float("inf")
    return "\n".join(
        [
            "# parquet_gsi Benchmark Summary",
            "",
            "## Baseline",
            f"- runs: {baseline['runs']}",
            f"- avg_ms: {baseline['avg_ms']:.3f}",
            f"- median_ms: {baseline['median_ms']:.3f}",
            f"- min_ms: {baseline['min_ms']:.3f}",
            f"- max_ms: {baseline['max_ms']:.3f}",
            "",
            "## Indexed",
            f"- runs: {indexed['runs']}",
            f"- avg_ms: {indexed['avg_ms']:.3f}",
            f"- median_ms: {indexed['median_ms']:.3f}",
            f"- min_ms: {indexed['min_ms']:.3f}",
            f"- max_ms: {indexed['max_ms']:.3f}",
            "",
            "## Difference",
            f"- avg improvement_ms: {improvement:.3f}",
            f"- avg speedup_x: {speedup:.3f}",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark baseline vs parquet_gsi query paths.")
    parser.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    parser.add_argument("--baseline-sql", required=True, help="Path to baseline SQL file")
    parser.add_argument("--indexed-sql", required=True, help="Path to indexed SQL file")
    parser.add_argument("--runs", type=int, default=5, help="Number of timed runs")
    parser.add_argument("--output", default="benchmarks/parquet_gsi_summary.md", help="Markdown output path")
    args = parser.parse_args()

    baseline_sql = read_sql(args.baseline_sql)
    indexed_sql = read_sql(args.indexed_sql)

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False
    try:
        baseline = summarize("baseline", run_case(conn, baseline_sql, args.runs))
        indexed = summarize("indexed", run_case(conn, indexed_sql, args.runs))
    finally:
        conn.close()

    report = markdown_report(baseline, indexed)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report + "\n", encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
