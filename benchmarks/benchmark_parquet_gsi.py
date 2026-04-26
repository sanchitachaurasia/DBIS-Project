#!/usr/bin/env python3
# python3 benchmarks/benchmark_parquet_gsi.py \
#   --dsn "host=localhost port=55432 dbname=dbis_project user=sanchita" \
#   --baseline-sql benchmarks/baseline_query.sql \
#   --indexed-sql benchmarks/indexed_query.sql \
#   --runs 5
import argparse
import re
import statistics
from pathlib import Path

import psycopg2


def read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip().rstrip(";")


def explain_analyze(cur, sql: str) -> tuple[list[str], float]:
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}")
    lines = [row[0] for row in cur.fetchall()]
    execution_time = None
    for line in lines:
        match = re.search(r"Execution Time: ([0-9.]+) ms", line)
        if match:
            execution_time = float(match.group(1))
            break
    if execution_time is None:
        raise RuntimeError("could not find Execution Time in EXPLAIN output")
    return lines, execution_time


def timed_query(cur, sql: str) -> float:
    _, execution_time = explain_analyze(cur, sql)
    return execution_time


def run_case(conn, sql: str, runs: int) -> list[dict]:
    results = []
    with conn.cursor() as cur:
        for _ in range(runs):
            lines, execution_time = explain_analyze(cur, sql)
            results.append({
                "timing_ms": execution_time,
                "plan_lines": lines,
            })
            conn.rollback()
    return results


def summarize(label: str, results: list[dict]) -> dict:
    timings = [result["timing_ms"] for result in results]
    return {
        "label": label,
        "runs": len(timings),
        "results": results,
        "timings_ms": timings,
        "avg_ms": statistics.mean(timings),
        "min_ms": min(timings),
        "max_ms": max(timings),
        "median_ms": statistics.median(timings),
    }


def markdown_report(baseline: dict, indexed: dict) -> str:
    improvement = baseline["avg_ms"] - indexed["avg_ms"]
    speedup = baseline["avg_ms"] / indexed["avg_ms"] if indexed["avg_ms"] else float("inf")

    def format_timings(stats: dict) -> str:
        return ", ".join(f"{timing:.3f}" for timing in stats["timings_ms"])

    def format_plan_lines(plan_lines: list[str]) -> str:
        return "\n".join(plan_lines)

    def render_case(stats: dict) -> list[str]:
        lines = [
            f"- runs: {stats['runs']}",
            f"- timings_ms: {format_timings(stats)}",
            f"- avg_ms: {stats['avg_ms']:.3f}",
            f"- median_ms: {stats['median_ms']:.3f}",
            f"- min_ms: {stats['min_ms']:.3f}",
            f"- max_ms: {stats['max_ms']:.3f}",
            "",
            "### Raw EXPLAIN ANALYZE Output",
        ]
        for index, result in enumerate(stats["results"], start=1):
            lines.extend([
                f"#### Run {index}",
                "```text",
                format_plan_lines(result["plan_lines"]),
                "```",
                "",
            ])
        return lines

    return "\n".join(
        [
            "# parquet_gsi EXPLAIN ANALYZE Benchmark Summary",
            "",
            "These timings come from `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` execution time.",
            "",
            "## Baseline",
            *render_case(baseline),
            "",
            "## Indexed",
            *render_case(indexed),
            "",
            "## Difference",
            f"- avg improvement_ms = baseline_avg_ms - indexed_avg_ms = {baseline['avg_ms']:.3f} - {indexed['avg_ms']:.3f} = {improvement:.3f}",
            f"- avg speedup_x = baseline_avg_ms / indexed_avg_ms = {baseline['avg_ms']:.3f} / {indexed['avg_ms']:.3f} = {speedup:.3f}",
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
