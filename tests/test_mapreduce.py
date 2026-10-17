"""Unit tests for the Hadoop Streaming mapper and reducer.

Both scripts read from stdin and write to stdout, so they are exercised as
subprocesses. No Hadoop cluster is required.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
STREAMING = ROOT / "streaming"
MAPPER = STREAMING / "mapper.py"
REDUCER = STREAMING / "reducer.py"

SAMPLE_ROWS = (
    "2012-01-01\t09:00\tSan Jose\tMen's Clothing\t214.05\tAmex\n"
    "2012-01-01\t09:00\tFort Worth\tWomen's Clothing\t153.57\tVisa\n"
    "2012-01-02\t10:15\tSan Jose\tMusic\t66.08\tCash\n"
)


def run(script: Path, stdin: str) -> list[str]:
    """Run a streaming script with ``stdin`` and return its stdout lines."""
    completed = subprocess.run(
        [sys.executable, str(script)],
        input=stdin,
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.splitlines()


def sort_by_key(lines: list[str]) -> list[str]:
    """Reproduce the shuffle/sort that Hadoop performs before the reducer."""
    return sorted(lines, key=lambda line: line.split("\t")[0])


def test_mapper_emits_one_per_city():
    assert run(MAPPER, SAMPLE_ROWS) == [
        "San Jose\t1",
        "Fort Worth\t1",
        "San Jose\t1",
    ]


@pytest.mark.parametrize(
    "line",
    ["", "\n", "only-one-field\n", "too\tfew\n"],
)
def test_mapper_ignores_malformed_lines(line):
    assert run(MAPPER, line) == []


def test_reducer_sums_a_single_key():
    assert run(REDUCER, "Paris\t1\n") == ["Paris\t1"]


def test_reducer_sums_consecutive_groups():
    data = "A\t1\nA\t1\nA\t1\nB\t1\nC\t1\nC\t1\n"
    assert run(REDUCER, data) == ["A\t3", "B\t1", "C\t2"]


def test_mapreduce_pipeline_counts_sales_per_city():
    mapped = run(MAPPER, SAMPLE_ROWS)
    reduced = run(REDUCER, "\n".join(sort_by_key(mapped)) + "\n")
    assert reduced == ["Fort Worth\t1", "San Jose\t2"]
