#!/usr/bin/env python3
import argparse
import json
import sys
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


def load_dataframe(json_path: Path, is_json_lines: bool = False, orient: Optional[str] = None) -> pd.DataFrame:
    """Load a JSON file into a pandas DataFrame.

    Supports standard JSON (array of objects) and JSON Lines (one JSON object per line).
    """
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    if is_json_lines:
        df = pd.read_json(json_path, lines=True)
    else:
        # If orient is provided, respect it; otherwise let pandas infer from array of objects
        read_kwargs = {}
        if orient:
            read_kwargs["orient"] = orient
        with json_path.open("r", encoding="utf-8") as f:
            # Try to detect whether file contains a single JSON object or an array
            first_non_ws = None
            for ch in iter(lambda: f.read(1), ""):
                if not ch.isspace():
                    first_non_ws = ch
                    break
            f.seek(0)
            if first_non_ws == "{":
                # A single JSON object; wrap into list if it's record-like
                data = json.load(f)
                if isinstance(data, dict):
                    # If the dict looks like a record mapping, make a single-row frame
                    df = pd.DataFrame([data])
                else:
                    df = pd.DataFrame(data)
            else:
                df = pd.read_json(f, **read_kwargs)
    return df


def print_dataframe(df: pd.DataFrame, max_rows: Optional[int] = None) -> None:
    if max_rows is not None:
        with pd.option_context("display.max_rows", max_rows, "display.max_columns", None, "display.width", 0):
            print(df.to_string(index=False))
    else:
        with pd.option_context("display.max_rows", 50, "display.max_columns", None, "display.width", 0):
            print(df.to_string(index=False))


def op_info(df: pd.DataFrame) -> None:
    buffer = StringIO()
    df.info(buf=buffer)
    info_text = buffer.getvalue()
    print(info_text.rstrip())
    print("")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")


def op_head(df: pd.DataFrame, n: int) -> pd.DataFrame:
    return df.head(n)


def op_describe(df: pd.DataFrame, include_all: bool, percentiles: Optional[List[float]]) -> pd.DataFrame:
    include = "all" if include_all else None
    return df.describe(include=include, percentiles=percentiles)


def op_select(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"Columns not found in DataFrame: {missing}")
    return df[columns]


def op_filter(df: pd.DataFrame, query_expr: str) -> pd.DataFrame:
    try:
        return df.query(query_expr)
    except Exception as exc:
        raise ValueError(f"Invalid filter/query expression: {query_expr}\n{exc}")


def parse_aggregations(agg_items: List[str]) -> Dict[str, List[str]]:
    """Parse aggregation specs of the form 'column=func' or 'column=func1,func2'."""
    aggregations: Dict[str, List[str]] = {}
    for item in agg_items:
        if "=" not in item:
            raise ValueError(
                "Aggregation must be of the form 'column=func' or 'column=func1,func2'"
            )
        column, funcs = item.split("=", 1)
        func_list = [f.strip() for f in funcs.split(",") if f.strip()]
        if not func_list:
            raise ValueError(f"No aggregation functions provided for column '{column}'")
        aggregations[column] = func_list
    return aggregations


def op_groupby(df: pd.DataFrame, by: List[str], agg_items: List[str], dropna: bool) -> pd.DataFrame:
    for col in by:
        if col not in df.columns:
            raise KeyError(f"Group-by column not found: {col}")
    aggs = parse_aggregations(agg_items)
    grouped = df.groupby(by=by, dropna=dropna)
    result = grouped.agg(aggs)
    # Flatten MultiIndex columns if present
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = ["_".join([str(level) for level in col if level != ""]).strip("_") for col in result.columns.values]
    return result.reset_index()


def export_result(df: pd.DataFrame, export_path: Optional[Path], export_format: Optional[str]) -> None:
    if not export_path:
        return
    export_format = (export_format or "").lower()
    if export_format not in {"csv", "json", "parquet"}:
        raise ValueError("--export-format must be one of: csv, json, parquet")

    export_path = export_path.expanduser().resolve()
    export_path.parent.mkdir(parents=True, exist_ok=True)

    if export_format == "csv":
        df.to_csv(export_path, index=False)
    elif export_format == "json":
        df.to_json(export_path, orient="records", lines=False, force_ascii=False)
    elif export_format == "parquet":
        try:
            df.to_parquet(export_path, index=False)
        except Exception as exc:
            raise RuntimeError(
                "Failed to export to Parquet. Ensure 'pyarrow' is installed."
            ) from exc
    print(f"Exported result to {export_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Load a JSON file into a pandas DataFrame and run operations on it.\n"
            "Supports standard JSON (array of objects) or JSON Lines."
        )
    )
    parser.add_argument("--json", required=True, help="Path to JSON file")
    parser.add_argument(
        "--lines",
        action="store_true",
        help="Interpret input as JSON Lines (one JSON object per line)",
    )
    parser.add_argument(
        "--orient",
        default=None,
        help="pandas.read_json orient (rarely needed; e.g., 'records', 'split')",
    )
    parser.add_argument(
        "--export-to", default=None, help="Optional path to export the operation result"
    )
    parser.add_argument(
        "--export-format",
        default=None,
        choices=["csv", "json", "parquet"],
        help="Export format if --export-to is specified",
    )

    subparsers = parser.add_subparsers(dest="operation", required=True)

    # info
    subparsers.add_parser("info", help="Show DataFrame info and columns")

    # head
    p_head = subparsers.add_parser("head", help="Show the first N rows")
    p_head.add_argument("-n", "--num-rows", type=int, default=5, help="Number of rows")

    # describe
    p_describe = subparsers.add_parser("describe", help="Describe statistics")
    p_describe.add_argument("--all", action="store_true", dest="include_all", help="Include non-numeric columns")
    p_describe.add_argument(
        "--percentiles",
        nargs="*",
        type=float,
        default=None,
        help="Custom percentiles (e.g., 0.1 0.5 0.9)",
    )

    # select
    p_select = subparsers.add_parser("select", help="Select subset of columns")
    p_select.add_argument("columns", nargs="+", help="Column names")

    # filter
    p_filter = subparsers.add_parser("filter", help="Filter rows using pandas.query expression")
    p_filter.add_argument("query", help="Query expression, e.g., age > 30 and country == 'US'")

    # groupby
    p_groupby = subparsers.add_parser("groupby", help="Group by columns and aggregate")
    p_groupby.add_argument("--by", nargs="+", required=True, help="Columns to group by")
    p_groupby.add_argument(
        "--agg",
        nargs="+",
        required=True,
        help="Aggregation specs like column=sum or column=sum,mean",
    )
    p_groupby.add_argument(
        "--keepna",
        action="store_true",
        help="Keep NaN groups (by default they are dropped)",
    )

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    json_path = Path(args.json).expanduser()
    try:
        df = load_dataframe(json_path=json_path, is_json_lines=args.lines, orient=args.orient)
    except Exception as exc:
        print(f"Error loading JSON: {exc}", file=sys.stderr)
        return 2

    op = args.operation
    result_df: Optional[pd.DataFrame] = None

    try:
        if op == "info":
            op_info(df)
        elif op == "head":
            result_df = op_head(df, n=args.num_rows)
        elif op == "describe":
            result_df = op_describe(df, include_all=args.include_all, percentiles=args.percentiles)
        elif op == "select":
            result_df = op_select(df, columns=args.columns)
        elif op == "filter":
            result_df = op_filter(df, query_expr=args.query)
        elif op == "groupby":
            result_df = op_groupby(df, by=args.by, agg_items=args.agg, dropna=not args.keepna)
        else:
            parser.error(f"Unknown operation: {op}")
    except Exception as exc:
        print(f"Error running operation '{op}': {exc}", file=sys.stderr)
        return 3

    # Print and/or export result if we have one
    if result_df is not None:
        print_dataframe(result_df)
        try:
            export_result(result_df, export_path=Path(args.export_to) if args.export_to else None, export_format=args.export_format)
        except Exception as exc:
            print(f"Warning: failed to export result: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

