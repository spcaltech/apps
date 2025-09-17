import json
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


def load_dataframe(json_path: Path, is_json_lines: bool = False, orient: Optional[str] = None) -> pd.DataFrame:
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    if is_json_lines:
        df = pd.read_json(json_path, lines=True)
    else:
        read_kwargs = {}
        if orient:
            read_kwargs["orient"] = orient
        with json_path.open("r", encoding="utf-8") as f:
            first_non_ws = None
            for ch in iter(lambda: f.read(1), ""):
                if not ch.isspace():
                    first_non_ws = ch
                    break
            f.seek(0)
            if first_non_ws == "{":
                data = json.load(f)
                if isinstance(data, dict):
                    df = pd.DataFrame([data])
                else:
                    df = pd.DataFrame(data)
            else:
                df = pd.read_json(f, **read_kwargs)
    return df


def dataframe_info_text(df: pd.DataFrame) -> str:
    buffer = StringIO()
    df.info(buf=buffer)
    info_text = buffer.getvalue().rstrip()
    extra = f"\n\nShape: {df.shape}\nColumns: {list(df.columns)}"
    return info_text + extra


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
    return df.query(query_expr)


def parse_aggregations(agg_items: List[str]) -> Dict[str, List[str]]:
    aggregations: Dict[str, List[str]] = {}
    for item in agg_items:
        if "=" not in item:
            raise ValueError("Aggregation must be 'column=func' or 'column=func1,func2'")
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
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = ["_".join([str(level) for level in col if level != ""]).strip("_") for col in result.columns.values]
    return result.reset_index()


def export_dataframe(df: pd.DataFrame, export_format: str) -> bytes:
    fmt = export_format.lower()
    if fmt == "csv":
        return df.to_csv(index=False).encode("utf-8")
    if fmt == "json":
        return df.to_json(orient="records", lines=False, force_ascii=False).encode("utf-8")
    if fmt == "parquet":
        # Return bytes; pandas requires a file-like buffer for parquet
        import io

        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return buf.getvalue()
    raise ValueError("export_format must be csv, json, or parquet")

