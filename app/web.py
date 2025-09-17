from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional
from uuid import uuid4

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import core


app = FastAPI(title="JSON → pandas GUI")

BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Simple in-memory stores for demo purposes
DATASETS: Dict[str, pd.DataFrame] = {}
RESULTS: Dict[str, pd.DataFrame] = {}


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    lines: bool = Form(False),
) -> RedirectResponse:
    try:
        # Save upload to a temporary path then load via core for consistency
        suffix = ".jsonl" if lines else ".json"
        tmp_path = BASE_DIR / f"upload_{uuid4().hex}{suffix}"
        content = await file.read()
        tmp_path.write_bytes(content)
        df = core.load_dataframe(tmp_path, is_json_lines=lines)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read JSON: {exc}")
    finally:
        try:
            if 'tmp_path' in locals() and tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass

    token = uuid4().hex
    DATASETS[token] = df
    RESULTS.pop(token, None)
    return RedirectResponse(url=f"/dataset/{token}", status_code=303)


@app.get("/dataset/{token}", response_class=HTMLResponse)
def dataset_page(request: Request, token: str) -> HTMLResponse:
    df = DATASETS.get(token)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    preview_html = df.head(20).to_html(index=False, border=0, classes="table table-striped")
    info_text = core.dataframe_info_text(df)
    result_df = RESULTS.get(token)
    result_html: Optional[str] = None
    if result_df is not None:
        result_html = result_df.to_html(index=False, border=0, classes="table table-striped")
    return templates.TemplateResponse(
        "dataset.html",
        {
            "request": request,
            "token": token,
            "columns": list(df.columns),
            "shape": df.shape,
            "preview_html": preview_html,
            "info_text": info_text,
            "result_html": result_html,
        },
    )


@app.post("/dataset/{token}/op/head")
def op_head(request: Request, token: str, num_rows: int = Form(5)) -> RedirectResponse:
    df = DATASETS.get(token)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    RESULTS[token] = core.op_head(df, n=num_rows)
    return RedirectResponse(url=f"/dataset/{token}", status_code=303)


@app.post("/dataset/{token}/op/describe")
def op_describe(
    request: Request,
    token: str,
    include_all: bool = Form(False),
    percentiles: str = Form(""),
) -> RedirectResponse:
    df = DATASETS.get(token)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    percentiles_list = None
    if percentiles.strip():
        try:
            percentiles_list = [float(x) for x in percentiles.replace(",", " ").split()]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid percentiles. Use numbers between 0 and 1.")
    RESULTS[token] = core.op_describe(df, include_all=include_all, percentiles=percentiles_list)
    return RedirectResponse(url=f"/dataset/{token}", status_code=303)


@app.post("/dataset/{token}/op/select")
def op_select(request: Request, token: str, columns: Optional[str] = Form(None)) -> RedirectResponse:
    df = DATASETS.get(token)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if not columns:
        raise HTTPException(status_code=400, detail="No columns provided")
    selected = [c for c in columns.split(",") if c.strip()]
    RESULTS[token] = core.op_select(df, columns=[c.strip() for c in selected])
    return RedirectResponse(url=f"/dataset/{token}", status_code=303)


@app.post("/dataset/{token}/op/filter")
def op_filter(request: Request, token: str, query: str = Form(...)) -> RedirectResponse:
    df = DATASETS.get(token)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    try:
        RESULTS[token] = core.op_filter(df, query_expr=query)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid filter expression: {exc}")
    return RedirectResponse(url=f"/dataset/{token}", status_code=303)


@app.post("/dataset/{token}/op/groupby")
def op_groupby(
    request: Request,
    token: str,
    by: str = Form(...),
    agg: str = Form(...),
    keepna: bool = Form(False),
) -> RedirectResponse:
    df = DATASETS.get(token)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    by_cols = [c.strip() for c in by.split(",") if c.strip()]
    agg_items = [a.strip() for a in agg.split("\n") if a.strip()]
    try:
        RESULTS[token] = core.op_groupby(df, by=by_cols, agg_items=agg_items, dropna=not keepna)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid groupby params: {exc}")
    return RedirectResponse(url=f"/dataset/{token}", status_code=303)


@app.get("/dataset/{token}/export")
def export(token: str, fmt: str) -> StreamingResponse:
    df = RESULTS.get(token) or DATASETS.get(token)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    fmt = fmt.lower()
    if fmt not in {"csv", "json", "parquet"}:
        raise HTTPException(status_code=400, detail="fmt must be csv, json, or parquet")
    try:
        data = core.export_dataframe(df, export_format=fmt)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    media_types = {
        "csv": "text/csv",
        "json": "application/json",
        "parquet": "application/octet-stream",
    }
    filename = f"result.{fmt}"
    return StreamingResponse(iter([data]), media_type=media_types[fmt], headers={
        "Content-Disposition": f"attachment; filename={filename}",
    })

