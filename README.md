## JSON → pandas CLI

Load a JSON file into a pandas DataFrame and run common operations (info, head, describe, select, filter, groupby), with optional export to CSV/JSON/Parquet.

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Sample data

There is a small sample at `data/sample.json` (array of objects).

### Usage

General form:

```bash
python app/cli.py --json data/sample.json <operation> [args] [--export-to out.csv --export-format csv]
```

Operations:

- **info**: Show DataFrame info
  ```bash
  python app/cli.py --json data/sample.json info
  ```

- **head**: First N rows
  ```bash
  python app/cli.py --json data/sample.json head -n 3
  ```

- **describe**: Stats (use `--all` to include non-numeric)
  ```bash
  python app/cli.py --json data/sample.json describe --all --percentiles 0.1 0.5 0.9
  ```

- **select**: Choose columns
  ```bash
  python app/cli.py --json data/sample.json select id name spend
  ```

- **filter**: Query rows (pandas.query syntax)
  ```bash
  python app/cli.py --json data/sample.json filter "age >= 30 and country == 'US'"
  ```

- **groupby**: Group and aggregate
  ```bash
  # Sum spend per country
  python app/cli.py --json data/sample.json groupby --by country --agg spend=sum

  # Multiple aggs on multiple columns
  python app/cli.py --json data/sample.json groupby --by country --agg spend=sum,mean --agg age=mean
  ```

### JSON Lines input

For JSON Lines (one object per line), pass `--lines`:

```bash
python app/cli.py --json data/events.jsonl --lines head -n 10
```

### Exporting results

Add `--export-to` with `--export-format` (csv|json|parquet):

```bash
python app/cli.py --json data/sample.json select id name --export-to out.csv --export-format csv
```

### Notes

- For Parquet export, `pyarrow` is required (already in requirements).
- `filter` uses `pandas.DataFrame.query`; quote strings as in the examples.
# Model Prefetcher

A minimal web app to list files in a Hugging Face model repo and prefetch selected files into project directories. Useful for preparing assets for multiple projects.

## Requirements
- Python 3.13+

## Setup
Install dependencies:

```bash
pip3 install --break-system-packages -r requirements.txt
```

## Run
Start the server:

```bash
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open the UI at `http://127.0.0.1:8000/`.

## Usage
1. Enter a Hugging Face repo id (e.g. `bert-base-uncased` or `meta-llama/Llama-2-7b-hf`).
2. Click "Load model files" to fetch the file list.
3. Enter one or more project names (comma-separated), e.g. `projA,projB`.
4. Select files to prefetch (use "Select recommended" for common model artifacts).
5. Click "Start prefetch". Progress and status will display live.

Files are copied into `data/<project>/<repo_id>/...`.

## API
- GET `/api/model-files?repo_id=...&revision=...` → `{ files: [{path, size}] }`
- POST `/api/prefetch` with JSON `{ repo_id, revision?, project_names[], files[] }` → `{ job_id }`
- GET `/api/status/{job_id}` → `{ job_id, status, progress, message, downloaded_files, total_files }`

## Notes
- Private repos require authentication; set the `HF_TOKEN` environment variable and the library will pick it up. For now, this app relies on your environment being already authenticated.
- Large files may take time to download; the app copies from the local cache to each project.
