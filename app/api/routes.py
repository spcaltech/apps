import threading
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from huggingface_hub import HfApi, hf_hub_download
import shutil

router = APIRouter()

DATA_DIR = Path("/workspace/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


class FileEntry(BaseModel):
    path: str
    size: int


class ModelFilesResponse(BaseModel):
    files: List[FileEntry]


class PrefetchRequest(BaseModel):
    repo_id: str
    project_names: List[str]
    files: List[str]
    revision: Optional[str] = None


class PrefetchResponse(BaseModel):
    job_id: str


class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: float
    message: Optional[str] = None
    downloaded_files: int = 0
    total_files: int = 0


_jobs_lock = threading.Lock()
_jobs: Dict[str, JobStatus] = {}


@router.get("/model-files", response_model=ModelFilesResponse)
def list_model_files(repo_id: str, revision: Optional[str] = None) -> ModelFilesResponse:
    api = HfApi()
    try:
        tree = api.list_repo_tree(repo_id=repo_id, revision=revision, recursive=True)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Failed to list files for {repo_id}: {exc}")

    files: List[FileEntry] = []
    for item in tree:
        if getattr(item, "type", "file") == "file":
            path = getattr(item, "path", None) or getattr(item, "rpath", None)
            size = getattr(item, "size", 0) or 0
            if path and not path.startswith("."):
                files.append(FileEntry(path=path, size=int(size)))

    return ModelFilesResponse(files=files)


def _download_job(job_id: str, repo_id: str, files: List[str], project_names: List[str], revision: Optional[str]) -> None:
    with _jobs_lock:
        _jobs[job_id] = JobStatus(job_id=job_id, status="running", progress=0.0, downloaded_files=0, total_files=len(files))

    total = max(1, len(files) * max(1, len(project_names)))
    completed = 0

    try:
        for file_path in files:
            cached_file = hf_hub_download(repo_id=repo_id, filename=file_path, revision=revision, local_files_only=False)

            for project in project_names:
                target_dir = DATA_DIR / project / repo_id
                target_path = target_dir / file_path
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(cached_file, target_path)

                completed += 1
                with _jobs_lock:
                    job = _jobs[job_id]
                    job.downloaded_files = completed
                    job.progress = min(1.0, completed / total)
                    _jobs[job_id] = job

        with _jobs_lock:
            job = _jobs[job_id]
            job.status = "completed"
            job.message = "Prefetch complete"
            job.progress = 1.0
            _jobs[job_id] = job

    except Exception as exc:  # noqa: BLE001
        with _jobs_lock:
            job = _jobs[job_id]
            job.status = "failed"
            job.message = f"Error: {exc}"
            _jobs[job_id] = job


@router.post("/prefetch", response_model=PrefetchResponse)
def start_prefetch(req: PrefetchRequest, background_tasks: BackgroundTasks) -> PrefetchResponse:
    if not req.repo_id:
        raise HTTPException(status_code=422, detail="repo_id required")
    if not req.project_names:
        raise HTTPException(status_code=422, detail="At least one project name required")
    if not req.files:
        raise HTTPException(status_code=422, detail="At least one file to prefetch required")

    job_id = str(uuid.uuid4())
    background_tasks.add_task(_download_job, job_id, req.repo_id, req.files, req.project_names, req.revision)

    with _jobs_lock:
        _jobs[job_id] = JobStatus(job_id=job_id, status="queued", progress=0.0, downloaded_files=0, total_files=len(req.files) * max(1, len(req.project_names)))

    return PrefetchResponse(job_id=job_id)


@router.get("/status/{job_id}", response_model=JobStatus)
def get_status(job_id: str) -> JobStatus:
    with _jobs_lock:
        status = _jobs.get(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status