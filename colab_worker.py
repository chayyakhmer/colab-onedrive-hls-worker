import subprocess
import time
import shutil
import os, time, shutil, subprocess, threading, traceback
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import quote

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

APP_PORT = int(os.getenv("PORT", "2323"))
WORK_DIR = Path(os.getenv("WORK_DIR", "/content/transcode_jobs"))
INPUT_DIR = WORK_DIR / "input"
OUTPUT_DIR = WORK_DIR / "output"

MS_CLIENT_ID = os.getenv("MS_CLIENT_ID", "").strip()
MS_CLIENT_SECRET = os.getenv("MS_CLIENT_SECRET", "").strip()
MS_TENANT = os.getenv("MS_TENANT", "consumers").strip()
MS_REFRESH_TOKEN = os.getenv("MS_REFRESH_TOKEN", "").strip()
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

app = FastAPI(title="Colab OneDrive HLS Worker")


# In-memory background job registry. Good for Colab sessions.
# Restarting the notebook clears this registry, but files remain under WORK_DIR.
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

def now_ts() -> float:
    return time.time()

def set_job(job_id: str, **updates):
    with JOBS_LOCK:
        rec = JOBS.setdefault(job_id, {})
        rec.update(updates)
        rec["updated_at"] = now_ts()
        return dict(rec)

def get_job(job_id: str) -> Dict[str, Any]:
    with JOBS_LOCK:
        return dict(JOBS.get(job_id, {}))

def path_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for f in path.rglob("*"):
        if f.is_file():
            try:
                total += f.stat().st_size
            except FileNotFoundError:
                pass
    return total

def count_segments(path: Path) -> int:
    return len(list(path.glob("seg_*.ts"))) if path.exists() else 0

class HlsJob(BaseModel):
    job_id: str
    source_path: Optional[str] = None
    download_url: Optional[str] = None
    output_onedrive_folder: str
    delete_temp_after: bool = True
    delete_original_after_success: bool = False
    video_bitrate: str = "2500k"
    audio_bitrate: str = "128k"
    hls_time: int = 6

class UploadOnlyJob(BaseModel):
    job_id: str
    source_path: Optional[str] = None
    download_url: Optional[str] = None
    output_onedrive_folder: str
    delete_temp_after: bool = True


class ProgressiveHlsJob(HlsJob):
    """
    Progressive mode:
    - FFmpeg writes pre_master.m3u8 while running.
    - Uploader uploads finished .ts files while FFmpeg is still running.
    - pre_master.m3u8 is uploaded repeatedly.
    - master.m3u8 is uploaded only after FFmpeg finishes.
    """
    upload_poll_sec: float = 2.0
    pre_master_upload_every_sec: float = 8.0
    stable_checks: int = 2

def require_env():
    missing = []
    for name, value in {"MS_CLIENT_ID": MS_CLIENT_ID, "MS_CLIENT_SECRET": MS_CLIENT_SECRET, "MS_REFRESH_TOKEN": MS_REFRESH_TOKEN}.items():
        if not value:
            missing.append(name)
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env: {', '.join(missing)}")

def get_access_token() -> str:
    require_env()
    url = f"https://login.microsoftonline.com/{MS_TENANT}/oauth2/v2.0/token"
    data = {
        "client_id": MS_CLIENT_ID,
        "client_secret": MS_CLIENT_SECRET,
        "refresh_token": MS_REFRESH_TOKEN,
        "grant_type": "refresh_token",
        "scope": "offline_access User.Read Files.ReadWrite",
    }
    r = requests.post(url, data=data, timeout=60)
    if not r.ok:
        raise HTTPException(status_code=500, detail=f"Token refresh failed: {r.text[:500]}")
    return r.json()["access_token"]

def graph_headers(token: str, json_type: bool = False) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    if json_type:
        headers["Content-Type"] = "application/json"
    return headers

def normalize_path(path: str) -> str:
    path = path.strip()
    return path if path.startswith("/") else "/" + path

def graph_path(path: str) -> str:
    return quote(normalize_path(path), safe="/._-()[] ")

def download_url_to_file(url: str, dest: Path, headers: Optional[dict] = None) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, headers=headers or {}, stream=True, timeout=120) as r:
        if not r.ok:
            raise HTTPException(status_code=500, detail=f"Download failed: {r.status_code} {r.text[:300]}")
        total = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
        return total


def download_from_url(url: str, dest: Path) -> int:
    """
    Download a direct video/stream URL into Colab local disk.

    Priority:
    1) aria2c multi-connection downloader with resume support
    2) Python requests fallback with resume/retry support

    This is better for large signed URLs because browser-like single requests
    can stall on datacenter IPs.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    # ---- Preferred: aria2c if installed ----
    aria2 = shutil.which("aria2c")
    if aria2:
        cmd = [
            aria2,
            "-c",                         # continue partial download
            "-x", os.getenv("ARIA2_CONNECTIONS", "8"),
            "-s", os.getenv("ARIA2_SPLITS", "8"),
            "-k", os.getenv("ARIA2_CHUNK_SIZE", "1M"),
            "--file-allocation=none",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--summary-interval=10",
            "--console-log-level=notice",
            "--max-tries=0",              # unlimited tries
            "--retry-wait=5",
            "--timeout=60",
            "--connect-timeout=30",
            "--lowest-speed-limit=50K",   # force retry when stuck
            "--user-agent=Mozilla/5.0 Colab-HLS-Worker",
            "-d", str(dest.parent),
            "-o", dest.name,
            url,
        ]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        output_tail = []
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                print("[aria2c]", line, flush=True)
                output_tail.append(line)
                output_tail = output_tail[-30:]

        rc = proc.wait()
        if rc == 0 and dest.exists() and dest.stat().st_size > 0:
            return dest.stat().st_size

        # aria2 sometimes fails on hosts that dislike multi-range requests.
        # Fall back to requests below.
        print("[download] aria2c failed; falling back to Python requests.", flush=True)
        if output_tail:
            print("\n".join(output_tail[-10:]), flush=True)

    # ---- Fallback: Python requests with resume/retry ----
    total = dest.stat().st_size if dest.exists() else 0
    max_retries = int(os.getenv("URL_DOWNLOAD_RETRIES", "20"))
    idle_timeout_sec = int(os.getenv("URL_DOWNLOAD_IDLE_TIMEOUT", "90"))

    headers_base = {
        "User-Agent": "Mozilla/5.0 Colab-HLS-Worker",
        "Accept": "*/*",
        "Connection": "keep-alive",
    }

    for attempt in range(1, max_retries + 1):
        headers = dict(headers_base)
        if dest.exists() and dest.stat().st_size > 0:
            headers["Range"] = f"bytes={dest.stat().st_size}-"

        try:
            with requests.get(url, headers=headers, stream=True, timeout=(30, 60)) as r:
                # 416 usually means local file already complete for range request
                if r.status_code == 416 and dest.exists() and dest.stat().st_size > 0:
                    return dest.stat().st_size

                if r.status_code not in (200, 206):
                    raise HTTPException(
                        status_code=500,
                        detail=f"URL download failed: {r.status_code} {r.text[:500]}"
                    )

                mode = "ab" if r.status_code == 206 and dest.exists() else "wb"
                if mode == "wb":
                    total = 0

                last_progress = time.time()
                with open(dest, mode) as f:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            total += len(chunk)
                            last_progress = time.time()
                        elif time.time() - last_progress > idle_timeout_sec:
                            raise TimeoutError("URL download stalled with no progress")

                if dest.exists() and dest.stat().st_size > 0:
                    return dest.stat().st_size

        except Exception as e:
            print(f"[download] attempt {attempt}/{max_retries} failed: {e}", flush=True)
            if attempt >= max_retries:
                raise
            time.sleep(min(5 * attempt, 60))

    return dest.stat().st_size if dest.exists() else 0

def download_from_onedrive_path(token: str, source_path: str, dest: Path) -> int:
    url = f"{GRAPH_BASE}/me/drive/root:{graph_path(source_path)}:/content"
    return download_url_to_file(url, dest, graph_headers(token))

def ensure_onedrive_folder(token: str, folder_path: str):
    folder_path = normalize_path(folder_path).strip("/")
    if not folder_path:
        return
    current = ""
    for part in folder_path.split("/"):
        current = f"{current}/{part}" if current else part
        check_url = f"{GRAPH_BASE}/me/drive/root:{graph_path('/' + current)}"
        check = requests.get(check_url, headers=graph_headers(token), timeout=60)
        if check.status_code == 200:
            continue
        parent = "/" + "/".join(current.split("/")[:-1])
        if parent == "/":
            create_url = f"{GRAPH_BASE}/me/drive/root/children"
        else:
            create_url = f"{GRAPH_BASE}/me/drive/root:{graph_path(parent)}:/children"
        body = {"name": part, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"}
        cr = requests.post(create_url, headers=graph_headers(token, True), json=body, timeout=60)
        if cr.status_code not in (200, 201):
            raise HTTPException(status_code=500, detail=f"Create folder failed {current}: {cr.status_code} {cr.text[:300]}")

def upload_small_file(token: str, local_file: Path, onedrive_path: str):
    url = f"{GRAPH_BASE}/me/drive/root:{graph_path(onedrive_path)}:/content"
    with open(local_file, "rb") as f:
        r = requests.put(url, headers=graph_headers(token), data=f, timeout=120)
    if r.status_code not in (200, 201):
        raise HTTPException(status_code=500, detail=f"Small upload failed {onedrive_path}: {r.status_code} {r.text[:300]}")

def upload_large_file(token: str, local_file: Path, onedrive_path: str):
    session_url = f"{GRAPH_BASE}/me/drive/root:{graph_path(onedrive_path)}:/createUploadSession"
    body = {"item": {"@microsoft.graph.conflictBehavior": "replace"}}
    s = requests.post(session_url, headers=graph_headers(token, True), json=body, timeout=60)
    if not s.ok:
        raise HTTPException(status_code=500, detail=f"Upload session failed {onedrive_path}: {s.status_code} {s.text[:300]}")
    upload_url = s.json()["uploadUrl"]
    size = local_file.stat().st_size
    chunk_size = 10 * 1024 * 1024
    with open(local_file, "rb") as f:
        start = 0
        while start < size:
            chunk = f.read(chunk_size)
            end = start + len(chunk) - 1
            headers = {"Content-Length": str(len(chunk)), "Content-Range": f"bytes {start}-{end}/{size}"}
            r = requests.put(upload_url, headers=headers, data=chunk, timeout=180)
            if r.status_code not in (200, 201, 202):
                raise HTTPException(status_code=500, detail=f"Chunk upload failed {onedrive_path}: {r.status_code} {r.text[:300]}")
            start = end + 1

def upload_file(token: str, local_file: Path, onedrive_path: str):
    if local_file.stat().st_size < 4 * 1024 * 1024:
        upload_small_file(token, local_file, onedrive_path)
    else:
        upload_large_file(token, local_file, onedrive_path)

def upload_folder_to_onedrive(token: str, local_folder: Path, remote_folder: str) -> List[str]:
    """
    Upload a whole local folder to OneDrive with visible progress.

    This is important for HLS because one movie can have hundreds of .ts files.
    """
    uploaded: List[str] = []
    local_folder = Path(local_folder)
    files = sorted([p for p in local_folder.iterdir() if p.is_file()])

    total = len(files)
    print(f"[upload] Files to upload: {total}", flush=True)

    for idx, file_path in enumerate(files, start=1):
        remote_path = remote_folder.rstrip("/") + "/" + file_path.name
        upload_file(token, file_path, remote_path)
        uploaded.append(remote_path)

        if idx == 1 or idx % 10 == 0 or idx == total or file_path.name.endswith(".m3u8"):
            size_mb = file_path.stat().st_size / 1024 / 1024
            print(f"[upload] Uploaded {idx}/{total}: {file_path.name} ({size_mb:.2f} MB)", flush=True)

    print(f"[upload] DONE uploaded {len(uploaded)}/{total} files to {remote_folder}", flush=True)
    return uploaded

def has_nvidia_gpu() -> bool:
    return shutil.which("nvidia-smi") is not None and subprocess.run(["nvidia-smi"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0

def ffmpeg_hls(input_file: Path, output_folder: Path, video_bitrate: str, audio_bitrate: str, hls_time: int):
    output_folder.mkdir(parents=True, exist_ok=True)
    playlist = output_folder / "master.m3u8"
    segment_pattern = output_folder / "seg_%05d.ts"
    encoder = "h264_nvenc" if has_nvidia_gpu() else "libx264"
    preset = "p4" if encoder == "h264_nvenc" else "veryfast"
    cmd = [
        "ffmpeg", "-y", "-i", str(input_file),
        "-c:v", encoder, "-preset", preset,
        "-b:v", video_bitrate, "-maxrate", video_bitrate, "-bufsize", "5000k",
        "-c:a", "aac", "-b:a", audio_bitrate, "-ac", "2",
        "-f", "hls", "-hls_time", str(hls_time), "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(segment_pattern), str(playlist),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.returncode != 0:
        raise HTTPException(status_code=500, detail={"message": "FFmpeg failed", "stderr_tail": proc.stderr[-3000:]})
    return playlist, encoder

def delete_onedrive_file(token: str, path: str):
    url = f"{GRAPH_BASE}/me/drive/root:{graph_path(path)}"
    r = requests.delete(url, headers=graph_headers(token), timeout=60)
    if r.status_code not in (200, 202, 204, 404):
        raise HTTPException(status_code=500, detail=f"Delete original failed: {r.status_code} {r.text[:300]}")



def run_ffmpeg_hls_popen(
    input_file: Path,
    output_folder: Path,
    video_bitrate: str,
    audio_bitrate: str,
    hls_time: int,
):
    """
    Start FFmpeg and return process, playlist, encoder.
    The playlist is named pre_master.m3u8 while FFmpeg is running.
    """
    output_folder.mkdir(parents=True, exist_ok=True)
    playlist = output_folder / "pre_master.m3u8"
    segment_pattern = output_folder / "seg_%05d.ts"

    encoder = "h264_nvenc" if has_nvidia_gpu() else "libx264"
    preset = "p4" if encoder == "h264_nvenc" else "veryfast"

    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_file),
        "-c:v", encoder,
        "-preset", preset,
        "-b:v", video_bitrate,
        "-maxrate", video_bitrate,
        "-bufsize", "5000k",
        "-c:a", "aac",
        "-b:a", audio_bitrate,
        "-ac", "2",
        "-f", "hls",
        "-hls_time", str(hls_time),
        "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(segment_pattern),
        str(playlist),
    ]

    proc = subprocess.Popen(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc, playlist, encoder, cmd


def write_temp_pre_master_without_endlist(src_playlist: Path, temp_playlist: Path):
    """
    For in-progress playlist, remove ENDLIST so players know it is not final yet.
    """
    if not src_playlist.exists():
        return False

    content = src_playlist.read_text(encoding="utf-8", errors="ignore")
    lines = [line for line in content.splitlines() if line.strip() != "#EXT-X-ENDLIST"]
    temp_playlist.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return True


def is_file_stable(path: Path, previous_sizes: dict, stable_checks: int) -> bool:
    """
    A segment is considered ready when its size is unchanged for N checks.
    """
    size = path.stat().st_size
    history = previous_sizes.get(str(path), [])
    history.append(size)
    history = history[-stable_checks:]
    previous_sizes[str(path)] = history

    if len(history) < stable_checks:
        return False
    return len(set(history)) == 1 and size > 0


def upload_progressive_while_ffmpeg_runs(
    token: str,
    proc: subprocess.Popen,
    output_folder: Path,
    remote_folder: str,
    playlist: Path,
    upload_poll_sec: float,
    pre_master_upload_every_sec: float,
    stable_checks: int,
    job_id: Optional[str] = None,
):
    """
    Poll output folder while FFmpeg runs.
    Upload stable .ts segments once.
    Upload pre_master.m3u8 repeatedly.
    After FFmpeg finishes, upload remaining segments and final master.m3u8.
    """
    ensure_onedrive_folder(token, remote_folder)

    uploaded_segments = set()
    previous_sizes = {}
    last_pre_upload = 0.0
    temp_pre = output_folder / "_upload_pre_master.m3u8"
    if job_id:
        set_job(job_id, status="transcoding", ffmpeg_pid=proc.pid, segments_uploaded=0)

    def upload_pre_master(final: bool = False):
        remote_pre = normalize_path(remote_folder).rstrip("/") + "/pre_master.m3u8"
        remote_master = normalize_path(remote_folder).rstrip("/") + "/master.m3u8"

        if final:
            if playlist.exists():
                upload_file(token, playlist, remote_master)
                upload_file(token, playlist, remote_pre)
            return

        ok = write_temp_pre_master_without_endlist(playlist, temp_pre)
        if ok:
            upload_file(token, temp_pre, remote_pre)

    while proc.poll() is None:
        for seg in sorted(output_folder.glob("seg_*.ts")):
            if str(seg) in uploaded_segments:
                continue

            if is_file_stable(seg, previous_sizes, stable_checks):
                remote_seg = normalize_path(remote_folder).rstrip("/") + "/" + seg.name
                upload_file(token, seg, remote_seg)
                uploaded_segments.add(str(seg))
                if job_id:
                    set_job(job_id, status="transcoding", segments_created=count_segments(output_folder), segments_uploaded=len(uploaded_segments), output_bytes=path_size_bytes(output_folder))

        now = time.time()
        if now - last_pre_upload >= pre_master_upload_every_sec:
            upload_pre_master(final=False)
            last_pre_upload = now
            if job_id:
                set_job(job_id, status="transcoding", segments_created=count_segments(output_folder), segments_uploaded=len(uploaded_segments), output_bytes=path_size_bytes(output_folder))

        time.sleep(upload_poll_sec)

    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "FFmpeg failed",
                "stderr_tail": (stderr or "")[-3000:],
            },
        )

    # Upload every remaining segment after FFmpeg exits.
    for seg in sorted(output_folder.glob("seg_*.ts")):
        if str(seg) in uploaded_segments:
            continue
        remote_seg = normalize_path(remote_folder).rstrip("/") + "/" + seg.name
        upload_file(token, seg, remote_seg)
        uploaded_segments.add(str(seg))

    # Upload final playlists last.
    upload_pre_master(final=True)

    if job_id:
        set_job(job_id, status="finalizing", segments_created=count_segments(output_folder), segments_uploaded=len(uploaded_segments), output_bytes=path_size_bytes(output_folder))

    return {
        "uploaded_segments": len(uploaded_segments),
        "stderr_tail": (stderr or "")[-1000:],
    }



def run_progressive_job_core(job: ProgressiveHlsJob, background: bool = False):
    """
    Shared progressive HLS implementation.
    background=True updates JOBS so UI can poll status.
    """
    started = time.time()
    token = get_access_token()

    job_input_dir = INPUT_DIR / job.job_id
    job_output_dir = OUTPUT_DIR / job.job_id
    shutil.rmtree(job_input_dir, ignore_errors=True)
    shutil.rmtree(job_output_dir, ignore_errors=True)
    job_input_dir.mkdir(parents=True, exist_ok=True)
    job_output_dir.mkdir(parents=True, exist_ok=True)

    ext = ".mp4"
    if job.source_path and "." in job.source_path:
        ext = "." + job.source_path.split(".")[-1].split("?")[0]
    input_file = job_input_dir / f"source{ext}"

    if background:
        set_job(
            job.job_id,
            status="downloading",
            started_at=started,
            source_path=job.source_path,
            has_download_url=bool(job.download_url),
            input_file=str(input_file),
            output_dir=str(job_output_dir),
            output_onedrive_folder=job.output_onedrive_folder,
            pre_master_m3u8=normalize_path(job.output_onedrive_folder).rstrip("/") + "/pre_master.m3u8",
            master_m3u8=normalize_path(job.output_onedrive_folder).rstrip("/") + "/master.m3u8",
            gpu=has_nvidia_gpu(),
            error=None,
        )

    if job.download_url:
        bytes_downloaded = download_from_url(job.download_url, input_file)
    elif job.source_path:
        bytes_downloaded = download_from_onedrive_path(token, job.source_path, input_file)
    else:
        raise HTTPException(status_code=400, detail="source_path or download_url required")

    if background:
        set_job(job.job_id, status="starting_ffmpeg", bytes_downloaded=bytes_downloaded, input_bytes=input_file.stat().st_size)

    proc, playlist, encoder, cmd = run_ffmpeg_hls_popen(
        input_file=input_file,
        output_folder=job_output_dir,
        video_bitrate=job.video_bitrate,
        audio_bitrate=job.audio_bitrate,
        hls_time=job.hls_time,
    )

    if background:
        set_job(job.job_id, status="transcoding", encoder=encoder, ffmpeg_pid=proc.pid, ffmpeg_cmd=" ".join(cmd))

    upload_result = upload_progressive_while_ffmpeg_runs(
        token=token,
        proc=proc,
        output_folder=job_output_dir,
        remote_folder=job.output_onedrive_folder,
        playlist=playlist,
        upload_poll_sec=job.upload_poll_sec,
        pre_master_upload_every_sec=job.pre_master_upload_every_sec,
        stable_checks=job.stable_checks,
        job_id=job.job_id if background else None,
    )

    if job.delete_original_after_success and job.source_path:
        delete_onedrive_file(token, job.source_path)

    result = {
        "ok": True,
        "mode": "progressive",
        "job_id": job.job_id,
        "gpu": has_nvidia_gpu(),
        "encoder": encoder,
        "bytes_downloaded": bytes_downloaded,
        "uploaded_segments": upload_result["uploaded_segments"],
        "pre_master_m3u8": normalize_path(job.output_onedrive_folder).rstrip("/") + "/pre_master.m3u8",
        "master_m3u8": normalize_path(job.output_onedrive_folder).rstrip("/") + "/master.m3u8",
        "elapsed_sec": round(time.time() - started, 2),
    }

    if job.delete_temp_after:
        shutil.rmtree(job_input_dir, ignore_errors=True)
        shutil.rmtree(job_output_dir, ignore_errors=True)

    if background:
        set_job(job.job_id, status="done", finished_at=now_ts(), result=result, ffmpeg_pid=None)

    return result


def background_progressive_runner(job_data: dict):
    job = ProgressiveHlsJob(**job_data)
    try:
        run_progressive_job_core(job, background=True)
    except Exception as exc:
        detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
        set_job(
            job.job_id,
            status="error",
            error=detail,
            traceback=traceback.format_exc()[-4000:],
            finished_at=now_ts(),
            ffmpeg_pid=None,
        )


@app.post("/job/start-progressive")
def start_progressive_background(job: ProgressiveHlsJob):
    if not job.source_path and not job.download_url:
        raise HTTPException(status_code=400, detail="source_path or download_url required")

    existing = get_job(job.job_id)
    if existing.get("status") in {"queued", "downloading", "starting_ffmpeg", "transcoding", "finalizing"}:
        raise HTTPException(status_code=409, detail=f"Job already running: {job.job_id}")

    set_job(
        job.job_id,
        status="queued",
        started_at=now_ts(),
        output_onedrive_folder=job.output_onedrive_folder,
        source_path=job.source_path,
        has_download_url=bool(job.download_url),
        gpu=has_nvidia_gpu(),
        error=None,
    )
    t = threading.Thread(target=background_progressive_runner, args=(job.dict(),), daemon=True)
    t.start()
    set_job(job.job_id, thread_alive=True)
    return {
        "ok": True,
        "background": True,
        "job_id": job.job_id,
        "status_url": f"/job/status/{job.job_id}",
        "message": "Job started in background. Browser can disconnect; poll status_url.",
    }


@app.get("/job/status/{job_id}")
def job_status(job_id: str):
    rec = get_job(job_id)
    if not rec:
        raise HTTPException(status_code=404, detail="job not found in current Colab session")

    input_file = Path(rec.get("input_file", "")) if rec.get("input_file") else INPUT_DIR / job_id / "source.mp4"
    output_dir = Path(rec.get("output_dir", "")) if rec.get("output_dir") else OUTPUT_DIR / job_id
    rec["input_bytes"] = path_size_bytes(input_file)
    rec["input_gb"] = round(rec["input_bytes"] / 1024 / 1024 / 1024, 3)
    rec["output_bytes"] = path_size_bytes(output_dir)
    rec["output_gb"] = round(rec["output_bytes"] / 1024 / 1024 / 1024, 3)
    rec["segments_created"] = count_segments(output_dir)
    rec["pre_master_exists"] = (output_dir / "pre_master.m3u8").exists()
    rec["master_local_exists"] = (output_dir / "master.m3u8").exists()
    rec["running"] = rec.get("status") in {"queued", "downloading", "starting_ffmpeg", "transcoding", "finalizing"}
    return rec


@app.post("/job/cancel/{job_id}")
def cancel_job(job_id: str):
    rec = get_job(job_id)
    if not rec:
        raise HTTPException(status_code=404, detail="job not found")
    pid = rec.get("ffmpeg_pid")
    if pid:
        try:
            subprocess.run(["kill", "-9", str(pid)], check=False)
        except Exception:
            pass
    set_job(job_id, status="cancelled", finished_at=now_ts(), ffmpeg_pid=None)
    return {"ok": True, "job_id": job_id, "status": "cancelled"}


@app.post("/job/transcode-hls-progressive")
def transcode_hls_progressive(job: ProgressiveHlsJob):
    """
    Old synchronous endpoint. Kept for compatibility.
    For large files, use /job/start-progressive so browser disconnects do not cancel the job.
    """
    return run_progressive_job_core(job, background=False)



UI_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Colab OneDrive HLS Transcoder</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: Arial, sans-serif; margin: 0; background: #111827; color: #e5e7eb; }
    header { padding: 18px 22px; background: #030712; border-bottom: 1px solid #374151; }
    h1 { margin: 0; font-size: 22px; }
    main { padding: 18px; max-width: 1200px; margin: auto; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .card { background: #1f2937; border: 1px solid #374151; border-radius: 14px; padding: 16px; }
    label { display: block; margin: 10px 0 5px; font-size: 13px; color: #cbd5e1; }
    input, select, button, textarea {
      width: 100%; box-sizing: border-box; padding: 10px; border-radius: 10px;
      border: 1px solid #4b5563; background: #111827; color: #f9fafb;
    }
    button { cursor: pointer; background: #2563eb; border: 0; font-weight: bold; margin-top: 10px; }
    button.secondary { background: #374151; }
    button.danger { background: #b91c1c; }
    .row { display: flex; gap: 8px; }
    .row > * { flex: 1; }
    .files { max-height: 340px; overflow: auto; border: 1px solid #374151; border-radius: 10px; margin-top: 10px; }
    .item { padding: 9px 10px; border-bottom: 1px solid #374151; cursor: pointer; display: flex; justify-content: space-between; gap: 8px; }
    .item:hover { background: #374151; }
    .folder { color: #93c5fd; }
    .file { color: #d1fae5; }
    .muted { color: #9ca3af; font-size: 13px; }
    pre { background: #030712; padding: 12px; border-radius: 12px; overflow: auto; max-height: 330px; }
    @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
<header>
  <h1>Colab OneDrive HLS Transcoder</h1>
  <div class="muted">OneDrive source / URL source → Colab FFmpeg GPU → HLS upload to OneDrive.</div>
</header>
<main>
  <div class="grid">
    <section class="card">
      <h2>1. OneDrive Browser</h2>
      <div class="row">
        <div>
          <label>Current OneDrive path</label>
          <input id="browsePath" value="/" />
        </div>
        <div style="flex:0.35">
          <label>&nbsp;</label>
          <button onclick="listOneDrive()">Open</button>
        </div>
      </div>
      <div class="row">
        <button class="secondary" onclick="goUp()">Up</button>
        <button class="secondary" onclick="listOneDrive()">Refresh</button>
      </div>
      <div id="fileList" class="files"></div>
      <label>Selected OneDrive video path</label>
      <input id="selectedPath" placeholder="/Raw/movie.mp4" />
      <label>Output HLS folder</label>
      <input id="outputFolder1" placeholder="/HLS/movie-name" />
      <button onclick="makeHlsFromOneDrive()">Make HLS from selected OneDrive file</button>
    </section>

    <section class="card">
      <h2>2. Stream URL → HLS</h2>
      <label>Direct video / stream URL</label>
      <textarea id="downloadUrl" rows="4" placeholder="https://example.com/video.mp4"></textarea>
      <label>Output HLS folder</label>
      <input id="outputFolder2" placeholder="/HLS/url-video-name" />
      <button onclick="makeHlsFromUrl()">Make HLS from URL</button>
      <h3>Target</h3>
      <label>Upload target</label>
      <select id="target">
        <option value="onedrive">OneDrive</option>
        <option value="nginx" disabled>Nginx backend later</option>
      </select>
      <div class="muted">Nginx target can be added later when your PC backend upload API is ready.</div>
    </section>
  </div>

  <section class="card" style="margin-top:16px">
    <h2>3. Transcode Settings</h2>
    <div class="grid">
      <div><label>Job ID</label><input id="jobId" placeholder="movie_job_001" /></div>
      <div><label>Video bitrate</label><input id="videoBitrate" value="2500k" /></div>
      <div><label>Audio bitrate</label><input id="audioBitrate" value="128k" /></div>
      <div><label>HLS segment seconds</label><input id="hlsTime" value="10" /></div>
      <div><label>Upload poll seconds</label><input id="uploadPollSec" value="2" /></div>
      <div><label>pre_master upload interval seconds</label><input id="preMasterSec" value="8" /></div>
    </div>
    <label><input type="checkbox" id="deleteOriginal" style="width:auto"> Delete original OneDrive source after success</label>
  </section>

  <section class="card" style="margin-top:16px">
    <h2>4. Status / Log</h2>
    <div class="row">
      <button class="secondary" onclick="health()">Health</button>
      <button class="secondary" onclick="debugEnv()">Debug Env</button>
      <button class="secondary" onclick="checkCurrentJob()">Check Job</button>
      <button class="danger" onclick="cancelCurrentJob()">Cancel Job</button>
      <button class="danger" onclick="clearLog()">Clear</button>
    </div>
    <pre id="log">Ready.</pre>
  </section>
</main>
<script>
function log(obj) {
  const el = document.getElementById('log');
  const text = typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2);
  el.textContent = text + "\n\n" + el.textContent;
}
function clearLog() { document.getElementById('log').textContent = ''; }
function safeNameFromPath(path) {
  let name = (path || 'job').split('/').filter(Boolean).pop() || 'job';
  return name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_-]+/g, '_');
}
function buildPayload(sourceType) {
  const selected = document.getElementById('selectedPath').value.trim();
  const url = document.getElementById('downloadUrl').value.trim();
  const output1 = document.getElementById('outputFolder1').value.trim();
  const output2 = document.getElementById('outputFolder2').value.trim();
  let base = sourceType === 'onedrive' ? safeNameFromPath(selected) : 'url_job_' + Date.now();
  let jobId = document.getElementById('jobId').value.trim() || base + '_prog';
  const payload = {
    job_id: jobId,
    output_onedrive_folder: sourceType === 'onedrive' ? output1 : output2,
    delete_original_after_success: document.getElementById('deleteOriginal').checked,
    video_bitrate: document.getElementById('videoBitrate').value.trim() || '2500k',
    audio_bitrate: document.getElementById('audioBitrate').value.trim() || '128k',
    hls_time: Number(document.getElementById('hlsTime').value || 10),
    upload_poll_sec: Number(document.getElementById('uploadPollSec').value || 2),
    pre_master_upload_every_sec: Number(document.getElementById('preMasterSec').value || 8)
  };
  if (sourceType === 'onedrive') payload.source_path = selected;
  if (sourceType === 'url') payload.download_url = url;
  return payload;
}
async function api(path, options={}) {
  const res = await fetch(path, options);
  const txt = await res.text();
  let data;
  try { data = JSON.parse(txt); } catch { data = txt; }
  if (!res.ok) throw data;
  return data;
}
async function health() { try { log(await api('/health')); } catch(e) { log(e); } }
async function debugEnv() { try { log(await api('/debug-env')); } catch(e) { log(e); } }
async function listOneDrive() {
  const path = document.getElementById('browsePath').value || '/';
  const box = document.getElementById('fileList');
  box.innerHTML = '<div class="item muted">Loading...</div>';
  try {
    const data = await api('/api/onedrive/list?path=' + encodeURIComponent(path));
    box.innerHTML = '';
    (data.items || []).forEach(item => {
      const div = document.createElement('div');
      div.className = 'item ' + (item.type === 'folder' ? 'folder' : 'file');
      div.innerHTML = `<span>${item.type === 'folder' ? '📁' : '🎬'} ${item.name}</span><span class="muted">${item.size_mb || ''}</span>`;
      div.onclick = () => {
        if (item.type === 'folder') {
          document.getElementById('browsePath').value = item.path;
          listOneDrive();
        } else {
          document.getElementById('selectedPath').value = item.path;
          const base = safeNameFromPath(item.path);
          document.getElementById('outputFolder1').value = '/HLS/' + base;
          document.getElementById('jobId').value = base + '_prog';
        }
      };
      box.appendChild(div);
    });
    log({ listed: path, count: (data.items || []).length });
  } catch(e) {
    box.innerHTML = '<div class="item">Failed to list OneDrive</div>';
    log(e);
  }
}
function goUp() {
  let p = document.getElementById('browsePath').value || '/';
  p = p.replace(/\/+$/, '');
  const parts = p.split('/').filter(Boolean);
  parts.pop();
  document.getElementById('browsePath').value = '/' + parts.join('/');
  if (document.getElementById('browsePath').value === '') document.getElementById('browsePath').value = '/';
  listOneDrive();
}
let currentJobId = null;
let statusTimer = null;

function shortJobView(s) {
  return {
    job_id: s.job_id || currentJobId,
    status: s.status,
    gpu: s.gpu,
    input_gb: s.input_gb,
    output_gb: s.output_gb,
    segments_created: s.segments_created,
    segments_uploaded: s.segments_uploaded,
    pre_master_exists: s.pre_master_exists,
    master_local_exists: s.master_local_exists,
    pre_master_m3u8: s.pre_master_m3u8,
    master_m3u8: s.master_m3u8,
    error: s.error || null
  };
}

async function startBackgroundJob(payload, label) {
  currentJobId = payload.job_id;
  document.getElementById('jobId').value = currentJobId;
  log({ start: label, mode: 'background', payload });
  try {
    const data = await api('/job/start-progressive', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
    log(data);
    startPolling(currentJobId);
  } catch(e) { log(e); }
}

function startPolling(jobId) {
  if (statusTimer) clearInterval(statusTimer);
  statusTimer = setInterval(async () => {
    try {
      const s = await api('/job/status/' + encodeURIComponent(jobId));
      log(shortJobView(s));
      if (['done', 'error', 'cancelled'].includes(s.status)) {
        clearInterval(statusTimer);
        statusTimer = null;
      }
    } catch(e) {
      log(e);
      clearInterval(statusTimer);
      statusTimer = null;
    }
  }, 5000);
}

async function checkCurrentJob() {
  const jobId = document.getElementById('jobId').value.trim() || currentJobId;
  if (!jobId) { log('No job ID.'); return; }
  currentJobId = jobId;
  try { log(shortJobView(await api('/job/status/' + encodeURIComponent(jobId)))); }
  catch(e) { log(e); }
}

async function cancelCurrentJob() {
  const jobId = document.getElementById('jobId').value.trim() || currentJobId;
  if (!jobId) { log('No job ID.'); return; }
  try { log(await api('/job/cancel/' + encodeURIComponent(jobId), { method: 'POST' })); }
  catch(e) { log(e); }
}

async function makeHlsFromOneDrive() {
  const payload = buildPayload('onedrive');
  if (!payload.source_path || !payload.output_onedrive_folder) { log('source_path and output folder are required.'); return; }
  startBackgroundJob(payload, 'OneDrive progressive HLS job');
}
async function makeHlsFromUrl() {
  const payload = buildPayload('url');
  if (!payload.download_url || !payload.output_onedrive_folder) { log('download_url and output folder are required.'); return; }
  startBackgroundJob(payload, 'URL progressive HLS job');
}
health();
listOneDrive();
</script>
</body>
</html>
"""


@app.get("/api/onedrive/list")
def api_onedrive_list(path: str = "/"):
    token = get_access_token()
    path = normalize_path(path)
    if path == "/":
        url = f"{GRAPH_BASE}/me/drive/root/children"
    else:
        encoded = graph_path(path)
        url = f"{GRAPH_BASE}/me/drive/root:{encoded}:/children"

    r = requests.get(url, headers=graph_headers(token), timeout=60)
    if not r.ok:
        raise HTTPException(status_code=500, detail=f"OneDrive list failed: {r.status_code} {r.text[:500]}")

    items = []
    for item in r.json().get("value", []):
        is_folder = "folder" in item
        name = item.get("name", "")
        child_path = normalize_path(path).rstrip("/") + "/" + name if path != "/" else "/" + name
        size = item.get("size", 0) or 0
        items.append({
            "name": name,
            "path": child_path,
            "type": "folder" if is_folder else "file",
            "size": size,
            "size_mb": "" if is_folder else round(size / 1024 / 1024, 2),
        })

    items.sort(key=lambda x: (x["type"] != "folder", x["name"].lower()))
    return {"ok": True, "path": path, "items": items}


@app.post("/api/onedrive/create-folder")
def api_create_onedrive_folder(payload: dict):
    token = get_access_token()
    folder = payload.get("path", "")
    if not folder:
        raise HTTPException(status_code=400, detail="path required")
    ensure_onedrive_folder(token, folder)
    return {"ok": True, "path": normalize_path(folder)}


@app.get("/")
def root():
    from fastapi.responses import HTMLResponse
    return HTMLResponse(UI_HTML)


@app.get("/health")
def health():
    return {"ok": True, "port": APP_PORT, "gpu": has_nvidia_gpu()}

@app.get("/debug-env")
def debug_env():
    return {"MS_CLIENT_ID": bool(MS_CLIENT_ID), "MS_CLIENT_SECRET": bool(MS_CLIENT_SECRET), "MS_TENANT": MS_TENANT, "MS_REFRESH_TOKEN": bool(MS_REFRESH_TOKEN), "WORK_DIR": str(WORK_DIR), "gpu": has_nvidia_gpu()}

@app.post("/job/download-upload-test")
def download_upload_test(job: UploadOnlyJob):
    token = get_access_token()
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    ext = ".bin"
    if job.source_path and "." in job.source_path:
        ext = "." + job.source_path.split(".")[-1]
    input_file = INPUT_DIR / f"{job.job_id}{ext}"
    if job.download_url:
        bytes_downloaded = download_url_to_file(job.download_url, input_file)
    elif job.source_path:
        bytes_downloaded = download_from_onedrive_path(token, job.source_path, input_file)
    else:
        raise HTTPException(status_code=400, detail="source_path or download_url required")
    ensure_onedrive_folder(token, job.output_onedrive_folder)
    remote_path = normalize_path(job.output_onedrive_folder).rstrip("/") + "/" + input_file.name
    upload_file(token, input_file, remote_path)
    if job.delete_temp_after:
        input_file.unlink(missing_ok=True)
    return {"ok": True, "job_id": job.job_id, "bytes_downloaded": bytes_downloaded, "uploaded_to": remote_path}

@app.post("/job/transcode-hls")
def transcode_hls(job: HlsJob):
    started = time.time()
    token = get_access_token()
    job_input_dir = INPUT_DIR / job.job_id
    job_output_dir = OUTPUT_DIR / job.job_id
    shutil.rmtree(job_input_dir, ignore_errors=True)
    shutil.rmtree(job_output_dir, ignore_errors=True)
    job_input_dir.mkdir(parents=True, exist_ok=True)
    job_output_dir.mkdir(parents=True, exist_ok=True)
    ext = ".mp4"
    if job.source_path and "." in job.source_path:
        ext = "." + job.source_path.split(".")[-1].split("?")[0]
    input_file = job_input_dir / f"source{ext}"
    if job.download_url:
        bytes_downloaded = download_url_to_file(job.download_url, input_file)
    elif job.source_path:
        bytes_downloaded = download_from_onedrive_path(token, job.source_path, input_file)
    else:
        raise HTTPException(status_code=400, detail="source_path or download_url required")
    playlist, encoder = ffmpeg_hls(input_file, job_output_dir, job.video_bitrate, job.audio_bitrate, job.hls_time)
    uploaded = upload_folder_to_onedrive(token, job_output_dir, job.output_onedrive_folder)
    if job.delete_original_after_success and job.source_path:
        delete_onedrive_file(token, job.source_path)
    if job.delete_temp_after:
        shutil.rmtree(job_input_dir, ignore_errors=True)
        shutil.rmtree(job_output_dir, ignore_errors=True)
    return {"ok": True, "job_id": job.job_id, "gpu": has_nvidia_gpu(), "encoder": encoder, "bytes_downloaded": bytes_downloaded, "uploaded_count": len(uploaded), "master_m3u8": normalize_path(job.output_onedrive_folder).rstrip("/") + "/master.m3u8", "uploaded_files_sample": uploaded[:10], "elapsed_sec": round(time.time() - started, 2)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)


# ============================================================
# Manual recovery / bugfix endpoints
# ============================================================

class LocalUploadJob(BaseModel):
    job_id: str
    output_onedrive_folder: str


@app.post("/job/upload-existing-hls")
def upload_existing_hls(job: LocalUploadJob):
    """
    Upload an already-created local HLS output folder to OneDrive.

    Useful when:
    - source download completed manually
    - ffmpeg was run manually
    - upload was interrupted and needs to be run again
    """
    local_dir = Path(f"/content/transcode_jobs/output/{job.job_id}")

    if not local_dir.exists():
        raise HTTPException(status_code=404, detail=f"Local HLS folder not found: {local_dir}")

    master = local_dir / "master.m3u8"
    if not master.exists():
        raise HTTPException(status_code=400, detail=f"master.m3u8 not found in: {local_dir}")

    token = get_access_token()
    uploaded = upload_folder_to_onedrive(token, local_dir, job.output_onedrive_folder)

    return {
        "ok": True,
        "job_id": job.job_id,
        "uploaded_files": len(uploaded),
        "output_onedrive_folder": job.output_onedrive_folder,
        "master_m3u8": job.output_onedrive_folder.rstrip("/") + "/master.m3u8",
    }


@app.get("/job/local-files/{job_id}")
def local_files_status(job_id: str):
    """
    Check local input/output state for a job.
    """
    input_dir = Path(f"/content/transcode_jobs/input/{job_id}")
    output_dir = Path(f"/content/transcode_jobs/output/{job_id}")
    source = input_dir / "source.mp4"
    aria2_meta = input_dir / "source.mp4.aria2"
    master = output_dir / "master.m3u8"

    segs = sorted(output_dir.glob("seg_*.ts")) if output_dir.exists() else []
    output_size = sum(p.stat().st_size for p in output_dir.glob("*") if p.is_file()) if output_dir.exists() else 0

    endlist = False
    if master.exists():
        try:
            endlist = "#EXT-X-ENDLIST" in master.read_text(errors="ignore")[-500:]
        except Exception:
            endlist = False

    return {
        "job_id": job_id,
        "input_exists": source.exists(),
        "input_gb": round(source.stat().st_size / 1024 / 1024 / 1024, 3) if source.exists() else 0,
        "aria2_resume_exists": aria2_meta.exists(),
        "output_exists": output_dir.exists(),
        "output_gb": round(output_size / 1024 / 1024 / 1024, 3),
        "segments_created": len(segs),
        "last_segment": segs[-1].name if segs else None,
        "master_exists": master.exists(),
        "master_has_endlist": endlist,
    }
