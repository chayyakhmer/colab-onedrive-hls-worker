import os, time, shutil, subprocess
from pathlib import Path
from typing import Optional, List
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
    ensure_onedrive_folder(token, remote_folder)
    uploaded = []
    for file in sorted(local_folder.rglob("*")):
        if not file.is_file():
            continue
        rel = file.relative_to(local_folder).as_posix()
        remote_path = normalize_path(remote_folder).rstrip("/") + "/" + rel
        parent = "/" + "/".join(remote_path.strip("/").split("/")[:-1])
        ensure_onedrive_folder(token, parent)
        upload_file(token, file, remote_path)
        uploaded.append(remote_path)
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

        now = time.time()
        if now - last_pre_upload >= pre_master_upload_every_sec:
            upload_pre_master(final=False)
            last_pre_upload = now

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

    return {
        "uploaded_segments": len(uploaded_segments),
        "stderr_tail": (stderr or "")[-1000:],
    }


@app.post("/job/transcode-hls-progressive")
def transcode_hls_progressive(job: ProgressiveHlsJob):
    """
    Progressive HLS mode:
    OneDrive/source URL -> full download -> FFmpeg HLS.
    While FFmpeg runs:
      - uploads finished seg_XXXXX.ts files
      - repeatedly uploads pre_master.m3u8
    After FFmpeg completes:
      - uploads final master.m3u8
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

    if job.download_url:
        bytes_downloaded = download_from_url(job.download_url, input_file)
    elif job.source_path:
        bytes_downloaded = download_from_onedrive_path(token, job.source_path, input_file)
    else:
        raise HTTPException(status_code=400, detail="source_path or download_url required")

    proc, playlist, encoder, cmd = run_ffmpeg_hls_popen(
        input_file=input_file,
        output_folder=job_output_dir,
        video_bitrate=job.video_bitrate,
        audio_bitrate=job.audio_bitrate,
        hls_time=job.hls_time,
    )

    upload_result = upload_progressive_while_ffmpeg_runs(
        token=token,
        proc=proc,
        output_folder=job_output_dir,
        remote_folder=job.output_onedrive_folder,
        playlist=playlist,
        upload_poll_sec=job.upload_poll_sec,
        pre_master_upload_every_sec=job.pre_master_upload_every_sec,
        stable_checks=job.stable_checks,
    )

    if job.delete_original_after_success and job.source_path:
        delete_onedrive_file(token, job.source_path)

    if job.delete_temp_after:
        shutil.rmtree(job_input_dir, ignore_errors=True)
        shutil.rmtree(job_output_dir, ignore_errors=True)

    return {
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


@app.get("/")
def root():
    return {"ok": True, "service": "Colab OneDrive HLS Worker", "port": APP_PORT, "endpoints": ["/health", "/debug-env", "/job/download-upload-test", "/job/transcode-hls"]}

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
