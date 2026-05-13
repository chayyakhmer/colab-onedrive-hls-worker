# Colab OneDrive HLS Worker UI - Background Job Version

This worker runs in Google Colab and exposes a web UI through Cloudflare Tunnel.

## Main flow

Source video from OneDrive or a direct URL goes to Colab, FFmpeg converts it to HLS, and HLS files are uploaded to OneDrive.

## New in this version

Large jobs now run in the background.

Old behavior:

```text
Browser request stays open -> download/transcode/upload -> response after finished
```

New behavior:

```text
Click Make HLS -> API returns job_id immediately -> job continues in background -> UI polls status
```

This is better for 4 GB, 10 GB, and 20 GB files because the job is not cancelled just because the browser request disconnects.

## Endpoints

```text
GET  /health
GET  /debug-env
GET  /api/onedrive/list?path=/
POST /job/start-progressive
GET  /job/status/{job_id}
POST /job/cancel/{job_id}
POST /job/transcode-hls-progressive   # old synchronous endpoint, kept for compatibility
```

## UI features

- Browse OneDrive files
- Start OneDrive file -> HLS
- Start URL -> HLS
- Background job mode
- Poll progress every 5 seconds
- Show input GB, output GB, segment count, upload count, pre_master, master playlist path
- Check current job manually
- Cancel current FFmpeg job

## Required environment variables

```python
import os
os.environ["MS_CLIENT_ID"] = "YOUR_CLIENT_ID"
os.environ["MS_CLIENT_SECRET"] = "YOUR_CLIENT_SECRET"
os.environ["MS_TENANT"] = "consumers"
os.environ["MS_REFRESH_TOKEN"] = "YOUR_REFRESH_TOKEN"
os.environ["PORT"] = "2323"
```

Do not commit secrets to GitHub.

## Colab quick start

```python
!rm -rf colab-onedrive-hls-worker
!git clone https://github.com/chayyakhmer/colab-onedrive-hls-worker.git
%cd colab-onedrive-hls-worker

!pip install -q -r requirements.txt
!apt-get update -y > /dev/null
!apt-get install -y ffmpeg > /dev/null

import os
os.environ["MS_CLIENT_ID"] = "YOUR_CLIENT_ID"
os.environ["MS_CLIENT_SECRET"] = "YOUR_CLIENT_SECRET"
os.environ["MS_TENANT"] = "consumers"
os.environ["MS_REFRESH_TOKEN"] = "YOUR_REFRESH_TOKEN"
os.environ["PORT"] = "2323"

!pkill -f colab_worker || true
!pkill -f cloudflared || true

!nohup python colab_worker.py > worker.log 2>&1 &

!wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O cloudflared
!chmod +x cloudflared
!nohup ./cloudflared tunnel --no-autoupdate run --token "YOUR_CLOUDFLARE_TUNNEL_TOKEN" > cloudflared.log 2>&1 &

!sleep 8
!curl -s http://127.0.0.1:2323/health
!curl -s https://transcode.labhome.xyz/health || true
```

## Notes

For unfinished jobs, `pre_master.m3u8` is uploaded progressively. `master.m3u8` is uploaded only after FFmpeg completes successfully.


## Update: aria2 URL downloader

This version uses `aria2c` for Stream URL downloads when available.

Benefits:
- Resume partial downloads with `-c`
- Multi-connection download with `-x 8 -s 8`
- Better retry behavior for large signed URLs
- Python requests fallback remains available

In Colab install block, include:

```python
!apt-get install -y ffmpeg aria2 > /dev/null
```

Optional environment tuning:

```python
os.environ["ARIA2_CONNECTIONS"] = "8"
os.environ["ARIA2_SPLITS"] = "8"
os.environ["ARIA2_CHUNK_SIZE"] = "1M"
```


## Stable bugfix version

This build keeps the fixes proven during testing:

- `download_from_url()` exists and uses `aria2c` when available.
- Stream URL downloads support resume through aria2 `.aria2` metadata.
- Background job endpoints are included:
  - `POST /job/start-progressive`
  - `GET /job/status/{job_id}`
- Upload progress is printed for HLS folders, useful for hundreds of `.ts` files.
- Recovery endpoint added:
  - `POST /job/upload-existing-hls`
- Local recovery status endpoint added:
  - `GET /job/local-files/{job_id}`

Colab install should include:

```python
!apt-get install -y ffmpeg aria2 > /dev/null
```

Recommended after cloning in Colab:

```python
!grep -n "aria2c" colab_worker.py
!grep -n "start-progressive" colab_worker.py
!grep -n "job/status" colab_worker.py
!grep -n "upload-existing-hls" colab_worker.py
```
