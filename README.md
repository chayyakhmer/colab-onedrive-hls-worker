# Colab OneDrive HLS Worker UI

## Features

```text
1. Browse OneDrive files/folders and click Make HLS
2. Paste stream/video URL and click Make HLS
3. Select output OneDrive folder path
4. Progressive HLS:
   pre_master.m3u8 while running
   master.m3u8 after finished
5. Nginx target placeholder for later PC backend upload
```

## Push/update to GitHub

Use `push_to_github.bat` from this bundle, or manually copy these files into your repo:

```text
colab_worker.py
requirements.txt
README.md
colab_quickstart.py
```

Repo:

```text
https://github.com/chayyakhmer/colab-onedrive-hls-worker.git
```

## Colab Cell 1 — setup + run all in background

Replace secrets before running.

```python
!rm -rf colab-onedrive-hls-worker
!git clone https://github.com/chayyakhmer/colab-onedrive-hls-worker.git
%cd colab-onedrive-hls-worker

!pip install -q -r requirements.txt
!apt-get update -y > /dev/null
!apt-get install -y ffmpeg > /dev/null

import os

os.environ["MS_CLIENT_ID"] = "YOUR_FULL_CLIENT_ID"
os.environ["MS_CLIENT_SECRET"] = "YOUR_CLIENT_SECRET"
os.environ["MS_TENANT"] = "consumers"
os.environ["MS_REFRESH_TOKEN"] = "YOUR_REFRESH_TOKEN"
os.environ["PORT"] = "2323"

!nvidia-smi || true

!pkill -f colab_worker || true
!pkill -f cloudflared || true

!nohup python colab_worker.py > worker.log 2>&1 &

!wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O cloudflared
!chmod +x cloudflared

!nohup ./cloudflared tunnel --no-autoupdate run --token "YOUR_CLOUDFLARE_TUNNEL_TOKEN" > cloudflared.log 2>&1 &

!sleep 8

print("===== LOCAL HEALTH =====")
!curl -s http://127.0.0.1:2323/health || true

print("\n===== DEBUG ENV =====")
!curl -s http://127.0.0.1:2323/debug-env || true

print("\n===== WORKER LOG =====")
!tail -n 30 worker.log

print("\n===== CLOUDFLARED LOG =====")
!tail -n 50 cloudflared.log
```

## Colab Cell 2 — test/logs

```python
print("===== LOCAL HEALTH =====")
!curl -s http://127.0.0.1:2323/health

print("\n===== PUBLIC HEALTH =====")
!curl -s https://transcode.labhome.xyz/health || true

print("\n===== LOGS =====")
!tail -n 80 worker.log
!tail -n 50 cloudflared.log
```

## Open frontend

```text
https://transcode.labhome.xyz
```

## Windows DNS workaround

```bat
curl -v --resolve transcode.labhome.xyz:443:104.21.17.142 https://transcode.labhome.xyz/health
```

## API endpoints

```text
GET  /
GET  /api/onedrive/list?path=/
POST /api/onedrive/create-folder
POST /job/transcode-hls-progressive
GET  /health
GET  /debug-env
```
