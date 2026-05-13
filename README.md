# Colab OneDrive HLS Worker

Flow:

```text
OneDrive raw video
→ Colab full download
→ FFmpeg GPU/CPU transcode to HLS
→ upload HLS folder back to OneDrive
```

Worker port: `2323`

## Required env

```env
MS_CLIENT_ID=your_full_client_id
MS_CLIENT_SECRET=your_client_secret
MS_TENANT=consumers
MS_REFRESH_TOKEN=your_refresh_token
PORT=2323
```

## Colab start

```python
!git clone https://github.com/YOUR_USERNAME/colab-onedrive-hls-worker.git
%cd colab-onedrive-hls-worker
!pip install -r requirements.txt
!apt-get update -y && apt-get install -y ffmpeg

import os
os.environ["MS_CLIENT_ID"] = "..."
os.environ["MS_CLIENT_SECRET"] = "..."
os.environ["MS_TENANT"] = "consumers"
os.environ["MS_REFRESH_TOKEN"] = "..."
os.environ["PORT"] = "2323"

!python colab_worker.py
```

## Test download + upload only

```bash
curl -X POST http://127.0.0.1:2323/job/download-upload-test \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "test1",
    "source_path": "/Raw/movieA.mp4",
    "output_onedrive_folder": "/UploadTest"
  }'
```

## Transcode to HLS

```bash
curl -X POST http://127.0.0.1:2323/job/transcode-hls \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "movieA",
    "source_path": "/Raw/movieA.mp4",
    "output_onedrive_folder": "/HLS/movieA",
    "delete_original_after_success": false,
    "video_bitrate": "2500k",
    "audio_bitrate": "128k",
    "hls_time": 6
  }'
```

Output in OneDrive:

```text
/HLS/movieA/master.m3u8
/HLS/movieA/seg_00000.ts
/HLS/movieA/seg_00001.ts
```

## Cloudflare Tunnel later

Quick random tunnel:

```bash
cloudflared tunnel --url http://127.0.0.1:2323
```

For fixed domain `https://trancode.labhome.xyz`, create a named Cloudflare Tunnel in Cloudflare dashboard and route it to:

```text
http://127.0.0.1:2323
```
