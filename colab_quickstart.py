# =========================
# COLAB QUICK START
# =========================

!git clone https://github.com/YOUR_USERNAME/colab-onedrive-hls-worker.git
%cd colab-onedrive-hls-worker

!pip install -r requirements.txt
!apt-get update -y && apt-get install -y ffmpeg

import os
os.environ["MS_CLIENT_ID"] = "YOUR_FULL_CLIENT_ID"
os.environ["MS_CLIENT_SECRET"] = "YOUR_CLIENT_SECRET"
os.environ["MS_TENANT"] = "consumers"
os.environ["MS_REFRESH_TOKEN"] = "YOUR_REFRESH_TOKEN"
os.environ["PORT"] = "2323"

!nvidia-smi || true
!python colab_worker.py
