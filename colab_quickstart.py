# Paste these commands into Colab cells, then add your own secrets.
# Do not commit secrets to GitHub.

# Install faster/resumable URL downloader
!apt-get install -y aria2 > /dev/null

# aria2 tuning for large signed URL downloads
import os
os.environ["ARIA2_CONNECTIONS"] = "8"
os.environ["ARIA2_SPLITS"] = "8"
os.environ["ARIA2_CHUNK_SIZE"] = "1M"
