import os
from pathlib import Path

from main import build_app


DB_PATH = Path(os.environ.get("AA_DB_PATH", "data/meetings.sqlite"))
app = build_app(DB_PATH)
