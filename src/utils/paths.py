import os
from pathlib import Path

def project_source_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent