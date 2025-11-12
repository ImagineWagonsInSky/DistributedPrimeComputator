import os
import json

DATA_DIR = "services/file_service/server_store"
SUBDIVISIONS_DIR = "services/file_service/server_store/subdivisions"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(SUBDIVISIONS_DIR, exist_ok=True)

META_PATH = os.path.join(DATA_DIR, "meta.json")


def file_timestamp(path):
    return int(os.path.getmtime(path))


def load_meta():
    if not os.path.exists(META_PATH):
        meta = {"term": 0, "voted_for": None, "last_leader": None, "file_sizes": {}}
        save_meta(meta)
        return meta
    with open(META_PATH, "r") as f:
        return json.load(f)


def save_meta(meta):
    tmp = META_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(meta, f)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, META_PATH)
