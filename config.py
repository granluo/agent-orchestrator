import tomllib
from decimal import Decimal

with open("config.toml", "rb") as f:
    _cfg = tomllib.load(f)

MAX_RETRY = _cfg["retry"]["max_retry"]
MAX_DELIVERY = _cfg["retry"]["max_delivery"]
THRESHOLD = _cfg["routing"]["prompt_threshold"]
PENDING_THRESHOLD = _cfg["routing"]["pending_threshold"]
LOCAL_MODEL = _cfg["models"]["local"]
CLOUD_MODEL = _cfg["models"]["cloud"]
LOCAL_COST_PER_TOKEN = Decimal(_cfg["cost"]["local_per_token"])
CLOUD_COST_PER_TOKEN = Decimal(_cfg["cost"]["cloud_per_token"])
OLLAMA_URL = _cfg["ollama"]["url"]
OLLAMA_TIMEOUT = _cfg["ollama"]["timeout_seconds"]
