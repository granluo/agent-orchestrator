import config
from scheduler import decide_route


# --- content signal ---

def test_short_prompt_low_load_routes_local():
    payload = {"prompt": "hi"}
    metrics = {"by_status": {"PENDING": 0}}
    assert decide_route(payload, metrics) == "local"


def test_long_prompt_routes_cloud():
    payload = {"prompt": "x" * (config.THRESHOLD + 1)}
    metrics = {"by_status": {"PENDING": 0}}
    assert decide_route(payload, metrics) == "cloud"


# --- load signal ---

def test_high_pending_backlog_routes_cloud():
    payload = {"prompt": "hi"}  # short prompt: content signal not triggered
    metrics = {"by_status": {"PENDING": config.PENDING_THRESHOLD + 1}}
    assert decide_route(payload, metrics) == "cloud"


# --- boundaries: the code uses strict '>', so exactly-at-threshold stays local ---

def test_prompt_exactly_at_threshold_routes_local():
    payload = {"prompt": "x" * config.THRESHOLD}
    metrics = {"by_status": {"PENDING": 0}}
    assert decide_route(payload, metrics) == "local"


def test_pending_exactly_at_threshold_routes_local():
    payload = {"prompt": "hi"}
    metrics = {"by_status": {"PENDING": config.PENDING_THRESHOLD}}
    assert decide_route(payload, metrics) == "local"


# --- defensive paths: missing keys must not crash, and default to local ---

def test_payload_without_prompt_key_routes_local():
    payload = {}  # no "prompt" key: .get("prompt", "") fallback
    metrics = {"by_status": {"PENDING": 0}}
    assert decide_route(payload, metrics) == "local"


def test_metrics_without_pending_key_routes_local():
    payload = {"prompt": "hi"}
    metrics = {"by_status": {}}  # no "PENDING" key: .get("PENDING", 0) fallback
    assert decide_route(payload, metrics) == "local"


def test_empty_metrics_routes_local():
    payload = {"prompt": "hi"}
    metrics = {}  # no "by_status" at all: outer .get("by_status", {}) fallback
    assert decide_route(payload, metrics) == "local"
