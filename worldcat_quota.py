"""
worldcat_quota.py

Persistent daily quota tracking for WorldCat API usage.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
import os
import threading


DAILY_REQUEST_LIMIT = 50000
USAGE_DIRECTORY = "OCNrecords"
USAGE_FILEPATH = os.path.join(USAGE_DIRECTORY, "worldcat_api_usage.json")
LOCK_FILEPATH = os.path.join(USAGE_DIRECTORY, "worldcat_api_usage.lock")

_PROCESS_LOCK = threading.Lock()


class WorldCatDailyQuotaError(RuntimeError):
    """Raised when the configured daily WorldCat request quota is exhausted."""


def _local_now(now: datetime | None = None) -> datetime:
    if now is None:
        return datetime.now().astimezone()
    if now.tzinfo is None:
        return now.astimezone()
    return now.astimezone()


def _build_default_state(now: datetime | None = None) -> dict:
    current = _local_now(now)
    return {
        "date": current.date().isoformat(),
        "requests_used": 0,
        "daily_limit": DAILY_REQUEST_LIMIT,
        "last_updated": current.isoformat(),
    }


def _read_state(now: datetime | None = None) -> tuple[dict, bool]:
    state = _build_default_state(now)
    if not os.path.exists(USAGE_FILEPATH):
        return state, True

    try:
        with open(USAGE_FILEPATH, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        if isinstance(loaded, dict):
            state.update(loaded)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return state, True

    current = _local_now(now)
    if state.get("date") != current.date().isoformat():
        return _build_default_state(current), True

    state["daily_limit"] = DAILY_REQUEST_LIMIT
    state["requests_used"] = max(0, int(state.get("requests_used", 0)))
    state["last_updated"] = state.get("last_updated") or current.isoformat()
    return state, False


def _write_state(state: dict) -> None:
    os.makedirs(USAGE_DIRECTORY, exist_ok=True)
    temp_path = f"{USAGE_FILEPATH}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=True, indent=2)
    os.replace(temp_path, USAGE_FILEPATH)


@contextmanager
def _locked_state_file():
    os.makedirs(USAGE_DIRECTORY, exist_ok=True)
    if not os.path.exists(LOCK_FILEPATH):
        with open(LOCK_FILEPATH, "w", encoding="utf-8") as handle:
            handle.write("0")

    with _PROCESS_LOCK:
        handle = open(LOCK_FILEPATH, "r+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)

            yield
        finally:
            try:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()


def _snapshot_from_state(state: dict) -> dict:
    used = max(0, min(int(state.get("requests_used", 0)), DAILY_REQUEST_LIMIT))
    remaining = max(0, DAILY_REQUEST_LIMIT - used)
    return {
        "date": str(state.get("date")),
        "requests_used": used,
        "requests_remaining": remaining,
        "daily_limit": DAILY_REQUEST_LIMIT,
        "usage_ratio": (used / DAILY_REQUEST_LIMIT) if DAILY_REQUEST_LIMIT else 0.0,
        "is_exhausted": remaining == 0,
        "last_updated": state.get("last_updated"),
    }


def get_usage_snapshot(now: datetime | None = None) -> dict:
    """Return the persisted usage state for the current local day."""
    with _locked_state_file():
        state, should_persist = _read_state(now)
        if should_persist:
            _write_state(state)
        return _snapshot_from_state(state)


def reserve_requests(count: int = 1, now: datetime | None = None) -> dict:
    """Atomically reserve request slots for outbound API calls."""
    if count <= 0:
        return get_usage_snapshot(now)

    with _locked_state_file():
        state, _ = _read_state(now)
        used = int(state.get("requests_used", 0))
        new_total = used + int(count)
        if new_total > DAILY_REQUEST_LIMIT:
            snapshot = _snapshot_from_state(state)
            raise WorldCatDailyQuotaError(
                f"Daily WorldCat quota reached ({snapshot['requests_used']}/{snapshot['daily_limit']}). "
                "Please wait until after local midnight before sending more requests."
            )

        current = _local_now(now)
        state["requests_used"] = new_total
        state["daily_limit"] = DAILY_REQUEST_LIMIT
        state["date"] = current.date().isoformat()
        state["last_updated"] = current.isoformat()
        _write_state(state)
        return _snapshot_from_state(state)
