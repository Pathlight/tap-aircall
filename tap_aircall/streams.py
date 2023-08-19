"""Stream type classes for tap-aircall."""

from typing import Optional, List, Dict

from tap_aircall.client import aircallStream
from .schemas import user_properties, call_properties

from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime


def get_stream_partitions_range(config: dict) -> List[Dict[str, float]]:
    if "start_date" not in config:
        return []

    now = datetime.now()
    try:
        start_date_as_dt = parse_datetime(config["start_date"])
    except Exception:
        start_date_as_dt = now - timedelta(days=30)  # Default to the last 30 days

    interval_hours = config.get("interval_hours") or 24
    delta = timedelta(hours=interval_hours)
    current_datetime = start_date_as_dt
    datetimes = []

    while current_datetime <= now:
        datetimes.append({
            'from': current_datetime.timestamp(),
            'to': (current_datetime + delta).timestamp()
        })
        current_datetime += delta
    return datetimes


class UsersStream(aircallStream):
    """Define custom stream."""
    name = "users"
    path = "v1/users"
    primary_keys = ["id"]
    replication_key = "created_at"
    schema = user_properties.to_dict()
    records_jsonpath = "$.users[*]"  # Or override `parse_response`.

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "user_id": record["id"]
        }

class CallsStream(aircallStream):
    """Define custom stream."""
    name = "calls"
    path = "v1/calls"
    primary_keys = ["id"]
    replication_key = "started_at"
    schema = call_properties.to_dict()
    records_jsonpath = "$.calls[*]"  # Or override `parse_response`.

    @property
    def partitions(self):
        # Since there's a limit of 10k results in a single request,
        # split up the requests so that we get the full amount
        return get_stream_partitions_range(self.config)

class UserStream(aircallStream):
    """Define custom stream."""
    name = "user"
    parent_stream_type = UsersStream
    path = "v1/users/{user_id}"

    primary_keys = ["id"]
    replication_key = "created_at"
    schema = user_properties.to_dict()
    records_jsonpath = "$.users[*]"  # Or override `parse_response`.
    #  not to store any state bookmarks for the child stream
    state_partitioning_keys = []
