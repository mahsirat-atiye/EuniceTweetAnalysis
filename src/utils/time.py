from datetime import datetime
import iso8601

def parse_time(ts: str) -> datetime:
    return iso8601.parse_date(ts)
