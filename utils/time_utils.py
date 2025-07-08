from datetime import datetime, timedelta

def get_yesterday_start():
    return datetime.strptime((datetime.today() - timedelta(1)).strftime('%d/%m/%Y') + " 00:00:00", "%d/%m/%Y %H:%M:%S")

def to_timestamp(dt: datetime) -> float:
    return dt.timestamp()

def from_timestamp(ts: float) -> datetime:
    return datetime.fromtimestamp(ts)
