from datetime import datetime, timedelta, timezone

from src.quality.duplicate_checks import duplicate_count
from src.quality.freshness_checks import age_in_seconds
from src.quality.null_checks import null_ratio
from src.quality.schema_checks import required_columns_present


def test_required_columns_present() -> None:
    assert required_columns_present(["symbol", "price", "event_time"], ["symbol", "price"])


def test_null_ratio() -> None:
    assert null_ratio([1, None, 2, None]) == 0.5


def test_duplicate_count() -> None:
    assert duplicate_count(["BTCUSDT", "ETHUSDT", "BTCUSDT"]) == 1


def test_age_in_seconds() -> None:
    reference_time = datetime.now(timezone.utc)
    event_time = reference_time - timedelta(seconds=30)

    assert age_in_seconds(event_time, reference_time) == 30
