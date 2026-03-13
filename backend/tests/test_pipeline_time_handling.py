from pipeline.base import _utcnow_db_naive


def test_utcnow_db_naive_is_naive_datetime():
    dt = _utcnow_db_naive()
    assert dt.tzinfo is None
