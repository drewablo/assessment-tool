import pytest

from pipeline import cli


@pytest.mark.asyncio
async def test_ensure_schema_exits_with_helpful_message_when_db_unreachable(monkeypatch, capsys):
    import db.connection

    async def fake_init_db():
        raise ConnectionRefusedError("Connect call failed ('127.0.0.1', 5432)")

    monkeypatch.setattr(db.connection, "init_db", fake_init_db)

    with pytest.raises(SystemExit) as excinfo:
        await cli._ensure_schema()

    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert "Could not connect to the database" in captured.err
    assert "pg_isready -h localhost -p 5432" in captured.err
    assert "DATABASE_URL env var" in captured.err


@pytest.mark.asyncio
async def test_cmd_init_db_uses_schema_guard(monkeypatch, capsys):
    calls = {"count": 0}

    async def fake_ensure_schema():
        calls["count"] += 1

    monkeypatch.setattr(cli, "_ensure_schema", fake_ensure_schema)

    await cli.cmd_init_db()

    assert calls["count"] == 1
    captured = capsys.readouterr()
    assert "Initializing database" in captured.out
    assert "Database initialized successfully." in captured.out
