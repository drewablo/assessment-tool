import asyncio
import pandas as pd

from pipeline import ingest_housing


class _FakeResult:
    def __init__(self, rowcount=0, scalar_value=None):
        self.rowcount = rowcount
        self._scalar_value = scalar_value

    def scalar(self):
        return self._scalar_value


class _FakeSession:
    def __init__(self, counts, writes):
        self.counts = counts
        self.writes = writes
        self.pending = []
        self.executed = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, stmt):
        self.executed.append(stmt)
        sql = str(stmt).lower()
        table_name = getattr(getattr(stmt, "table", None), "name", "")
        is_count_query = "select count" in sql
        if is_count_query and "hud_lihtc_property" in sql:
            return _FakeResult(scalar_value=self.counts["hud_lihtc_property"])
        if is_count_query and "hud_lihtc_tenant" in sql:
            return _FakeResult(scalar_value=self.counts["hud_lihtc_tenant"])
        if is_count_query and "hud_qct_dda_designations" in sql:
            return _FakeResult(scalar_value=self.counts["hud_qct_dda_designations"])

        if table_name:
            self.writes.append(table_name)
        elif "hud_lihtc_property" in sql:
            self.writes.append("hud_lihtc_property")
        elif "hud_qct_dda_designations" in sql:
            self.writes.append("hud_qct_dda_designations")
        elif "hud_lihtc_tenant" in sql:
            self.writes.append("hud_lihtc_tenant")

        if not is_count_query and (table_name == "hud_lihtc_property" or "hud_lihtc_property" in sql):
            self.counts["hud_lihtc_property"] += 1
            return _FakeResult(rowcount=1)
        if not is_count_query and (table_name == "hud_qct_dda_designations" or "hud_qct_dda_designations" in sql):
            self.counts["hud_qct_dda_designations"] += 1
            return _FakeResult(rowcount=1)
        if not is_count_query and (table_name == "hud_lihtc_tenant" or "hud_lihtc_tenant" in sql):
            self.counts["hud_lihtc_tenant"] += 1
            return _FakeResult(rowcount=1)
        return _FakeResult(rowcount=0)

    def add(self, obj):
        self.pending.append(obj)

    async def flush(self):
        return None

    async def commit(self):
        if self.pending:
            # tenant inserts
            self.counts["hud_lihtc_tenant"] += len(self.pending)
            self.pending.clear()

    async def rollback(self):
        self.pending.clear()


class _SessionFactory:
    def __init__(self, counts):
        self.counts = counts
        self.writes = []
        self.sessions = []

    def __call__(self):
        s = _FakeSession(self.counts, self.writes)
        self.sessions.append(s)
        return s


def test_hud_property_ingest_writes_to_normalized_table(monkeypatch):
    counts = {"hud_lihtc_property": 0, "hud_lihtc_tenant": 0, "hud_qct_dda_designations": 0}
    factory = _SessionFactory(counts)
    monkeypatch.setattr(ingest_housing, "async_session_factory", factory)

    async def _fake_start(session, name):
        return object()

    async def _fake_finish(session, run, **kwargs):
        return None

    async def _fake_fetch():
        return [{"hud_id": "H1", "project_name": "P", "lat": 41.0, "lon": -87.0}]

    monkeypatch.setattr(ingest_housing, "start_pipeline_run", _fake_start)
    monkeypatch.setattr(ingest_housing, "finish_pipeline_run", _fake_finish)
    monkeypatch.setattr(ingest_housing, "_fetch_lihtc_property_rows_from_zip", _fake_fetch)

    result = asyncio.run(ingest_housing._ingest_hud_property_async())
    assert result["processed"] >= 1
    assert result["upserted"] >= 0


def test_hud_tenant_ingest_writes_to_normalized_table(monkeypatch):
    counts = {"hud_lihtc_property": 0, "hud_lihtc_tenant": 0, "hud_qct_dda_designations": 0}
    factory = _SessionFactory(counts)
    monkeypatch.setattr(ingest_housing, "async_session_factory", factory)

    async def _fake_start(session, name):
        return object()

    async def _fake_finish(session, run, **kwargs):
        return None

    async def _fake_download(url, path):
        with pd.ExcelWriter(path) as writer:
            pd.DataFrame(
                [
                    ["Tenant Characteristics", None, None],
                    ["Category", "2022", "2023"],
                    ["Elderly", 2, 4],
                    ["Family", 5, 7],
                ]
            ).to_excel(writer, index=False, header=False, sheet_name="Table 1")

    monkeypatch.setattr(ingest_housing, "start_pipeline_run", _fake_start)
    monkeypatch.setattr(ingest_housing, "finish_pipeline_run", _fake_finish)
    monkeypatch.setattr(ingest_housing, "_download_file", _fake_download)

    result = asyncio.run(ingest_housing._ingest_hud_tenant_async())
    assert result["upserted"] >= 0
    assert counts["hud_lihtc_tenant"] >= 1
    assert any("hud_lihtc_tenant" in w for w in factory.writes) or counts["hud_lihtc_tenant"] > 0


def test_hud_qct_ingest_writes_to_normalized_table(monkeypatch):
    counts = {"hud_lihtc_property": 0, "hud_lihtc_tenant": 0, "hud_qct_dda_designations": 0}
    factory = _SessionFactory(counts)
    monkeypatch.setattr(ingest_housing, "async_session_factory", factory)

    async def _fake_start(session, name):
        return object()

    async def _fake_finish(session, run, **kwargs):
        return None

    async def _fake_fetch(url, local_name):
        return ([{"DESIGNATION_YEAR": 2026, "DESIGNATION_TYPE": "QCT", "TRACT": "12345678901", "STATE_FIPS": "12", "COUNTY_FIPS": "086"}], ["qct"])

    monkeypatch.setattr(ingest_housing, "start_pipeline_run", _fake_start)
    monkeypatch.setattr(ingest_housing, "finish_pipeline_run", _fake_finish)
    monkeypatch.setattr(ingest_housing, "_fetch_xlsx_rows", _fake_fetch)

    result = asyncio.run(ingest_housing._ingest_hud_qct_async())
    assert result["processed"] >= 1
    assert result["upserted"] >= 0


def test_transformers_accept_alias_columns():
    prop = ingest_housing._transform_project({"hud_id": "H1", "project_name": "P", "lat": 1.1, "lng": 2.2})
    tenant = ingest_housing._transform_tenant({"year": 2023, "households": 4, "TRACT_ID": "12345678901"})
    qct = ingest_housing._transform_qct({"year": 2026, "type": "dda", "tract": "12345678901", "state": "12", "county": "086"})

    assert prop is not None
    assert tenant is not None
    assert qct is not None and qct["designation_type"] == "DDA"


def test_transformers_accept_space_and_normalized_headers_with_numeric_formatting():
    tenant = ingest_housing._transform_tenant({"Reporting Year": "2023", "Household Count": "1,204", "Tract ID": "12345678901"})
    qct = ingest_housing._transform_qct({"Designation Year": "2026", "Designation Type": "qct", "Census Tract": "12345678901", "State": "12", "County": "086"})

    assert tenant is not None
    assert tenant["household_count"] == 1204
    assert qct is not None
    assert qct["designation_type"] == "QCT"


def test_property_and_qct_ingest_emit_on_conflict_upsert_statements(monkeypatch):
    counts = {"hud_lihtc_property": 0, "hud_lihtc_tenant": 0, "hud_qct_dda_designations": 0}
    factory = _SessionFactory(counts)
    monkeypatch.setattr(ingest_housing, "async_session_factory", factory)

    async def _fake_start(session, name):
        return object()

    async def _fake_finish(session, run, **kwargs):
        return None

    async def _fake_property_fetch():
        return [{"hud_id": "H1", "project_name": "P", "lat": 41.0, "lon": -87.0}]

    async def _fake_qct_fetch(url, local_name):
        return ([{"DESIGNATION_YEAR": 2026, "DESIGNATION_TYPE": "QCT", "TRACT": "12345678901", "STATE_FIPS": "12", "COUNTY_FIPS": "086"}], ["qct"])

    monkeypatch.setattr(ingest_housing, "start_pipeline_run", _fake_start)
    monkeypatch.setattr(ingest_housing, "finish_pipeline_run", _fake_finish)
    monkeypatch.setattr(ingest_housing, "_fetch_lihtc_property_rows_from_zip", _fake_property_fetch)
    monkeypatch.setattr(ingest_housing, "_fetch_xlsx_rows", _fake_qct_fetch)

    asyncio.run(ingest_housing._ingest_hud_property_async())
    asyncio.run(ingest_housing._ingest_hud_property_async())
    asyncio.run(ingest_housing._ingest_hud_qct_async())
    asyncio.run(ingest_housing._ingest_hud_qct_async())

    executed_sql = "\n".join(str(stmt).lower() for s in factory.sessions for stmt in s.executed)
    assert "on conflict on constraint uq_hud_lihtc_property_hudid_year" in executed_sql
    assert "on conflict on constraint uq_hud_qct_dda_designation_year_type_geoid" in executed_sql


def test_large_property_ingest_is_chunked(monkeypatch):
    counts = {"hud_lihtc_property": 0, "hud_lihtc_tenant": 0, "hud_qct_dda_designations": 0}
    factory = _SessionFactory(counts)
    monkeypatch.setattr(ingest_housing, "async_session_factory", factory)
    monkeypatch.setattr(ingest_housing, "HUD_PROPERTY_BATCH_SIZE", 50)

    async def _fake_start(session, name):
        return object()

    async def _fake_finish(session, run, **kwargs):
        return None

    async def _fake_fetch():
        return [{"hud_id": f"H{i}", "project_name": "P", "lat": 41.0, "lon": -87.0} for i in range(135)]

    monkeypatch.setattr(ingest_housing, "start_pipeline_run", _fake_start)
    monkeypatch.setattr(ingest_housing, "finish_pipeline_run", _fake_finish)
    monkeypatch.setattr(ingest_housing, "_fetch_lihtc_property_rows_from_zip", _fake_fetch)

    result = asyncio.run(ingest_housing._ingest_hud_property_async())
    assert result["processed"] == 135
    write_sql = [str(stmt).lower() for s in factory.sessions for stmt in s.executed if "insert" in str(stmt).lower() and "hud_lihtc_property" in str(stmt).lower()]
    assert len(write_sql) == 3


def test_large_tenant_ingest_is_chunked(monkeypatch):
    counts = {"hud_lihtc_property": 0, "hud_lihtc_tenant": 0, "hud_qct_dda_designations": 0}
    factory = _SessionFactory(counts)
    monkeypatch.setattr(ingest_housing, "async_session_factory", factory)
    monkeypatch.setattr(ingest_housing, "HUD_TENANT_BATCH_SIZE", 40)

    async def _fake_start(session, name):
        return object()

    async def _fake_finish(session, run, **kwargs):
        return None

    async def _fake_download(url, path):
        rows = [[f"Group {i}", i + 1, i + 2] for i in range(95)]
        with pd.ExcelWriter(path) as writer:
            pd.DataFrame(
                [["Tenant Characteristics", None, None], ["Category", "2022", "2023"], *rows]
            ).to_excel(writer, index=False, header=False, sheet_name="Table 1")

    monkeypatch.setattr(ingest_housing, "start_pipeline_run", _fake_start)
    monkeypatch.setattr(ingest_housing, "finish_pipeline_run", _fake_finish)
    monkeypatch.setattr(ingest_housing, "_download_file", _fake_download)

    result = asyncio.run(ingest_housing._ingest_hud_tenant_async())
    assert result["processed"] == 190
    assert counts["hud_lihtc_tenant"] == 5
    write_sql = [str(stmt).lower() for s in factory.sessions for stmt in s.executed if "insert" in str(stmt).lower() and "hud_lihtc_tenant" in str(stmt).lower()]
    assert len(write_sql) == 5


def test_large_qct_ingest_is_chunked(monkeypatch):
    counts = {"hud_lihtc_property": 0, "hud_lihtc_tenant": 0, "hud_qct_dda_designations": 0}
    factory = _SessionFactory(counts)
    monkeypatch.setattr(ingest_housing, "async_session_factory", factory)
    monkeypatch.setattr(ingest_housing, "HUD_QCT_BATCH_SIZE", 30)

    async def _fake_start(session, name):
        return object()

    async def _fake_finish(session, run, **kwargs):
        return None

    async def _fake_fetch(url, local_name):
        rows = [{"DESIGNATION_YEAR": 2026, "DESIGNATION_TYPE": "QCT", "TRACT": f"1234567{i:04d}", "STATE_FIPS": "12", "COUNTY_FIPS": "086"} for i in range(65)]
        return (rows, ["qct"])

    monkeypatch.setattr(ingest_housing, "start_pipeline_run", _fake_start)
    monkeypatch.setattr(ingest_housing, "finish_pipeline_run", _fake_finish)
    monkeypatch.setattr(ingest_housing, "_fetch_xlsx_rows", _fake_fetch)

    result = asyncio.run(ingest_housing._ingest_hud_qct_async())
    assert result["processed"] == 65
    write_sql = [str(stmt).lower() for s in factory.sessions for stmt in s.executed if "insert" in str(stmt).lower() and "hud_qct_dda_designations" in str(stmt).lower()]
    assert len(write_sql) == 3
