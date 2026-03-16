import importlib

import pandas as pd


hud_section202 = importlib.import_module("competitors.hud_section202")


def _reset_cache():
    hud_section202._S202_CACHE_DF = None
    hud_section202._S202_CACHE_SIG = None


def test_loader_prefers_servicing_site_name_text_over_name(tmp_path, monkeypatch):
    csv_path = tmp_path / "hud_section_202_properties.csv"
    pd.DataFrame(
        [
            {
                "lat": 41.88,
                "lon": -87.63,
                "SERVICING_SITE_NAME_TEXT": "St. Anne Senior Residences",
                "name": "Chicago Hub",
            }
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.setattr(hud_section202, "DATA_FILE", csv_path)
    _reset_cache()

    rows = hud_section202.get_nearby_section202_projects(41.88, -87.63, 5)

    assert len(rows) == 1
    assert rows[0]["name"] == "St. Anne Senior Residences"


def test_loader_falls_back_when_servicing_name_missing(tmp_path, monkeypatch):
    csv_path = tmp_path / "hud_section_202_properties.csv"
    pd.DataFrame(
        [
            {
                "lat": 41.88,
                "lon": -87.63,
                "SERVICING_SITE_NAME_TEXT": "",
                "property_name": "Mercy Manor",
                "name": "Chicago Hub",
            }
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.setattr(hud_section202, "DATA_FILE", csv_path)
    _reset_cache()

    rows = hud_section202.get_nearby_section202_projects(41.88, -87.63, 5)

    assert len(rows) == 1
    assert rows[0]["name"] == "Mercy Manor"
