import numpy as np
import pandas as pd
import pytest
import requests

from selection_service.core.error_handle import ProviderError
from selection_service.processing.criteria import SearchCriteria
from selection_service.processing.mappers import ColumnMapperFactory, NGAW3ColumnMapper
from selection_service.providers.afad.afad_file_manager import AfadFileManager
from selection_service.providers.interfaces import supports_download
from selection_service.providers.ngaw3_provider import (
    NGAW3Provider,
    at2_text,
    make_file_key,
    parse_file_key,
    payload_to_frame,
)
from selection_service.providers.providers_factory import ProviderFactory
from selection_service.enums.enums import ProviderName

# /flatfile yaniti sutun bazli sozluktur (dokuman Quick Start: ``.json()['motion_id']``).
FLATFILE = {
    "motion_id": [11, 12, 13],
    "event_id": [1, 1, 2],
    "comcat_id": ["ci38457511", "ci38457511", None],
    "datetime": ["2019-07-06 03:19:53", "2019-07-06 03:19:53", "2023-02-06 01:17:36"],
    "magnitude": [7.1, 7.1, 7.8],
    "magnitude_type": ["Mw", "Mw", "Mw"],
    "hypocenter_latitude": [35.77, 35.77, 37.17],
    "hypocenter_longitude": [-117.6, -117.6, 37.04],
    "hypocenter_depth": [8.0, 8.0, 17.9],
    "rake": [-176.0, 5.0, 3.0],
    "mechanism_based_on_Rake": [0, 0, 2],
    "network_code": ["CI", "CI", "TK"],
    "station_code": ["CCC", "CLC", None],
    "station_name": ["Little Lake", "Ridgecrest", "Pazarcik"],
    "station_latitude": [35.5, 35.8, 37.4],
    "station_longitude": [-117.4, -117.6, 37.3],
    "vs30": [400.0, 350.0, None],
    "rjb": [2.8, 5.1, 12.0],
    "rrup": [3.0, 5.5, 13.0],
    "epicentral_distance": [34.5, 5.1, 20.0],
    "pga_rotd50": [0.4, 0.5, 0.3],
    "pgv_rotd50": [40.0, 50.0, 30.0],
    "hpass_fc_h1": [0.1, 0.2, 0.05],
    "hpass_fc_h2": [0.15, 0.1, None],
}


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    def __init__(self, flatfile=None, motions=None, login=None, error=None):
        self.flatfile = flatfile if flatfile is not None else FakeResponse(payload=FLATFILE)
        self.motions = motions
        self.login = login or FakeResponse(payload={"token": "tok-1"})
        self.error = error
        self.calls = []

    def get(self, url, params=None, headers=None, auth=None, timeout=None):
        self.calls.append(("GET", url, dict(params or {}), dict(headers or {}), auth))
        if self.error:
            raise self.error
        return self.login if url.endswith("/users/login") else self.flatfile

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(("POST", url, json, dict(headers or {}), None))
        if self.error:
            raise self.error
        return self.motions


def provider(session=None, tmp_path=None, **kwargs):
    kwargs.setdefault("username", "user")
    kwargs.setdefault("password", "pw")
    if tmp_path is not None:
        kwargs["file_manager"] = AfadFileManager(base_dir=str(tmp_path))
    return NGAW3Provider(column_mapper=NGAW3ColumnMapper(), session=session or FakeSession(), **kwargs)


def criteria(**kwargs):
    return SearchCriteria(start_date="2019-07-01", end_date="2023-12-31", **kwargs)


def test_flatfile_is_mapped_to_standard_columns():
    df = NGAW3ColumnMapper().map_columns(pd.DataFrame(FLATFILE))
    a, b, c = (df.iloc[i] for i in range(3))
    assert a["PROVIDER"] == "NGAW3" and a["RSN"] == "11" and a["FILE_NAME_H1"] == "NGAW3_MID11"
    assert a["EVENT"] == "ci38457511" and c["EVENT"] == "NGAW3-2"
    assert a["SSN"] == "CI.CCC" and c["SSN"] == "TK.Pazarcik"  # istasyon kodu yoksa ad
    assert a["YEAR"] == 2019 and a["MAGNITUDE"] == 7.1 and a["MAGNITUDE_TYPE"] == "Mw"
    assert a["MECHANISM"] == "StrikeSlip" and c["MECHANISM"] == "Reverse"
    assert a["RJB(km)"] == 2.8 and a["RRUP(km)"] == 3.0 and a["VS30(m/s)"] == 400.0
    assert pd.isna(c["VS30(m/s)"])
    assert a["PGA(cm2/sec)"] == pytest.approx(0.4 * 980.665) and a["PGV(cm/sec)"] == 40.0
    assert pd.isna(a["PGD(cm)"])
    assert a["LOWFREQ(Hz)"] == 0.15 and c["LOWFREQ(Hz)"] == 0.05
    assert a["HYPO_DEPTH(km)"] == 8.0 and a["HYPO_LAT"] == 35.77


def test_file_key_roundtrip_and_validation():
    assert make_file_key(123) == "NGAW3_MID123" and make_file_key(123.0) == "NGAW3_MID123"
    assert parse_file_key("NGAW3_MID123") == 123
    for bad in ("MID123", "NGAW3_MIDx", "CI.CCC~1.0"):
        with pytest.raises(ProviderError):
            parse_file_key(bad)


def test_payload_forms_and_server_errors():
    assert payload_to_frame({"a": [1, 2]}).shape == (2, 1)
    assert payload_to_frame([{"a": 1}, {"a": 2}]).shape == (2, 1)
    assert payload_to_frame([]).empty and payload_to_frame(None).empty
    # Sunucu hatasi HTTP 200 ile metin doner.
    with pytest.raises(ProviderError, match="unknown field"):
        payload_to_frame("unknown field 'x' in where")
    with pytest.raises(ProviderError, match="bad"):
        payload_to_frame({"success": False, "errorMessage": "bad"})


def test_criteria_build_server_where_and_client_dates():
    c = criteria(min_magnitude=6, max_magnitude=8, max_Rjb=50, min_vs30=200, bbox=(30, 40, 20, 45))
    where = c.to_ngaw3_params()["where"]
    assert where.startswith("public_time_series=1")
    for part in ("magnitude>=6", "magnitude<=8", "rjb<=50", "vs30>=200", "hypocenter_latitude>=30",
                 "hypocenter_longitude<=45"):
        assert part in where
    assert c.ngaw3_date_range() == ("2019-07-01", "2023-12-31")
    assert "magnitude" not in criteria().to_ngaw3_params()["where"]


def test_search_logs_in_with_basic_auth_and_filters_client_side():
    session = FakeSession()
    p = provider(session)
    result = p.fetch_data_sync(p.map_criteria(criteria(min_magnitude=7.5, max_Repi=50)))
    assert result.success
    assert list(result.value["RSN"]) == ["13"]  # M7.5 alt siniri ve Repi 20 km
    login, flat = session.calls
    assert login[1].endswith("/users/login") and login[4] == ("user", "pw")
    assert flat[1].endswith("/flatfile") and flat[3]["Authorization"] == "Bearer tok-1"
    assert flat[2]["limit"] == 1000 and flat[2]["page"] == 1 and "motion_id" in flat[2]["select"]
    assert "magnitude>=7.5" in flat[2]["where"]


def test_date_range_and_dedup_are_applied():
    dup = {key: values + values[:1] for key, values in FLATFILE.items()}
    session = FakeSession(flatfile=FakeResponse(payload=dup))
    p = provider(session)
    in_2019 = SearchCriteria(start_date="2019-01-01", end_date="2019-12-31")
    result = p.fetch_data_sync(p.map_criteria(in_2019))
    assert list(result.value["RSN"]) == ["11", "12"]


def test_paging_stops_on_short_page(monkeypatch):
    import selection_service.providers.ngaw3_provider as module

    monkeypatch.setattr(module, "PAGE_SIZE", 2)
    page1 = {k: v[:2] for k, v in FLATFILE.items()}
    page2 = {k: v[2:] for k, v in FLATFILE.items()}
    pages = iter([FakeResponse(payload=page1), FakeResponse(payload=page2)])
    session = FakeSession()
    original_get = session.get

    def get(url, params=None, headers=None, auth=None, timeout=None):
        if url.endswith("/users/login"):
            return original_get(url, params, headers, auth, timeout)
        session.calls.append(("GET", url, dict(params), {}, None))
        return next(pages)

    session.get = get
    p = provider(session)
    result = p.fetch_data_sync(p.map_criteria(criteria()))
    assert len(result.value) == 3
    assert [c[2]["page"] for c in session.calls if c[1].endswith("/flatfile")] == [1, 2]


def test_missing_credentials_and_bad_login(monkeypatch):
    monkeypatch.delenv("NGAGM_USERNAME", raising=False)
    monkeypatch.delenv("NGAGM_PASSWORD", raising=False)
    p = NGAW3Provider(column_mapper=NGAW3ColumnMapper(), session=FakeSession())
    result = p.fetch_data_sync(p.map_criteria(criteria()))
    assert not result.success and "NGAGM_USERNAME" in str(result.error)
    bad = provider(FakeSession(login=FakeResponse(status_code=401)))
    result = bad.fetch_data_sync(bad.map_criteria(criteria()))
    assert not result.success and "reddedildi" in str(result.error)


def test_credentials_from_environment(monkeypatch):
    monkeypatch.setenv("NGAGM_USERNAME", "envuser")
    monkeypatch.setenv("NGAGM_PASSWORD", "envpw")
    session = FakeSession()
    p = NGAW3Provider(column_mapper=NGAW3ColumnMapper(), session=session)
    assert p.fetch_data_sync(p.map_criteria(criteria())).success
    assert session.calls[0][4] == ("envuser", "envpw")


def test_connection_error_and_server_error_text():
    p = provider(FakeSession(error=requests.ConnectionError("down")))
    assert not p.fetch_data_sync(p.map_criteria(criteria())).success
    p = provider(FakeSession(flatfile=FakeResponse(payload="Invalid where")))
    result = p.fetch_data_sync(p.map_criteria(criteria()))
    assert not result.success and "Invalid where" in str(result.error)


def test_at2_text_matches_nga_west3_layout():
    text = at2_text("title", np.array([1e-4, -2e-4, 3e-4, 4e-4, 5e-4, 6e-4]), 0.005)
    lines = text.splitlines()
    assert lines[2] == "ACCELERATION TIME SERIES IN UNITS OF G"
    assert lines[3] == "NPTS=      6, DT=   0.0050 SEC"
    assert len(lines[4].split()) == 5 and len(lines[5].split()) == 1
    assert float(lines[4].split()[1]) == pytest.approx(-2e-4)


def test_download_writes_at2_files(tmp_path):
    payload = {"success": True, "output": [{
        "motion_id": 11, "sampling_rate": 200.0, "azimuth_h1": 0.0, "azimuth_h2": 90.0,
        "acc_h1": [0.1, 0.2, 0.3], "acc_h2": [0.0, -0.1, 0.1], "acc_v": [0.01, 0.02, 0.03],
    }]}
    session = FakeSession(motions=FakeResponse(payload=payload))
    p = provider(session, tmp_path)
    result = p.download_single_waveforms("NGAW3_MID11", event_id="ci38457511")
    assert result.success
    folder = tmp_path / "event_ci38457511"
    assert sorted(f.name for f in folder.iterdir()) == [
        "NGAwest3_MID11_000.AT2", "NGAwest3_MID11_090.AT2", "NGAwest3_MID11_UP.AT2",
    ]
    assert "DT=   0.0050" in (folder / "NGAwest3_MID11_000.AT2").read_text()
    post = [c for c in session.calls if c[0] == "POST"][0]
    assert post[2] == {"checked_motion_ids": [11], "format": "json", "component": "acc_h1,acc_h2,acc_v"}
    assert post[3]["Authorization"] == "Bearer tok-1"


def test_download_errors(tmp_path):
    for response, text in (
        (FakeResponse(status_code=403, payload={"success": False, "errorMessage": "no public time series"}), "public"),
        (FakeResponse(payload={"success": True, "output": []}), "zaman serisi yok"),
    ):
        p = provider(FakeSession(motions=response), tmp_path)
        result = p.download_single_waveforms("NGAW3_MID11", event_id="e1")
        assert not result.success and text in str(result.error)
    assert not provider(FakeSession(motions=FakeResponse()), tmp_path).download_single_waveforms("bad").success


def test_batch_reports_failures(tmp_path):
    payload = {"success": True, "output": [{"sampling_rate": 100.0, "azimuth_h1": 10.0, "acc_h1": [0.1]}]}
    p = provider(FakeSession(motions=FakeResponse(payload=payload)), tmp_path)
    result = p.download_waveforms_batch(["NGAW3_MID1", "bad"], event_ids=["e1", "e2"])
    assert result.success and result.value["downloaded"] == 1 and len(result.value["failures"]) == 1


def test_factory_and_mapper_registration():
    assert isinstance(ColumnMapperFactory.get_mapper(ProviderName.NGAW3), NGAW3ColumnMapper)
    created = ProviderFactory.create_provider(ProviderName.NGAW3, use_cache=False, username="u", password="p")
    assert isinstance(created, NGAW3Provider) and created.get_name() == "NGAW3"
    assert supports_download(created)
