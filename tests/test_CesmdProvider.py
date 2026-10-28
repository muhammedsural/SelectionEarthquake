import io
import zipfile

import pandas as pd
import pytest
import requests

from selection_service.core.error_handle import ProviderError
from selection_service.enums.enums import ProviderName
from selection_service.processing.criteria import SearchCriteria
from selection_service.processing.mappers import CESMDColumnMapper, ColumnMapperFactory
from selection_service.providers.afad.afad_file_manager import AfadFileManager
from selection_service.providers.cesmd_provider import (
    CESMDProvider,
    make_file_key,
    parse_file_key,
    records_to_frame,
)
from selection_service.providers.providers_factory import ProviderFactory


def station(net, code, epidist, *, pgav2=0.4, fault_dist=None, vs30=400, raw=True, processed=True, sttype="Ground"):
    return {
        "code": code, "network": net, "name": f"{code} station", "type": sttype,
        "latitude": 35.5, "longitude": -117.4, "Vs30": vs30, "siteclass": "C",
        "record": {
            "epidist": epidist, "pgav1": (pgav2 or 0) + 0.05, "pgav2": pgav2, "pgv": 40.0, "pgd": 12.0,
            "fault_dist": fault_dist,
            "data_availability": {"plot": "true", "processed": str(processed).lower(), "raw": str(raw).lower()},
        },
    }


# wserv/records/query?rettype=metadata&groupby=event yanit bicimi (2026-10 dogrulandi).
PAYLOAD = {
    "details": {"status": 200}, "count": 4,
    "results": {"events": [
        {
            "id": "ci38457511", "cesmd_id": "ci38457511", "mag": 7.1, "magType": "MW", "faultType": "SS",
            "latitude": 35.7695, "longitude": -117.5993, "depth": 8.0, "time": "2019-07-06 03:19:53",
            "stations": [
                station("CI", "CCC", 34.5, pgav2=0.476, fault_dist=2.8),
                station("CI", "CLC", 5.1, pgav2=0.484),
                station("CI", "NODATA", 10.0, raw=False, processed=False),
            ],
        },
        {
            "id": "us6000jllz", "cesmd_id": "us6000jllz", "mag": 7.8, "magType": "MWW", "faultType": None,
            "latitude": 37.1662, "longitude": 37.0421, "depth": 17.9, "time": "2023-02-06 01:17:36",
            "stations": [station("TK", "0118", 153.9, pgav2=None, vs30=None)],
        },
    ]},
}


class FakeResponse:
    def __init__(self, status_code=200, payload=None, content=b"", text=""):
        self.status_code = status_code
        self._payload = payload
        self.content = content
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response = response or FakeResponse(payload=PAYLOAD)
        self.error = error
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {}), timeout))
        if self.error:
            raise self.error
        return self.response


def zip_bytes(names):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name in names:
            archive.writestr(name, "data")
    return buffer.getvalue()


def provider(session=None, tmp_path=None, **kwargs):
    if tmp_path is not None:
        kwargs["file_manager"] = AfadFileManager(base_dir=str(tmp_path))
    return CESMDProvider(column_mapper=CESMDColumnMapper(), session=session or FakeSession(), **kwargs)


def criteria(**kwargs):
    return SearchCriteria(start_date="2019-07-01", end_date="2023-12-31", **kwargs)


def test_records_are_flattened_and_mapped_to_standard_columns():
    df = CESMDColumnMapper().map_columns(records_to_frame(PAYLOAD))
    # Ne ham ne islenmis verisi olan kayit atilir.
    assert list(df["SSN"]) == ["CI.CCC", "CI.CLC", "TK.0118"]
    ccc, clc, tk = (df.iloc[i] for i in range(3))
    assert ccc["PROVIDER"] == "CESMD" and ccc["EVENT"] == "ci38457511"
    assert ccc["FILE_NAME_H1"] == "CI.CCC~34.5" and ccc["RSN"] == "ci38457511|CI.CCC~34.5"
    assert ccc["MAGNITUDE"] == 7.1 and ccc["MAGNITUDE_TYPE"] == "MW" and ccc["YEAR"] == 2019
    assert ccc["MECHANISM"] == "StrikeSlip" and ccc["VS30(m/s)"] == 400
    # Fay mesafesi varsa o, yoksa Repi.
    assert ccc["RJB(km)"] == 2.8 and ccc["RRUP(km)"] == 2.8
    assert clc["RJB(km)"] == 5.1 and clc["RRUP(km)"] == 5.1
    # g -> cm/s^2, PGV cm/s, PGD cm.
    assert ccc["PGA(cm2/sec)"] == pytest.approx(0.476 * 980.665)
    assert ccc["PGV(cm/sec)"] == 40.0 and ccc["PGD(cm)"] == 12.0
    # islenmis PGA yoksa ham PGA'ya duser; bilinmeyen mekanizma.
    assert pd.isna(tk["VS30(m/s)"]) and tk["MECHANISM"] == "Unknown"
    assert tk["PGA(cm2/sec)"] == pytest.approx(0.05 * 980.665)
    assert tk["HYPO_LAT"] == 37.1662 and tk["HYPO_DEPTH(km)"] == 17.9
    assert records_to_frame({}).empty and records_to_frame({"results": {"events": []}}).empty


def test_file_key_roundtrip_and_validation():
    assert make_file_key("CI", "CCC", 34.54) == "CI.CCC~34.5"
    assert make_file_key("CI", "CCC", None) == "CI.CCC"
    assert parse_file_key("CI.CCC~34.5") == ("CI", "CCC", 34.5)
    assert parse_file_key("CI.CCC") == ("CI", "CCC", None)
    with pytest.raises(ProviderError):
        parse_file_key("CCC")


def test_search_sends_server_filters_and_applies_the_rest_client_side():
    session = FakeSession()
    cesmd = provider(session)
    crit = criteria(
        min_magnitude=7.0, max_magnitude=8.0, max_Repi=200, min_Rrup=1, max_Rrup=100,
        min_pga=300, bbox=(30.0, 40.0, -120.0, 40.0),
    )
    result = cesmd.fetch_data_sync(cesmd.map_criteria(crit))
    assert result.success
    # TK.0118 Repi 153.9 ve PGA 49 cm/s^2 -> min_pga=300 ile elenir.
    assert list(result.value["SSN"]) == ["CI.CCC", "CI.CLC"]
    url, params, timeout = session.calls[0]
    assert url == "https://strongmotioncenter.org/wserv/records/query" and timeout == 90
    assert params["rettype"] == "metadata" and params["groupby"] == "event" and params["format"] == "json"
    assert params["startdate"] == "2019-07-01 00:00:00" and params["enddate"] == "2023-12-31 23:59:59"
    assert params["minmag"] == 7.0 and params["maxepidist"] == 200 and params["sttype"] == "G"
    assert params["eminlat"] == 30.0 and params["emaxlon"] == 40.0
    # Rrup ve PGA CESMD'ye gonderilmez.
    assert not any("rup" in key or "pga" in key for key in params)


def test_circle_search_uses_radius_parameters():
    params = criteria(circleLatitude=37.2, circleLongitude=37.0, circleRadius=150).to_cesmd_params()
    assert (params["elat"], params["elon"], params["erad"]) == (37.2, 37.0, 150)


@pytest.mark.parametrize(
    "response,error,ok_empty",
    [(FakeResponse(204), None, True), (FakeResponse(404, text="No data found!"), None, True)],
)
def test_no_data_responses_give_an_empty_result(response, error, ok_empty):
    cesmd = provider(FakeSession(response))
    result = cesmd.fetch_data_sync(cesmd.map_criteria(criteria()))
    assert result.success and result.value.empty


def test_search_errors_are_wrapped():
    for session in (
        FakeSession(error=requests.ConnectionError("down")),
        FakeSession(FakeResponse(500, text="boom")),
        FakeSession(FakeResponse(200, payload=None)),
    ):
        cesmd = provider(session)
        result = cesmd.fetch_data_sync(cesmd.map_criteria(criteria()))
        assert not result.success


def test_download_requires_a_registered_email(tmp_path, monkeypatch):
    monkeypatch.delenv("CESMD_EMAIL", raising=False)
    session = FakeSession()
    cesmd = provider(session, tmp_path)
    result = cesmd.download_single_waveforms("CI.CCC~34.5", event_id="ci38457511")
    assert not result.success and "register.pl" in str(result.error)
    assert session.calls == []


def test_download_builds_dataset_query_and_extracts_the_zip(tmp_path, monkeypatch):
    monkeypatch.setenv("CESMD_EMAIL", "kullanici@example.org")
    body = zip_bytes(["CI.CCC.V2.zip.txt", "CI.CCC_HNE.V2", "CI.CLC_HNE.V2"])
    session = FakeSession(FakeResponse(200, content=body))
    cesmd = provider(session, tmp_path)
    result = cesmd.download_single_waveforms("CI.CCC~34.5", event_id="ci38457511", file_status="RawAcc")
    assert result.success
    url, params, timeout = session.calls[0]
    assert url.endswith("/wserv/records/query") and timeout == 180
    assert params["rettype"] == "dataset" and params["download"] == "R" and params["netid"] == "CI"
    assert params["eventid"] == "ci38457511" and params["email"] == "kullanici@example.org"
    assert params["minepidist"] == pytest.approx(34.44) and params["maxepidist"] == pytest.approx(34.56)
    kept = sorted(path.name for path in tmp_path.rglob("*") if path.is_file())
    assert kept == ["CI.CCC.V2.zip.txt", "CI.CCC_HNE.V2"]  # baska istasyon dosyasi atilir


def test_download_uses_processed_data_by_default_and_explicit_email(tmp_path, monkeypatch):
    monkeypatch.delenv("CESMD_EMAIL", raising=False)
    session = FakeSession(FakeResponse(200, content=zip_bytes(["a.V2"])))
    cesmd = provider(session, tmp_path, email="kullanici@example.org")
    assert cesmd.download_single_waveforms("CI.CCC~34.5", event_id="x").success
    assert session.calls[0][1]["download"] == "P"


def test_download_failures_are_reported(tmp_path, monkeypatch):
    monkeypatch.setenv("CESMD_EMAIL", "kullanici@example.org")
    cases = [
        FakeSession(FakeResponse(404, text="No data found!")),
        FakeSession(FakeResponse(400, text="Error 400")),
        FakeSession(FakeResponse(200, content=b"<html>not a zip</html>", text="not a zip")),
        FakeSession(error=requests.Timeout("slow")),
    ]
    for session in cases:
        result = provider(session, tmp_path).download_single_waveforms("CI.CCC~34.5", event_id="x")
        assert not result.success
    assert not provider(FakeSession(), tmp_path).download_single_waveforms("CI.CCC").success  # event_id yok


def test_batch_download_counts_successes_and_failures(tmp_path, monkeypatch):
    monkeypatch.setenv("CESMD_EMAIL", "kullanici@example.org")

    class Flaky(FakeSession):
        def get(self, url, params=None, timeout=None):
            self.calls.append((url, dict(params or {}), timeout))
            if params["netid"] == "XX":
                return FakeResponse(404)
            return FakeResponse(200, content=zip_bytes(["a.V2"]))

    cesmd = provider(Flaky(), tmp_path)
    result = cesmd.download_waveforms_batch(["CI.CCC~1.0", "XX.AAA~2.0"], event_ids=["e1", "e2"])
    assert result.success and result.value["downloaded"] == 1 and len(result.value["failures"]) == 1
    assert not cesmd.download_waveforms_batch(["XX.AAA~2.0"], event_ids=["e2"]).success


def test_factory_and_mapper_registration():
    created = ProviderFactory.create_provider(ProviderName.CESMD, use_cache=False)
    assert isinstance(created, CESMDProvider) and created.get_name() == "CESMD"
    assert isinstance(ColumnMapperFactory.get_mapper(ProviderName.CESMD), CESMDColumnMapper)
    assert isinstance(ColumnMapperFactory.create(ProviderName.CESMD), CESMDColumnMapper)
