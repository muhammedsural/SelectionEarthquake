from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from selection_service.enums.enums import ProviderName
from selection_service.processing.criteria import SearchCriteria
from selection_service.processing.mappers import FDSNColumnMapper
from selection_service.providers.afad.afad_file_manager import AfadFileManager
from selection_service.providers.fdsn_provider import FDSNProvider, parse_text_table
from selection_service.waveform import read_mseed

DATA = Path(__file__).parent / "data" / "mseed"

# fdsnws format=text yanitlari (USGS olay, IRIS/KOERI istasyon bicimi).
EVENTS = (
    "#EventID|Time|Latitude|Longitude|Depth/km|Author|Catalog|Contributor|ContributorID|MagType|Magnitude"
    "|MagAuthor|EventLocationName\n"
    "us6000jllz|2023-02-06T01:17:34.861|37.2256|37.0143|10.0|us|us|us|us6000jllz|mww|7.8|us|Pazarcik\n"
    "us6000jlqa|2023-02-06T10:24:48.799|38.0106|37.1962|7.4|us|us|us|us6000jlqa|mww|7.5|us|Elbistan\n"
)
STATIONS = (
    "#Network|Station|Location|Channel|Latitude|Longitude|Elevation|Depth|Azimuth|Dip|SensorDescription"
    "|Scale|ScaleFreq|ScaleUnits|SampleRate|StartTime|EndTime\n"
    "TK|4614||HNE|37.3|37.1|500|0|90|0|Acc|2.0E5|1.0|M/S**2|100|2010-01-01T00:00:00|\n"
    "TK|4614||HNN|37.3|37.1|500|0|0|0|Acc|2.0E5|1.0|M/S**2|100|2010-01-01T00:00:00|\n"
    "TK|4614||HHZ|37.3|37.1|500|0|0|-90|Vel|6.0E8|1.0|M/S|100|2010-01-01T00:00:00|\n"
    "KO|FAR|00|HGE|38.5|37.0|900|0|90|0|Acc|4.0E5|1.0|M/S**2|100|2010-01-01T00:00:00|\n"
    "KO|FAR|10|HNE|38.5|37.0|900|0|90|0|Acc|3.0E5|1.0|M/S**2|100|2010-01-01T00:00:00|\n"
)


class FakeResponse:
    def __init__(self, status_code=200, text="", content=b""):
        self.status_code = status_code
        self.text = text
        self.content = content or text.encode()


class FakeSession:
    """Adres yoluna gore yanit doner; cagrilari kaydeder."""

    def __init__(self, routes=None, error=None):
        self.routes = {"event": FakeResponse(text=EVENTS), "station": FakeResponse(text=STATIONS),
                       "dataselect": FakeResponse(content=(DATA / "steim2.mseed").read_bytes())}
        self.routes.update(routes or {})
        self.error = error
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        if self.error:
            raise self.error
        service = url.split("/fdsnws/")[1].split("/")[0]
        return self.routes[service]


def criteria(**kwargs):
    defaults = {"start_date": "2023-02-06", "end_date": "2023-02-07", "min_magnitude": 7.0,
                "bbox": (35.0, 39.0, 35.0, 40.0)}
    defaults.update(kwargs)
    return SearchCriteria(**defaults)


def provider(session=None, **kwargs):
    return FDSNProvider(FDSNColumnMapper(), session=session or FakeSession(), **kwargs)


def test_parse_text_table_reads_header_and_pads_short_rows():
    rows = parse_text_table("#A|B|C\n1|2\n\n#comment\n4|5|6\n")
    assert rows == [{"a": "1", "b": "2", "c": ""}, {"a": "4", "b": "5", "c": "6"}]
    assert parse_text_table("Error 400: bad request") == []
    # Servislerin cogu ayraclarin cevresine bosluk koyar.
    assert parse_text_table("#EventID | Time \nus1 | 2023-01-01T00:00:00\n") == [
        {"eventid": "us1", "time": "2023-01-01T00:00:00"}]


def test_event_search_maps_criteria_and_returns_event_rows():
    session = FakeSession()
    fdsn = provider(session, service="EMSC")
    mapped = fdsn.map_criteria(criteria(max_depth=20.0))
    assert mapped == {
        "starttime": "2023-02-06T00:00:00.000Z", "endtime": "2023-02-07T23:59:59.999Z",
        "minmagnitude": 7.0, "maxdepth": 20.0, "minlatitude": 35.0, "maxlatitude": 39.0,
        "minlongitude": 35.0, "maxlongitude": 40.0,
    }
    result = fdsn.fetch_data_sync(mapped)
    assert result.success
    df = result.value
    assert list(df["EVENT"]) == ["us6000jllz", "us6000jlqa"]
    row = df.iloc[0]
    assert row["PROVIDER"] == "FDSN_EMSC" and row["MAGNITUDE"] == 7.8 and row["MAGNITUDE_TYPE"] == "mww"
    assert row["YEAR"] == 2023 and row["HYPO_DEPTH(km)"] == 10.0
    url, params = session.calls[0]
    assert url == "https://www.seismicportal.eu/fdsnws/event/1/query"
    assert params["format"] == "text" and params["minmagnitude"] == 7.0


def test_custom_base_urls_and_no_content():
    session = FakeSession({"event": FakeResponse(status_code=204)})
    fdsn = provider(session, service="X", base_url="https://example.org/", data_base_url="https://data.example")
    result = fdsn.fetch_data_sync({"starttime": "2023-01-01"})
    assert result.success and result.value.empty
    assert session.calls[0][0] == "https://example.org/fdsnws/event/1/query"
    assert fdsn.data_url == "https://data.example"


def test_errors_are_wrapped():
    fdsn = provider(FakeSession({"event": FakeResponse(status_code=400, text="bad")}))
    result = fdsn.fetch_data_sync({})
    assert not result.success and "FDSN HTTP 400" in str(result.error)
    import requests
    offline = provider(FakeSession(error=requests.ConnectionError("offline")))
    assert "baglanti" in str(offline.fetch_data_sync({}).error)


def test_record_search_finds_strong_motion_stations_per_event():
    session = FakeSession()
    fdsn = provider(session, service="USGS", data_service="KOERI", record_search=True)
    mapped = fdsn.map_criteria(criteria(max_Rjb=200, min_vs30=300, network="TK"))
    assert mapped["radius_km"] == 200 and "min_vs30" not in mapped["filters"]
    assert mapped["stations"] == {"network": "TK", "station": None}

    df = fdsn.fetch_data_sync(mapped).value
    # Iki deprem x iki istasyon; KO.FAR icin HN bandi HG'den once gelir (konum 10).
    assert sorted(df["FILE_NAME_H1"]) == ["KO.FAR.10.HN", "KO.FAR.10.HN", "TK.4614..HN", "TK.4614..HN"]
    first = df[df["EVENT"] == "us6000jllz"].set_index("STATION")
    assert first.loc["4614", "RSN"] == "us6000jllz|TK.4614..HN"
    assert first.loc["4614", "SSN"] == "TK.4614" and first.loc["4614", "PROVIDER"] == "FDSN_USGS"
    assert first.loc["4614", "RJB(km)"] == pytest.approx(11.22, abs=0.01)
    assert first.loc["4614", "RRUP(km)"] == pytest.approx(np.hypot(first.loc["4614", "RJB(km)"], 10.0), abs=0.01)
    assert first.loc["4614", "MECHANISM"] == "Unknown" and pd.isna(first.loc["4614", "VS30(m/s)"])
    station_url, params = session.calls[1]
    assert station_url == "https://eida.koeri.boun.edu.tr/fdsnws/station/1/query"
    assert params["network"] == "TK" and params["channel"] == "HN?,HG?,EN?,BN?"
    assert params["maxradius"] == pytest.approx(200 / 111.195, abs=1e-3)
    assert params["starttime"] == "2023-02-06T01:17:34.861" and params["level"] == "channel"


def test_record_search_applies_distance_filters_and_event_limit():
    session = FakeSession()
    fdsn = provider(session, record_search=True, max_events=1)
    df = fdsn.fetch_data_sync(fdsn.map_criteria(criteria(max_Repi=50))).value
    assert list(df["FILE_NAME_H1"]) == ["TK.4614..HN"]
    assert len([call for call in session.calls if "station" in call[0]]) == 1


def test_record_search_skips_failing_station_queries():
    fdsn = provider(FakeSession({"station": FakeResponse(status_code=503, text="busy")}), record_search=True)
    result = fdsn.fetch_data_sync(fdsn.map_criteria(criteria()))
    assert result.success and result.value.empty


def test_station_search_flattens_channels():
    session = FakeSession()
    df = provider(session).fetch_stations_sync(network="TK", level="station").value
    assert len(df) == 5
    row = df.iloc[0]
    assert (row["NETWORK"], row["STATION"], row["LOCATION"], row["CHANNEL"]) == ("TK", "4614", "", "HNE")
    assert row["SENSITIVITY"] == 2.0e5 and row["SENSITIVITY_UNITS"] == "M/S**2" and row["PROVIDER"] == "FDSN_IRIS"
    assert session.calls[0][1]["level"] == "channel"


@pytest.mark.asyncio
async def test_async_variants():
    fdsn = provider()
    assert (await fdsn.fetch_data_async({})).success
    assert len((await fdsn.fetch_stations_async()).value) == 5
    traces = (await fdsn.fetch_waveforms_async(**waveform_params())).value
    assert [trace.channel for trace in traces] == ["HNE", "HNN"]


def waveform_params():
    return {"network": "TK", "station": "4614", "location": "00", "channel": "HN?",
            "starttime": "2023-02-06T01:16:00", "endtime": "2023-02-06T01:20:00"}


def test_waveform_query_decodes_miniseed():
    session = FakeSession()
    result = provider(session).fetch_waveforms_sync(**waveform_params())
    assert result.success and len(result.value[0].data) == 3000
    assert session.calls[0][0] == "https://service.earthscope.org/fdsnws/dataselect/1/query"


def test_waveform_query_reports_missing_parameter_and_empty_data():
    params = waveform_params()
    del params["channel"]
    result = provider().fetch_waveforms_sync(**params)
    assert not result.success and "Missing FDSN waveform parameters: channel" in str(result.error)
    empty = provider(FakeSession({"dataselect": FakeResponse(status_code=204)}))
    assert "veri yok" in str(empty.fetch_waveforms_sync(**waveform_params()).error)


def test_download_writes_calibrated_ascii(tmp_path):
    session = FakeSession()
    fdsn = provider(session, record_search=True, file_manager=AfadFileManager(base_dir=str(tmp_path)))
    fdsn.fetch_data_sync(fdsn.map_criteria(criteria(max_Repi=50)))
    result = fdsn.download_single_waveforms("TK.4614..HN", event_id="us6000jllz", export_type="asc2",
                                            file_status="RawAcc")
    assert result.success
    url, params = session.calls[-2]
    assert url.endswith("/dataselect/1/query")
    assert params["location"] == "--" and params["channel"] == "HN?"
    assert params["starttime"] == "2023-02-06T01:17:04.861"
    # 11 km: pencere en az 180 s.
    assert params["endtime"] == "2023-02-06T01:20:34.861"
    files = sorted((tmp_path / "event_us6000jllz").glob("*.ASC"))
    # Fikstur konum kodu 00 tasir; istasyon listesinde yalniz bos konum var -> sayim olarak kalir.
    assert [path.name for path in files] == ["fdsn_TK.4614..HN_HNE.ASC", "fdsn_TK.4614..HN_HNN.ASC"]
    text = files[0].read_text()
    assert "EVENT_ID: us6000jllz" in text and "NDATA: 3000" in text and "SAMPLING_INTERVAL_S: 0.010000" in text


def test_calibrate_converts_counts_to_cm_s2():
    traces = read_mseed((DATA / "steim2.mseed").read_bytes())
    rows = [{"network": "TK", "station": "4614", "location": "00", "channel": "HNE",
             "scale": "2.0E5", "scaleunits": "M/S**2"},
            {"network": "TK", "station": "4614", "location": "00", "channel": "HNN",
             "scale": "4.0", "scaleunits": "M/S"}]
    east, north = FDSNProvider.calibrate(traces, rows)
    assert east.units == "cm/s^2" and np.allclose(east.data, traces[0].data / 2.0e5 * 100)
    assert north.units == "M/S" and np.allclose(north.data, traces[1].data / 4.0)
    assert east.meta == {"SENSITIVITY": 2.0e5}


def test_download_mseed_and_unknown_event(tmp_path):
    session = FakeSession()
    fdsn = provider(session, file_manager=AfadFileManager(base_dir=str(tmp_path)))
    # Olay onbellekte yok: eventid ile olay servisinden alinir; mesafe bilinmedigi icin 300 s.
    assert fdsn.download_single_waveforms("TK.4614.00.HNE", event_id="us6000jllz", export_type="mseed").success
    assert session.calls[0][1]["eventid"] == "us6000jllz"
    assert session.calls[1][1]["endtime"] == "2023-02-06T01:22:34.861"
    assert (tmp_path / "event_us6000jllz" / "fdsn_TK.4614.00.HN.mseed").read_bytes()[:6].isdigit()
    missing = provider(FakeSession({"event": FakeResponse(status_code=204)}))
    assert "bulunamadi" in str(missing.download_single_waveforms("TK.4614..HN", event_id="nope").error)


def test_download_rejects_bad_requests(tmp_path):
    fdsn = provider(file_manager=AfadFileManager(base_dir=str(tmp_path)))
    assert "event_id" in str(fdsn.download_single_waveforms("TK.4614..HN").error)
    assert "Vel" in str(fdsn.download_single_waveforms("TK.4614..HN", event_id="e", file_status="Vel").error)
    assert "Gecersiz" in str(fdsn.download_single_waveforms("TK", event_id="us6000jllz").error)


def test_download_batch_counts_failures(tmp_path):
    fdsn = provider(file_manager=AfadFileManager(base_dir=str(tmp_path)))
    result = fdsn.download_waveforms_batch(["TK.4614..HN", "bad"], event_ids=["us6000jllz", "us6000jllz"],
                                           export_type="mseed")
    assert result.success and result.value["downloaded"] == 1 and len(result.value["failures"]) == 1
    assert not fdsn.download_waveforms_batch(["bad"], event_id="us6000jllz").success


def test_provider_has_stable_registry_name():
    assert provider().get_name() == ProviderName.FDSN.value
