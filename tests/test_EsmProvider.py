import io
import zipfile

import pandas as pd
import pytest
import requests

from selection_service.core.error_handle import ProviderError
from selection_service.enums.enums import ProviderName
from selection_service.processing.criteria import SearchCriteria
from selection_service.processing.mappers import ColumnMapperFactory, ESMColumnMapper
from selection_service.providers.afad.afad_file_manager import AfadFileManager
from selection_service.providers.esm_provider import (
    ESMProvider,
    FLATFILE_FIELDS,
    apply_client_filters,
    parse_flatfile,
)
from selection_service.providers.providers_factory import ProviderFactory

# esm-db.eu flatfile yanit bicimi (2026-10 dogrulandi): noktali virgul, kucuk harf baslik,
# bos alan = eksik deger, event_time "YYYY-MM-DDTHH:MM:SS".
HEADER = (
    "esm_event_id;event_time;ev_latitude;ev_longitude;ev_depth_km;fm_type_code;mw;emec_mw;ms;ml;mb;"
    "es_strike;es_dip;es_rake;network_code;station_code;location_code;channel_code;st_latitude;st_longitude;"
    "preferred_vs30_m_s;epi_dist;hyp_dist;jb_dist;rup_dist;u_hp;v_hp;rotd50_pga;u_pga;v_pga;rotd50_pgv;u_pgv;v_pgv;"
    "rotd50_pgd;u_pgd;v_pgd;u_t90;v_t90;u_ia;v_ia"
)
ROWS = [
    # Norcia Mw 6.6: sonlu fay mesafeleri ve RotD50 dolu.
    "EMSC-20161030_0000029;2016-10-30T06:40:18;42.8379;13.1232;6.2;NF;6.6;;;6.1;;151;47;-89;IT;NRC;;HG;42.7925;13.0964;"
    "498;5.6;8.4;0.2;4.1;0.05;0.06;734.8;700;760;48.2;45;50;12.1;11;13;6.5;7.5;250;350",
    # Kucuk deprem: Mw yok (Ml), Rjb/Rrup bos -> Repi/Rhyp; RotD50 bos -> geometrik ortalama.
    "INT-20220209_0000149;2022-02-09T10:00:00;43.0;13.0;10;;;;;4.2;;;;;IT;NVL;00;HN;43.1;13.1;;20;22.4;;;0.1;0.2;"
    ";40;90;;;;;;;;;;",
    # Turkiye: ters faylanma, Rjb 60 km.
    "TK-2023-0001;2023-02-06T01:17:34;37.17;37.08;10;TF;7.8;;;;;;;;TK;4614;;HN;37.5;37.9;360;80;80.6;60;60.5;;;"
    "300;;;;;;;;;;;;",
]


class FakeResponse:
    def __init__(self, status_code=200, text="", content=b""):
        self.status_code = status_code
        self.text = text
        self.content = content or text.encode()


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response = response or FakeResponse(text="\n".join([HEADER, *ROWS]))
        self.error = error
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {}), timeout))
        if self.error:
            raise self.error
        return self.response


def provider(session=None, tmp_path=None):
    kwargs = {"session": session or FakeSession()}
    if tmp_path is not None:
        kwargs["file_manager"] = AfadFileManager(base_dir=str(tmp_path))
    return ESMProvider(column_mapper=ESMColumnMapper(), **kwargs)


def criteria(**kwargs):
    return SearchCriteria(start_date="2000-01-01", end_date="2024-12-31", **kwargs)


def test_flatfile_is_parsed_and_mapped_to_standard_columns():
    df = ESMColumnMapper().map_columns(parse_flatfile("\n".join([HEADER, *ROWS])))
    norcia, small, turkey = (df.iloc[i] for i in range(3))
    assert norcia["PROVIDER"] == "ESM" and norcia["EVENT"] == "EMSC-20161030_0000029"
    assert norcia["SSN"] == "IT.NRC" and norcia["FILE_NAME_H1"] == "IT.NRC..HG"
    assert norcia["RSN"] == "EMSC-20161030_0000029|IT.NRC..HG"
    assert norcia["YEAR"] == 2016 and norcia["MAGNITUDE"] == 6.6 and norcia["MAGNITUDE_TYPE"] == "Mw"
    assert norcia["MECHANISM"] == "Normal" and norcia["RAKE1"] == -89
    assert norcia["RJB(km)"] == 0.2 and norcia["RRUP(km)"] == 4.1 and norcia["VS30(m/s)"] == 498
    assert norcia["PGA(cm2/sec)"] == 734.8 and norcia["PGV(cm/sec)"] == 48.2 and norcia["PGD(cm)"] == 12.1
    assert norcia["T90_avg(sec)"] == 7.0 and norcia["ARIAS_INTENSITY(m/sec)"] == pytest.approx(3.0)
    assert norcia["LOWFREQ(Hz)"] == 0.06
    # Ml'ye duser; nokta kaynak mesafeleri; RotD50 yoksa yatay geometrik ortalama.
    assert small["MAGNITUDE"] == 4.2 and small["MAGNITUDE_TYPE"] == "Ml"
    assert small["RJB(km)"] == 20 and small["RRUP(km)"] == 22.4
    assert small["PGA(cm2/sec)"] == pytest.approx(60.0) and small["MECHANISM"] == "Unknown"
    assert small["FILE_NAME_H1"] == "IT.NVL.00.HN" and pd.isna(small["VS30(m/s)"])
    assert turkey["MECHANISM"] == "Reverse" and turkey["SSN"] == "TK.4614"
    assert parse_flatfile("").empty and parse_flatfile("   ").empty


def test_search_sends_server_filters_and_applies_the_rest_client_side():
    session = FakeSession()
    esm = provider(session)
    crit = criteria(
        min_magnitude=6.0, max_magnitude=8.0, min_depth=0, max_depth=30,
        min_Rrup=1, max_Rrup=70, min_pga=200, max_Rjb=100,
        bbox=(30.0, 45.0, 10.0, 40.0), mechanisms=["Normal", "Reverse"],
    )
    result = esm.fetch_data_sync(esm.map_criteria(crit))
    assert result.success
    df = result.value
    assert list(df["EVENT"]) == ["EMSC-20161030_0000029", "TK-2023-0001"]
    url, params, timeout = session.calls[0]
    assert url == "https://esm-db.eu/esmws/flatfile/1/query" and timeout == 60
    assert params["starttime"] == "2000-01-01" and params["endtime"] == "2024-12-31T23:59:59"
    assert params["min-magnitude"] == 6.0 and params["max-joynerboore-distance"] == 100
    assert params["min-event-latitude"] == 30.0 and params["max-event-longitude"] == 40.0
    assert params["style-of-faulting"] == "NF,TF"
    assert params["include-fields"].split(",") == list(FLATFILE_FIELDS)
    # Rrup ve PGA ESM'e gonderilmez (servis desteklemiyor).
    assert not any("rup" in key or "pga" in key for key in params)


def test_client_filters_cover_magnitude_type_distances_and_circle():
    df = ESMColumnMapper().map_columns(parse_flatfile("\n".join([HEADER, *ROWS])))
    df["REPI(km)"] = [5.6, 20, 80]
    # Sunucu min-magnitude'u herhangi bir buyukluk turune uygular; secilen buyukluk yeniden suzulur.
    assert list(apply_client_filters(df, {"min_magnitude": 5})["EVENT"]) == ["EMSC-20161030_0000029", "TK-2023-0001"]
    assert list(apply_client_filters(df, {"max_Repi": 10})["STATION"]) == ["NRC"]
    assert list(apply_client_filters(df, {"mechanisms": ["Oblique"]})["STATION"]) == []
    near_norcia = apply_client_filters(df, {"circle": [42.8, 13.1, 50]})
    assert set(near_norcia["STATION"]) == {"NRC", "NVL"}
    assert apply_client_filters(df, {}) is df


def test_unmappable_mechanism_stays_client_side_and_station_search_is_exact():
    params = criteria(mechanisms=["Oblique"], station_code="NRC", network="IT").to_esm_params()
    assert "style-of-faulting" not in params
    assert params["station"] == "NRC" and params["network"] == "IT" and params["contains-string"] == "False"
    circle = criteria(circleLatitude=40, circleLongitude=30, circleRadius=50).esm_client_filters()
    assert circle == {"circle": [40, 30, 50]}


@pytest.mark.parametrize("response, success, rows", [
    (FakeResponse(status_code=204), True, 0),
    (FakeResponse(status_code=404, text="No data"), True, 0),
    (FakeResponse(text=HEADER), True, 0),
    (FakeResponse(status_code=500, text="boom"), False, None),
])
def test_empty_and_error_responses(response, success, rows):
    result = provider(FakeSession(response)).fetch_data_sync({"query": {}, "filters": {}})
    assert result.success is success
    if success:
        assert len(result.value) == rows
    else:
        assert "500" in str(result.error)


def test_network_error_becomes_provider_error():
    result = provider(FakeSession(error=requests.ConnectionError("down"))).fetch_data_sync({"query": {}})
    assert not result.success and isinstance(result.error, ProviderError)


@pytest.mark.asyncio
async def test_async_search_uses_the_same_request():
    session = FakeSession()
    result = await provider(session).fetch_data_async({"query": {"starttime": "2016-01-01"}, "filters": {}})
    assert result.success and len(result.value) == 3
    assert session.calls[0][1]["starttime"] == "2016-01-01"


def _zip(names):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name in names:
            archive.writestr(name, "NDATA: 2\nSAMPLING_INTERVAL_S: 0.005\n1.0\n2.0\n")
    return buffer.getvalue()


def test_waveform_download_requests_eventdata_and_extracts(tmp_path):
    names = [f"IT.NRC..HG{c}.D.20161030.064018.C.ACC.ASC" for c in "ENZ"]
    session = FakeSession(FakeResponse(content=_zip(names)))
    esm = provider(session, tmp_path)
    result = esm.download_single_waveforms(
        "IT.NRC..HG", event_id="EMSC-20161030_0000029", station_code="ESM.IT.NRC",
        export_type="asc2", file_status="Acc",
    )
    assert result.success
    url, params, timeout = session.calls[0]
    assert url == "https://esm-db.eu/esmws/eventdata/1/query" and timeout == 120
    assert params == {
        "eventid": "EMSC-20161030_0000029", "network": "IT", "station": "NRC", "location": "--",
        "channel": "HG?", "format": "ascii", "processing-type": "MP", "data-type": "ACC",
    }
    extracted = sorted(p.name for p in tmp_path.rglob("*.ASC"))
    assert extracted == sorted(names)


@pytest.mark.parametrize("status, expected", [
    ("RawAcc", ("CV", "ACC")), ("Vel", ("MP", "VEL")), ("Disp", ("MP", "DIS")),
    ("ResSpecAcc", ("MP", "SA")), ("Husid", ("MP", "ACC")),
])
def test_eventdata_params_map_afad_style_options(status, expected):
    params = ESMProvider.eventdata_params("TK.4614.00.HN", "TK-2023-0001", file_status=status, export_type="mseed")
    assert (params["processing-type"], params["data-type"]) == expected
    assert params["format"] == "mseed" and params["location"] == "00" and params["channel"] == "HN?"
    assert ESMProvider.eventdata_params("TK.4614", "E", export_type="asd")["format"] == "ascii"
    assert ESMProvider.eventdata_params("TK.4614", "E")["channel"] == "*"


def test_download_errors(tmp_path):
    assert not provider(FakeSession(), tmp_path).download_single_waveforms("IT.NRC..HG").success  # event_id yok
    assert not provider(FakeSession(), tmp_path).download_single_waveforms("bad", event_id="E").success
    for response in (FakeResponse(status_code=204), FakeResponse(status_code=503, text="busy")):
        assert not provider(FakeSession(response), tmp_path).download_single_waveforms("IT.NRC..HG", event_id="E").success
    failed = provider(FakeSession(error=requests.Timeout("slow")), tmp_path)
    assert not failed.download_single_waveforms("IT.NRC..HG", event_id="E").success


def test_batch_download_counts_successes(tmp_path):
    session = FakeSession(FakeResponse(content=_zip(["a.ASC"])))
    esm = provider(session, tmp_path)
    result = esm.download_waveforms_batch(["IT.NRC..HG", "IT.NVL.00.HN"], event_ids=["E1", "E2"])
    assert result.success and result.value["downloaded"] == 2
    assert [call[1]["eventid"] for call in session.calls] == ["E1", "E2"]
    none = provider(FakeSession(FakeResponse(status_code=204)), tmp_path)
    assert not none.download_waveforms_batch(["IT.NRC..HG"], event_id="E1").success


def test_factory_and_registry_create_esm():
    created = ProviderFactory.create_provider(ProviderName.ESM, use_cache=False)
    assert isinstance(created, ESMProvider) and created.get_name() == "ESM"
    assert isinstance(ColumnMapperFactory.get_mapper(ProviderName.ESM), ESMColumnMapper)
    assert isinstance(ColumnMapperFactory.create(ProviderName.ESM), ESMColumnMapper)
