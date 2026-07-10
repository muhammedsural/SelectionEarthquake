from types import SimpleNamespace

import pandas as pd
import pytest

from selection_service.enums.Enums import ProviderName
from selection_service.processing.Mappers import FDSNColumnMapper
from selection_service.processing.Selection import SearchCriteria
from selection_service.providers.FdsnProvider import FDSNProvider


class FakeEvent:
    def __init__(self):
        self.resource_id = SimpleNamespace(id="smi:service/event/123")
        self.origins = [
            SimpleNamespace(
                time=SimpleNamespace(year=2023),
                latitude=37.1,
                longitude=37.2,
                depth=7500.0,
            )
        ]
        self.magnitudes = [SimpleNamespace(mag=6.4, magnitude_type="Mw")]

    def preferred_origin(self):
        return self.origins[0]

    def preferred_magnitude(self):
        return self.magnitudes[0]


class FakeClient:
    def __init__(self, catalog=None, inventory=None, stream=None, error=None):
        self.catalog = catalog or []
        self.inventory = inventory or []
        self.stream = stream
        self.error = error
        self.calls = []

    def get_events(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.catalog

    def get_stations(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.inventory

    def get_waveforms(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.stream


def criteria(**kwargs):
    defaults = {
        "start_date": "2023-02-06",
        "end_date": "2023-02-07",
        "min_magnitude": 5.0,
        "max_magnitude": 7.0,
        "min_depth": 1.0,
        "max_depth": 20.0,
        "bbox": (35.0, 39.0, 35.0, 40.0),
    }
    defaults.update(kwargs)
    return SearchCriteria(**defaults)


def test_fdsn_maps_supported_search_criteria_without_empty_values():
    provider = FDSNProvider(FDSNColumnMapper(), client=FakeClient())

    mapped = provider.map_criteria(criteria())

    assert mapped == {
        "starttime": "2023-02-06T00:00:00.000Z",
        "endtime": "2023-02-07T23:59:59.999Z",
        "minmagnitude": 5.0,
        "maxmagnitude": 7.0,
        "mindepth": 1.0,
        "maxdepth": 20.0,
        "minlatitude": 35.0,
        "maxlatitude": 39.0,
        "minlongitude": 35.0,
        "maxlongitude": 40.0,
    }


def test_fdsn_preserves_full_iso_timestamps():
    mapped = criteria(
        start_date="2023-02-06T01:16:00.000Z",
        end_date="2023-02-06T01:18:41.000Z",
    ).to_fdsn_params()

    assert mapped["starttime"] == "2023-02-06T01:16:00.000Z"
    assert mapped["endtime"] == "2023-02-06T01:18:41.000Z"


def test_fdsn_sync_search_returns_standardized_event_rows():
    client = FakeClient([FakeEvent()])
    provider = FDSNProvider(FDSNColumnMapper(), service="USGS", client=client)
    mapped = provider.map_criteria(criteria())

    result = provider.fetch_data_sync(mapped)

    assert result.success
    assert isinstance(result.value, pd.DataFrame)
    row = result.value.iloc[0]
    assert row["RSN"] == "smi:service/event/123"
    assert row["MAGNITUDE"] == 6.4
    assert row["HYPO_DEPTH(km)"] == 7.5
    assert row["PROVIDER"] == "FDSN_USGS"
    assert client.calls == [mapped]


@pytest.mark.asyncio
async def test_fdsn_async_search_uses_same_mapping_and_schema():
    client = FakeClient([FakeEvent()])
    provider = FDSNProvider(FDSNColumnMapper(), client=client)
    mapped = provider.map_criteria(criteria())

    result = await provider.fetch_data_async(mapped)

    assert result.success
    assert result.value.iloc[0]["PROVIDER"] == "FDSN_USGS"
    assert client.calls == [mapped]


def test_fdsn_returns_provider_error_on_client_failure():
    provider = FDSNProvider(
        FDSNColumnMapper(), client=FakeClient(error=RuntimeError("offline"))
    )

    result = provider.fetch_data_sync(provider.map_criteria(criteria()))

    assert not result.success
    assert "FDSN event query failed" in str(result.error)


def test_fdsn_provider_has_stable_registry_name():
    provider = FDSNProvider(FDSNColumnMapper(), service="USGS", client=FakeClient())
    assert provider.get_name() == ProviderName.FDSN.value


class InventoryStation(list):
    code = "ANK"
    latitude = 39.0
    longitude = 32.0
    elevation = 900.0


class InventoryNetwork(list):
    code = "TU"


def inventory_fixture():
    channel = SimpleNamespace(
        code="HNZ",
        location_code="00",
        latitude=39.1,
        longitude=32.2,
        elevation=910.0,
        sample_rate=100.0,
        start_date="2020-01-01",
        end_date=None,
    )
    return [InventoryNetwork([InventoryStation([channel])])]


def test_fdsn_station_search_flattens_channel_inventory():
    client = FakeClient(inventory=inventory_fixture())
    provider = FDSNProvider(FDSNColumnMapper(), service="USGS", client=client)

    result = provider.fetch_stations_sync(network="TU", station="ANK", channel="HN?")

    assert result.success
    assert result.value.to_dict("records") == [
        {
            "NETWORK": "TU",
            "STATION": "ANK",
            "LOCATION": "00",
            "CHANNEL": "HNZ",
            "LATITUDE": 39.1,
            "LONGITUDE": 32.2,
            "ELEVATION(m)": 910.0,
            "SAMPLE_RATE": 100.0,
            "START_DATE": "2020-01-01",
            "END_DATE": None,
            "PROVIDER": "FDSN_IRIS",
        }
    ]
    assert client.calls[0]["level"] == "channel"


@pytest.mark.asyncio
async def test_fdsn_station_search_async_uses_client():
    client = FakeClient(inventory=inventory_fixture())
    provider = FDSNProvider(FDSNColumnMapper(), client=client)

    result = await provider.fetch_stations_async(network="TU")

    assert result.success
    assert result.value.iloc[0]["CHANNEL"] == "HNZ"


def waveform_params():
    return {
        "network": "TU",
        "station": "ANK",
        "location": "00",
        "channel": "HN?",
        "starttime": "2023-02-06T01:16:00Z",
        "endtime": "2023-02-06T01:17:00Z",
    }


def test_fdsn_waveform_search_returns_stream_unchanged():
    stream = object()
    client = FakeClient(stream=stream)
    provider = FDSNProvider(FDSNColumnMapper(), client=client)

    result = provider.fetch_waveforms_sync(**waveform_params())

    assert result.success
    assert result.value is stream
    assert client.calls[0]["network"] == "TU"
    assert str(client.calls[0]["starttime"]) == "2023-02-06T01:16:00.000000Z"
    assert str(client.calls[0]["endtime"]) == "2023-02-06T01:17:00.000000Z"


@pytest.mark.asyncio
async def test_fdsn_waveform_search_async_returns_stream():
    stream = object()
    provider = FDSNProvider(FDSNColumnMapper(), client=FakeClient(stream=stream))

    result = await provider.fetch_waveforms_async(**waveform_params())

    assert result.success
    assert result.value is stream


def test_fdsn_waveform_search_reports_missing_required_parameter():
    provider = FDSNProvider(FDSNColumnMapper(), client=FakeClient())
    params = waveform_params()
    del params["channel"]

    result = provider.fetch_waveforms_sync(**params)

    assert not result.success
    assert "Missing FDSN waveform parameters: channel" in str(result.error)
