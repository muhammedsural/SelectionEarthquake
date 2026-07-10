"""FDSN event web-service provider."""

from __future__ import annotations

import asyncio
from typing import Any

import pandas as pd
from obspy import UTCDateTime
from obspy.clients.fdsn import Client

from ..core.ErrorHandle import ProviderError
from ..enums.Enums import ProviderName
from ..processing.Mappers import IColumnMapper
from ..processing.ResultHandle import Result
from ..processing.Selection import SearchCriteria
from .interfaces import IDataFetcher


class FDSNProvider(IDataFetcher):
    """Search an ObsPy-compatible FDSN event service.

    ``service`` accepts an ObsPy shortcut such as ``IRIS`` or ``USGS``, or a
    custom FDSN base URL. A client can be injected to keep tests and consuming
    applications independent from the network.
    """

    def __init__(
        self,
        column_mapper: IColumnMapper,
        service: str = "USGS",
        data_service: str = "IRIS",
        base_url: str | None = None,
        data_base_url: str | None = None,
        timeout: int = 15,
        client: Any | None = None,
        station_client: Any | None = None,
        waveform_client: Any | None = None,
        **_: Any,
    ) -> None:
        self.column_mapper = column_mapper
        self.service = service
        self.data_service = data_service
        # ObsPy 1.4.x still resolves these aliases to HTTP endpoints. Several
        # public nodes now require HTTPS, so normalize the built-in defaults.
        known_urls = {
            "USGS": "https://earthquake.usgs.gov",
            "IRIS": "https://service.iris.edu",
        }
        event_endpoint = base_url or known_urls.get(service.upper(), service)
        data_endpoint = data_base_url or known_urls.get(
            data_service.upper(), data_service
        )
        self._client = client or Client(event_endpoint, timeout=timeout)
        shared_data_client = client or station_client or waveform_client
        if shared_data_client is None:
            shared_data_client = Client(data_endpoint, timeout=timeout)
        self._station_client = station_client or shared_data_client
        self._waveform_client = waveform_client or shared_data_client

    def get_name(self) -> str:
        """Return the stable registry/cache key for every FDSN endpoint."""
        return ProviderName.FDSN.value

    def map_criteria(self, criteria: SearchCriteria) -> dict[str, Any]:
        """Map shared search criteria and omit unsupported empty values."""
        return {
            key: value
            for key, value in criteria.to_fdsn_params().items()
            if value is not None
        }

    async def fetch_data_async(self, criteria: dict[str, Any]) -> Result:
        """Run ObsPy's blocking event request outside the event loop."""
        try:
            catalog = await asyncio.to_thread(self._client.get_events, **criteria)
            return Result.ok(self._catalog_to_dataframe(catalog))
        except Exception as exc:
            return Result.fail(
                ProviderError(self.get_name(), exc, f"FDSN event query failed: {exc}")
            )

    def fetch_data_sync(self, criteria: dict[str, Any]) -> Result:
        """Search the configured FDSN endpoint synchronously."""
        try:
            catalog = self._client.get_events(**criteria)
            return Result.ok(self._catalog_to_dataframe(catalog))
        except Exception as exc:
            return Result.fail(
                ProviderError(self.get_name(), exc, f"FDSN event query failed: {exc}")
            )

    async def fetch_stations_async(self, **params: Any) -> Result:
        """Query station metadata without blocking the active event loop."""
        try:
            query = self._station_params(params)
            inventory = await asyncio.to_thread(
                self._station_client.get_stations, **query
            )
            return Result.ok(self._inventory_to_dataframe(inventory))
        except Exception as exc:
            return Result.fail(
                ProviderError(self.get_name(), exc, f"FDSN station query failed: {exc}")
            )

    def fetch_stations_sync(self, **params: Any) -> Result:
        """Query networks, stations, and channels from the FDSN endpoint."""
        try:
            inventory = self._station_client.get_stations(
                **self._station_params(params)
            )
            return Result.ok(self._inventory_to_dataframe(inventory))
        except Exception as exc:
            return Result.fail(
                ProviderError(self.get_name(), exc, f"FDSN station query failed: {exc}")
            )

    async def fetch_waveforms_async(self, **params: Any) -> Result:
        """Query waveform traces without blocking the active event loop."""
        try:
            query = self._waveform_params(params)
            stream = await asyncio.to_thread(
                self._waveform_client.get_waveforms, **query
            )
            return Result.ok(stream)
        except Exception as exc:
            return Result.fail(
                ProviderError(self.get_name(), exc, f"FDSN waveform query failed: {exc}")
            )

    def fetch_waveforms_sync(self, **params: Any) -> Result:
        """Query waveform traces for one network/station/channel selection."""
        try:
            stream = self._waveform_client.get_waveforms(
                **self._waveform_params(params)
            )
            return Result.ok(stream)
        except Exception as exc:
            return Result.fail(
                ProviderError(self.get_name(), exc, f"FDSN waveform query failed: {exc}")
            )

    def _station_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """Apply useful station defaults while preserving caller filters."""
        query = {
            "network": "*",
            "station": "*",
            "location": "*",
            "channel": "*",
            "level": "channel",
            **params,
        }
        return {key: value for key, value in query.items() if value is not None}

    def _waveform_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """Validate required FDSN dataselect parameters."""
        required = ("network", "station", "location", "channel", "starttime", "endtime")
        missing = [key for key in required if params.get(key) is None]
        if missing:
            raise ValueError(
                "Missing FDSN waveform parameters: " + ", ".join(missing)
            )
        query = {key: value for key, value in params.items() if value is not None}
        for key in ("starttime", "endtime"):
            if not isinstance(query[key], UTCDateTime):
                query[key] = UTCDateTime(query[key])
        return query

    def _inventory_to_dataframe(self, inventory: Any) -> pd.DataFrame:
        """Flatten an ObsPy Inventory to one row per channel."""
        records: list[dict[str, Any]] = []
        for network in inventory:
            for station in network:
                channels = list(station) or [None]
                for channel in channels:
                    records.append(
                        {
                            "NETWORK": getattr(network, "code", None),
                            "STATION": getattr(station, "code", None),
                            "LOCATION": getattr(channel, "location_code", None),
                            "CHANNEL": getattr(channel, "code", None),
                            "LATITUDE": getattr(channel, "latitude", None)
                            if channel is not None
                            else getattr(station, "latitude", None),
                            "LONGITUDE": getattr(channel, "longitude", None)
                            if channel is not None
                            else getattr(station, "longitude", None),
                            "ELEVATION(m)": getattr(channel, "elevation", None)
                            if channel is not None
                            else getattr(station, "elevation", None),
                            "SAMPLE_RATE": getattr(channel, "sample_rate", None),
                            "START_DATE": getattr(channel, "start_date", None),
                            "END_DATE": getattr(channel, "end_date", None),
                            "PROVIDER": f"FDSN_{self.data_service}",
                        }
                    )
        return pd.DataFrame(records)

    def _catalog_to_dataframe(self, catalog: Any) -> pd.DataFrame:
        """Convert an ObsPy Catalog into the package's standard schema."""
        records: list[dict[str, Any]] = []
        for event in catalog:
            origin = event.preferred_origin() or (event.origins[0] if event.origins else None)
            magnitude = event.preferred_magnitude() or (
                event.magnitudes[0] if event.magnitudes else None
            )
            if origin is None:
                continue

            resource_id = getattr(event, "resource_id", None)
            event_id = getattr(resource_id, "id", None) or str(resource_id or "")
            depth = getattr(origin, "depth", None)
            origin_time = getattr(origin, "time", None)
            records.append(
                {
                    "RSN": event_id,
                    "EVENT": event_id,
                    "YEAR": getattr(origin_time, "year", None),
                    "MAGNITUDE": getattr(magnitude, "mag", None),
                    "MAGNITUDE_TYPE": getattr(magnitude, "magnitude_type", None),
                    "HYPO_LAT": getattr(origin, "latitude", None),
                    "HYPO_LON": getattr(origin, "longitude", None),
                    "HYPO_DEPTH(km)": depth / 1000.0 if depth is not None else None,
                    "ENDPOINTSOURCE": event_id or None,
                }
            )

        mapped = self.column_mapper.map_columns(pd.DataFrame(records))
        mapped["PROVIDER"] = f"FDSN_{self.service}"
        return mapped
