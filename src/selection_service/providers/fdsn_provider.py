"""FDSN web servisleri saglayicisi (ObsPy gerektirmez).

FDSN standardindaki uc servis duz HTTP ile kullanilir:

- ``fdsnws/event/1``: olay katalogu (``format=text``),
- ``fdsnws/station/1``: istasyon ve kanal bilgisi (``format=text``),
- ``fdsnws/dataselect/1``: ham miniSEED dalga formu.

Olay katalogu ve veri merkezi ayri dugumler olabilir (ornegin USGS katalogu,
IRIS veya KOERI veri merkezi). ``record_search=True`` iken her deprem icin
cevresindeki ivme olcer istasyonlari aranir ve kayit basina bir satir
dondurulur; boylece FDSN kayitlari da secilip indirilebilir. Varsayilan
(``False``) yalniz olay katalogunu dondurur.

Mesafeler nokta kaynak varsayimiyla hesaplanir: ``RJB(km)`` merkez ussu
mesafesi, ``RRUP(km)`` hiposantr mesafesidir. Vs30, PGA ve faylanma turu FDSN
servislerinde yoktur; bu alanlara bagli filtreler FDSN satirlarina uygulanmaz.
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from ..core.error_handle import ProviderError
from ..enums.enums import ProviderName
from ..processing.criteria import SearchCriteria
from ..processing.mappers import IColumnMapper
from ..processing.result_handle import Result, result_decorator
from ..waveform import Trace, align, read_mseed, write_ascii
from .afad.afad_file_manager import AfadFileManager
from .esm_provider import apply_client_filters
from .interfaces import IDataFetcher, IWaveformDownloader

logger = logging.getLogger(__name__)

# Bilinen dugumlerin kok adresleri; diger dugumler base_url ile verilir.
FDSN_NODES = {
    "USGS": "https://earthquake.usgs.gov",
    "IRIS": "https://service.earthscope.org",
    "EARTHSCOPE": "https://service.earthscope.org",
    "EMSC": "https://www.seismicportal.eu",
    "ISC": "https://www.isc.ac.uk",
    "GFZ": "https://geofon.gfz.de",
    "INGV": "https://webservices.ingv.it",
    "ORFEUS": "https://www.orfeus-eu.org",
    "KOERI": "https://eida.koeri.boun.edu.tr",
    "RESIF": "https://ws.resif.fr",
    "ETH": "https://eida.ethz.ch",
    "NOA": "https://eida.gein.noa.gr",
    "GEONET": "https://service.geonet.org.nz",
    "NCEDC": "https://service.ncedc.org",
    "SCEDC": "https://service.scedc.caltech.edu",
    "RASPISHAKE": "https://data.raspberryshake.org",
}

# Ivme olcer kanallari, oncelik sirasiyla (SEED alet kodu N; ESM'de HG de ivmedir).
DEFAULT_CHANNELS = ("HN", "HG", "EN", "BN")
KM_PER_DEGREE = 111.19492664455873
# Mesafe sinirlari disindaki filtreler FDSN verisinde bulunmayan alanlara dayanir.
UNSUPPORTED_FILTERS = ("min_vs30", "max_vs30", "min_pga", "max_pga", "min_pgv", "max_pgv",
                       "min_pgd", "max_pgd", "mechanisms")
ACCELERATION_UNITS = {"M/S**2", "M/S2", "M/S/S", "M/SEC**2", "M/S^2"}


def _node_url(name_or_url: str) -> str:
    value = str(name_or_url or "").strip()
    return FDSN_NODES.get(value.upper(), value).rstrip("/")


def parse_text_table(text: str) -> List[Dict[str, str]]:
    """FDSN ``format=text`` yanitini (``#`` ile baslayan baslik, ``|`` ayrac) satirlara cevirir."""
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines or not lines[0].startswith("#"):
        return []
    header = [name.strip().lower() for name in lines[0].lstrip("#").split("|")]
    rows = []
    for line in lines[1:]:
        if line.startswith("#"):
            continue
        values = [value.strip() for value in line.split("|")]
        rows.append(dict(zip(header, values + [""] * (len(header) - len(values)))))
    return rows


def _float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def _parse_time(value: Any) -> Optional[datetime]:
    text = str(value or "").strip().replace("Z", "")
    if not text:
        return None
    for pattern in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:26], pattern).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _fdsn_time(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def epicentral_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    a = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * 6371.0088 * math.asin(min(1.0, math.sqrt(a)))


class FDSNProvider(IDataFetcher, IWaveformDownloader):
    """FDSN olay aramasi; istege bagli kayit aramasi ve dalga formu indirme."""

    def __init__(
        self,
        column_mapper: IColumnMapper,
        service: str = "USGS",
        data_service: str = "IRIS",
        base_url: str | None = None,
        data_base_url: str | None = None,
        timeout: int = 30,
        session: Any | None = None,
        record_search: bool = False,
        channels: Tuple[str, ...] = DEFAULT_CHANNELS,
        max_events: int = 20,
        default_radius_km: float = 150.0,
        pre_event_s: float = 30.0,
        file_manager: AfadFileManager | None = None,
        **_: Any,
    ) -> None:
        self.column_mapper = column_mapper
        self.service = str(service).upper()
        self.data_service = str(data_service).upper()
        self.event_url = _node_url(base_url or service)
        self.data_url = _node_url(data_base_url or data_service)
        self.timeout = timeout
        self.session = session or requests.Session()
        self.record_search = record_search
        self.channels = tuple(code.upper()[:2] for code in channels)
        self.max_events = max_events
        self.default_radius_km = default_radius_km
        self.pre_event_s = pre_event_s
        self.file_manager = file_manager or AfadFileManager(base_dir="FDSN_events")
        # Kayit aramasinda gorulen depremler; indirmede zaman penceresi icin kullanilir.
        self._events: Dict[str, Dict[str, Any]] = {}
        self._distances: Dict[Tuple[str, str], float] = {}

    # ──────────────────────────────────────────────────────────────
    # IDataFetcher sozlesmesi
    # ──────────────────────────────────────────────────────────────

    def get_name(self) -> str:
        """Kayit defteri ve onbellek anahtari tum dugumler icin ayni kalir."""
        return ProviderName.FDSN.value

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        params = {key: value for key, value in criteria.to_fdsn_params().items() if value is not None}
        if not self.record_search:
            return params
        filters = {key: value for key, value in criteria.esm_client_filters().items()
                   if key not in UNSUPPORTED_FILTERS}
        distances = [filters.get(key) for key in ("max_Repi", "max_Rjb", "max_Rrup", "max_Rhyp")]
        radius = max([value for value in distances if value is not None], default=self.default_radius_km)
        return {
            "events": params,
            "stations": {"network": criteria.network, "station": criteria.station_code},
            "radius_km": float(radius),
            "filters": filters,
        }

    async def fetch_data_async(self, criteria: Dict[str, Any]) -> Result:
        return await asyncio.to_thread(self.fetch_data_sync, criteria)

    def fetch_data_sync(self, criteria: Dict[str, Any]) -> Result:
        try:
            if self.record_search:
                return Result.ok(self._search_records(criteria))
            return Result.ok(self._events_frame(self._query_events(criteria)))
        except Exception as exc:
            return Result.fail(ProviderError(self.get_name(), exc, f"FDSN event query failed: {exc}"))

    # ──────────────────────────────────────────────────────────────
    # Istasyon ve dalga formu sorgulari
    # ──────────────────────────────────────────────────────────────

    async def fetch_stations_async(self, **params: Any) -> Result:
        return await asyncio.to_thread(self.fetch_stations_sync, **params)

    def fetch_stations_sync(self, **params: Any) -> Result:
        """Kanal basina bir satir (ag, istasyon, konum, kanal, konum, duyarlilik)."""
        try:
            return Result.ok(self._stations_frame(self._query_stations(params)))
        except Exception as exc:
            return Result.fail(ProviderError(self.get_name(), exc, f"FDSN station query failed: {exc}"))

    async def fetch_waveforms_async(self, **params: Any) -> Result:
        return await asyncio.to_thread(self.fetch_waveforms_sync, **params)

    def fetch_waveforms_sync(self, **params: Any) -> Result:
        """Dataselect sorgusu; cozulmus :class:`~selection_service.waveform.Trace` listesi doner."""
        try:
            return Result.ok(read_mseed(self._dataselect(params)))
        except Exception as exc:
            return Result.fail(ProviderError(self.get_name(), exc, f"FDSN waveform query failed: {exc}"))

    # ──────────────────────────────────────────────────────────────
    # IWaveformDownloader sozlesmesi
    # ──────────────────────────────────────────────────────────────

    @result_decorator
    def download_single_waveforms(self, filename: str, **kwargs: Any) -> bool:
        """Bir kaydin uc bilesenini indirir.

        ``filename`` aramadaki ``FILE_NAME_H1`` degeridir (``AG.ISTASYON.KONUM.BANT``,
        ornegin ``TK.4614..HN``). ``export_type`` ``mseed`` ise ham miniSEED
        (sayim) yazilir; ``asc2``/``ascii`` icin kanal duyarliligiyla fiziksel
        birime cevrilmis ESM benzeri ASCII dosyalari yazilir. Yalniz ivme
        (``RawAcc``/``Acc``) istenebilir; islenmis seriler FDSN'de yoktur.
        """
        event_id = str(kwargs.get("event_id") or "").strip()
        if not event_id:
            raise ProviderError(self.get_name(), None, "FDSN indirmesi icin event_id gerekli")
        status = str(kwargs.get("file_status") or "Acc")
        if status not in ("RawAcc", "Acc"):
            raise ProviderError(self.get_name(), None,
                                f"FDSN yalniz ham ivme kaydi verir; {status} desteklenmiyor")
        network, station, location, band = self._split_filename(filename)
        event = self._event(event_id)
        distance = self._distances.get((event_id, filename))
        start = event["time"] - timedelta(seconds=self.pre_event_s)
        end = event["time"] + timedelta(seconds=self._post_event_s(distance))
        query = {"network": network, "station": station, "location": location or "--",
                 "channel": f"{band}?", "starttime": _fdsn_time(start), "endtime": _fdsn_time(end)}
        raw = self._dataselect(query)
        target = Path(self.file_manager.base_dir) / f"event_{_safe(event_id)}"
        target.mkdir(parents=True, exist_ok=True)
        label = _safe(f"{network}.{station}.{location}.{band}")
        export_type = str(kwargs.get("export_type") or "asc2").lower()
        if export_type == "mseed":
            (target / f"fdsn_{label}.mseed").write_bytes(raw)
            return True
        # Veri merkezi parcalari pencereye kirpmadan dondurebilir; bilesenler istenen
        # pencereye ve ortak araliga kirpilir (ayni baslangic, ayni ornek sayisi).
        traces = self.calibrate(align(read_mseed(raw), start, end), self._query_stations(query))
        if not traces:
            raise ProviderError(self.get_name(), None, f"{filename} icin dalga formu yok")
        write_ascii(traces, target, f"fdsn_{label}", header={
            "EVENT_ID": event_id, "EVENT_DATE_TIME": _fdsn_time(event["time"]),
            "EVENT_LATITUDE_DEGREE": event.get("latitude"), "EVENT_LONGITUDE_DEGREE": event.get("longitude"),
            "EVENT_DEPTH_KM": event.get("depth"), "MAGNITUDE": event.get("magnitude"),
            "DATA_SOURCE": self.data_url,
        })
        return True

    @result_decorator
    def download_waveforms_batch(self, filenames: List[str], **kwargs: Any) -> Dict[str, Any]:
        event_ids = kwargs.pop("event_ids", None) or kwargs.pop("event_id", None)
        if not isinstance(event_ids, (list, tuple)):
            event_ids = [event_ids] * len(filenames)
        downloaded, failures = 0, []
        for filename, event_id in zip(filenames, event_ids):
            result = self.download_single_waveforms(filename, event_id=event_id, **kwargs)
            if result.success:
                downloaded += 1
            else:
                failures.append(f"{filename}: {result.error}")
        if filenames and downloaded == 0:
            raise ProviderError(self.get_name(), None, "; ".join(failures) or "FDSN indirmesi basarisiz")
        return {"total": len(filenames), "downloaded": downloaded, "failures": failures}

    @staticmethod
    def calibrate(traces: List[Trace], channels: List[Dict[str, str]]) -> List[Trace]:
        """Sayimlari kanal duyarliligina boler; ivmeyi cm/s^2 olarak dondurur.

        Duyarlilik (``Scale``) referans frekansinda gecerli tek bir kazanctir; ivme
        olcerlerin duz bandinda alet tepkisinin giderilmesine esdegerdir.
        """
        scales = {}
        for row in channels:
            key = ".".join(row.get(name, "") for name in ("network", "station", "location", "channel"))
            scales[key] = (_float(row.get("scale")), str(row.get("scaleunits", "")).upper())
        calibrated = []
        for trace in traces:
            scale, units = scales.get(trace.id, (math.nan, ""))
            if not math.isfinite(scale) or scale == 0:
                logger.warning("%s icin duyarlilik yok; kayit sayim olarak birakildi", trace.id)
                calibrated.append(trace)
                continue
            data = trace.data / scale
            if units.replace(" ", "") in ACCELERATION_UNITS:
                data, units = data * 100.0, "cm/s^2"
            calibrated.append(Trace(trace.network, trace.station, trace.location, trace.channel,
                                    trace.starttime, trace.sampling_rate, data, units or "unknown",
                                    {"SENSITIVITY": scale}))
        return calibrated

    # ──────────────────────────────────────────────────────────────
    # HTTP
    # ──────────────────────────────────────────────────────────────

    def _get(self, url: str, params: Dict[str, Any]) -> Optional[requests.Response]:
        query = {key: value for key, value in params.items() if value not in (None, "")}
        try:
            response = self.session.get(url, params=query, timeout=self.timeout)
        except requests.RequestException as exc:
            raise ProviderError(self.get_name(), exc, f"FDSN baglanti hatasi: {exc}") from exc
        if response.status_code in (204, 404):
            return None
        if response.status_code >= 400:
            raise ProviderError(self.get_name(), None,
                                f"FDSN HTTP {response.status_code}: {response.text[:200]}")
        return response

    def _query_events(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self._get(f"{self.event_url}/fdsnws/event/1/query", {**params, "format": "text"})
        events = []
        for row in parse_text_table(response.text if response is not None else ""):
            moment = _parse_time(row.get("time"))
            event_id = row.get("eventid", "")
            if moment is None or not event_id:
                continue
            event = {
                "id": event_id, "time": moment,
                "latitude": _float(row.get("latitude")), "longitude": _float(row.get("longitude")),
                "depth": _float(row.get("depth/km")),
                "magnitude": _float(row.get("magnitude")), "magnitude_type": row.get("magtype") or None,
                "name": row.get("eventlocationname") or None,
            }
            self._events[event_id] = event
            events.append(event)
        return events

    def _event(self, event_id: str) -> Dict[str, Any]:
        if event_id not in self._events:
            self._query_events({"eventid": event_id})
        if event_id not in self._events:
            raise ProviderError(self.get_name(), None, f"FDSN olayi bulunamadi: {event_id}")
        return self._events[event_id]

    def _query_stations(self, params: Dict[str, Any]) -> List[Dict[str, str]]:
        query = {"network": "*", "station": "*", "location": "*", "channel": "*", "level": "channel",
                 **params, "format": "text"}
        if str(query.get("level")).lower() != "channel":
            query["level"] = "channel"
        response = self._get(f"{self.data_url}/fdsnws/station/1/query", query)
        return parse_text_table(response.text if response is not None else "")

    def _dataselect(self, params: Dict[str, Any]) -> bytes:
        required = ("network", "station", "location", "channel", "starttime", "endtime")
        missing = [key for key in required if params.get(key) in (None, "")]
        if missing:
            raise ValueError("Missing FDSN waveform parameters: " + ", ".join(missing))
        query = dict(params)
        for key in ("starttime", "endtime"):
            if isinstance(query[key], datetime):
                query[key] = _fdsn_time(query[key])
        response = self._get(f"{self.data_url}/fdsnws/dataselect/1/query", query)
        if response is None or not response.content:
            raise ProviderError(self.get_name(), None, "FDSN veri merkezinde bu kayit icin veri yok")
        return response.content

    # ──────────────────────────────────────────────────────────────
    # Tablolar
    # ──────────────────────────────────────────────────────────────

    def _events_frame(self, events: List[Dict[str, Any]]) -> pd.DataFrame:
        records = [{
            "RSN": event["id"], "EVENT": event["id"], "YEAR": event["time"].year,
            "MAGNITUDE": event["magnitude"], "MAGNITUDE_TYPE": event["magnitude_type"],
            "HYPO_LAT": event["latitude"], "HYPO_LON": event["longitude"], "HYPO_DEPTH(km)": event["depth"],
            "ENDPOINTSOURCE": f"{self.event_url}/fdsnws/event/1/query?eventid={event['id']}",
        } for event in events]
        mapped = self.column_mapper.map_columns(pd.DataFrame(records))
        mapped["PROVIDER"] = f"FDSN_{self.service}"
        return mapped

    def _stations_frame(self, rows: List[Dict[str, str]]) -> pd.DataFrame:
        return pd.DataFrame([{
            "NETWORK": row.get("network"), "STATION": row.get("station"),
            "LOCATION": row.get("location"), "CHANNEL": row.get("channel"),
            "LATITUDE": _float(row.get("latitude")), "LONGITUDE": _float(row.get("longitude")),
            "ELEVATION(m)": _float(row.get("elevation")), "SAMPLE_RATE": _float(row.get("samplerate")),
            "SENSITIVITY": _float(row.get("scale")), "SENSITIVITY_UNITS": row.get("scaleunits") or None,
            "START_DATE": row.get("starttime") or None, "END_DATE": row.get("endtime") or None,
            "PROVIDER": f"FDSN_{self.data_service}",
        } for row in rows])

    def _search_records(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        events = self._query_events(criteria.get("events") or {})
        events.sort(key=lambda item: -item["magnitude"] if math.isfinite(item["magnitude"]) else 0)
        if len(events) > self.max_events:
            logger.info("FDSN: %d depremden en buyuk %d tanesi icin istasyon aranacak",
                        len(events), self.max_events)
            events = events[: self.max_events]
        radius = float(criteria.get("radius_km") or self.default_radius_km)
        station_filter = criteria.get("stations") or {}
        records = []
        for event in events:
            if not (math.isfinite(event["latitude"]) and math.isfinite(event["longitude"])):
                continue
            try:
                channels = self._query_stations({
                    "network": station_filter.get("network") or "*",
                    "station": station_filter.get("station") or "*",
                    "channel": ",".join(f"{band}?" for band in self.channels),
                    "latitude": event["latitude"], "longitude": event["longitude"],
                    "maxradius": round(radius / KM_PER_DEGREE, 4),
                    "starttime": _fdsn_time(event["time"]),
                    "endtime": _fdsn_time(event["time"] + timedelta(seconds=1)),
                })
            except ProviderError as exc:
                logger.warning("FDSN istasyon sorgusu basarisiz (%s): %s", event["id"], exc)
                continue
            records.extend(self._event_records(event, channels))
        if not records:
            return self.column_mapper.map_columns(pd.DataFrame())
        frame = pd.DataFrame(records)
        mapped = self.column_mapper.map_columns(frame)
        mapped["PROVIDER"] = f"FDSN_{self.service}"
        mapped["REPI(km)"] = frame["REPI(km)"]
        mapped["RHYP(km)"] = frame["RRUP(km)"]
        filtered = apply_client_filters(mapped, criteria.get("filters") or {})
        filtered = filtered.drop(columns=["REPI(km)", "RHYP(km)"]).reset_index(drop=True)
        for _, row in filtered.iterrows():
            self._distances[(str(row["EVENT"]), str(row["FILE_NAME_H1"]))] = _float(row["RJB(km)"])
        logger.info("FDSN'den %d kayit alindi (%d filtre sonrasi).", len(frame), len(filtered))
        return filtered

    def _event_records(self, event: Dict[str, Any], channels: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        best: Dict[Tuple[str, str], Tuple[Tuple[int, str], Dict[str, str]]] = {}
        for row in channels:
            band = row.get("channel", "")[:2].upper()
            if band not in self.channels:
                continue
            key = (row.get("network", ""), row.get("station", ""))
            rank = (self.channels.index(band), row.get("location", ""))
            if key not in best or rank < best[key][0]:
                best[key] = (rank, row)
        records = []
        depth = event["depth"] if math.isfinite(event["depth"]) else 0.0
        for (network, station), (_, row) in best.items():
            lat, lon = _float(row.get("latitude")), _float(row.get("longitude"))
            if not (math.isfinite(lat) and math.isfinite(lon)):
                continue
            repi = epicentral_km(event["latitude"], event["longitude"], lat, lon)
            filename = f"{network}.{station}.{row.get('location', '')}.{row.get('channel', '')[:2].upper()}"
            records.append({
                "RSN": f"{event['id']}|{filename}", "EVENT": event["id"], "YEAR": event["time"].year,
                "MAGNITUDE": event["magnitude"], "MAGNITUDE_TYPE": event["magnitude_type"],
                "HYPO_LAT": event["latitude"], "HYPO_LON": event["longitude"], "HYPO_DEPTH(km)": event["depth"],
                "STATION": station, "SSN": f"{network}.{station}", "STATION_ID": f"{network}.{station}",
                "STATION_LAT": lat, "STATION_LON": lon, "MECHANISM": "Unknown",
                "RJB(km)": round(repi, 3), "RRUP(km)": round(math.hypot(repi, depth), 3), "REPI(km)": repi,
                "FILE_NAME_H1": filename,
                "ENDPOINTSOURCE": f"{self.event_url}/fdsnws/event/1/query?eventid={event['id']}",
            })
        return records

    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def _split_filename(filename: str) -> Tuple[str, str, str, str]:
        parts = (str(filename).split(".") + ["", "", "", ""])[:4]
        network, station, location, band = (part.strip() for part in parts)
        if not network or not station or len(band) < 2:
            raise ProviderError(ProviderName.FDSN.value, None, f"Gecersiz FDSN kayit adi: {filename}")
        return network, station, location, band[:2].upper()

    @staticmethod
    def _post_event_s(distance_km: Optional[float]) -> float:
        """S dalgasi ve kuyruk icin pencere: 120 s + mesafe / 3.5 km/s (en az 180 s)."""
        if distance_km is None or not math.isfinite(distance_km):
            return 300.0
        return max(180.0, 120.0 + distance_km / 3.5)


def _safe(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in str(value))
