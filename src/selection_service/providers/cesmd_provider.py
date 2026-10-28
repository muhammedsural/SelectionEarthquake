"""CESMD (Center for Engineering Strong Motion Data) veri saglayicisi.

CESMD (https://strongmotioncenter.org, USGS/CGS) ABD basta olmak uzere dunyanin
dort bir yanindaki kuvvetli yer hareketi kayitlarini sunar. Iki servis kullanilir:

- ``wserv/records/query`` (``rettype=metadata``, ``groupby=event``): deprem ve
  istasyon bilgisi ile kayit basina PGA/PGV/PGD, Repi ve fay mesafesi. Anahtar
  gerekmez.
- ``wserv/records/query`` (``rettype=dataset``): kayitlarin ZIP olarak indirilmesi
  (``download=P`` islenmis, ``R`` ham). Servis kayitli bir e-posta ister
  (https://strongmotioncenter.org/cgi-bin/CESMD/register.pl); e-posta
  ``email=`` argumani veya ``CESMD_EMAIL`` ortam degiskeniyle verilir. Arama
  e-postasiz calisir, yalniz indirme icin gerekir.

Sunucu tarafinda tarih, buyukluk, deprem kutusu/dairesi, Repi ve istasyon turu
suzulur; digerleri (Rjb, Rrup, Vs30, PGA/PGV/PGD, mekanizma) yanit geldikten sonra
uygulanir. Buyukluk sinirlari sunucuda herhangi bir buyukluk turune gore
uygulandigi icin istemcide yeniden uygulanir.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List

import pandas as pd
import requests

from ..core.error_handle import ProviderError
from ..enums.enums import ProviderName
from ..processing.criteria import SearchCriteria
from ..processing.mappers import IColumnMapper
from ..processing.result_handle import result_decorator
from .afad.afad_file_manager import AfadFileManager
from .esm_provider import apply_client_filters
from .interfaces import IDataFetcher, IWaveformDownloader

logger = logging.getLogger(__name__)

CESMD_BASE_URL = "https://strongmotioncenter.org"
RECORDS_PATH = "/wserv/records/query"
REGISTER_URL = "https://strongmotioncenter.org/cgi-bin/CESMD/register.pl"
EMAIL_ENV = "CESMD_EMAIL"

# Ayni istasyonu ayirt etmek icin Repi penceresi (servis 0.1 km duyarlikla verir).
EPIDIST_TOLERANCE_KM = 0.06

# Uygulamalarin AFAD'a gore verdigi file_status -> CESMD download (P islenmis, R ham).
RAW_STATUSES = {"RawAcc"}


def make_file_key(network: str, station: str, epidist: Any) -> str:
    """Arama satirindaki indirme anahtari: ``AG.ISTASYON~REPI`` (REPI km)."""
    key = f"{network}.{station}"
    try:
        return f"{key}~{float(epidist):.1f}"
    except (TypeError, ValueError):
        return key


def parse_file_key(filename: str) -> tuple[str, str, float | None]:
    """``AG.ISTASYON~REPI`` anahtarini (ag, istasyon, repi) olarak ayirir."""
    name, _, epidist = str(filename).partition("~")
    network, _, station = name.partition(".")
    if not network.strip() or not station.strip():
        raise ProviderError(ProviderName.CESMD.value, None, f"Gecersiz CESMD kayit adi: {filename}")
    try:
        epi = float(epidist) if epidist.strip() else None
    except ValueError:
        epi = None
    return network.strip(), station.strip(), epi


def records_to_frame(payload: Dict[str, Any]) -> pd.DataFrame:
    """``groupby=event`` yanitini kayit basina bir satirlik duz tabloya cevirir."""
    rows: List[Dict[str, Any]] = []
    events = ((payload or {}).get("results") or {}).get("events") or []
    for event in events:
        event_fields = {f"ev_{key}": value for key, value in event.items() if key != "stations"}
        for station in event.get("stations") or []:
            record = station.get("record") or {}
            availability = record.get("data_availability") or {}
            row = dict(event_fields)
            row.update(
                {
                    "st_network": station.get("network"),
                    "st_code": station.get("code"),
                    "st_name": station.get("name"),
                    "st_type": station.get("type"),
                    "st_latitude": station.get("latitude"),
                    "st_longitude": station.get("longitude"),
                    "st_vs30": station.get("Vs30"),
                    "st_siteclass": station.get("siteclass"),
                    "rec_epidist": record.get("epidist"),
                    "rec_fault_dist": record.get("fault_dist"),
                    "rec_pgav1": record.get("pgav1"),
                    "rec_pgav2": record.get("pgav2"),
                    "rec_pgv": record.get("pgv"),
                    "rec_pgd": record.get("pgd"),
                    "rec_raw": str(availability.get("raw", "")).lower() == "true",
                    "rec_processed": str(availability.get("processed", "")).lower() == "true",
                }
            )
            rows.append(row)
    return pd.DataFrame(rows)


class CESMDProvider(IDataFetcher, IWaveformDownloader):
    """CESMD records servisiyle arama ve ZIP dataset indirme."""

    def __init__(
        self,
        column_mapper: IColumnMapper,
        timeout: int = 90,
        base_url: str = CESMD_BASE_URL,
        session: Any | None = None,
        file_manager: AfadFileManager | None = None,
        email: str | None = None,
        **_: Any,
    ) -> None:
        self.name = ProviderName.CESMD.value
        self.column_mapper = column_mapper
        self.timeout = timeout
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.file_manager = file_manager or AfadFileManager(base_dir="CESMD_events")
        self.email = email

    # ──────────────────────────────────────────────────────────────
    # IDataFetcher sozlesmesi
    # ──────────────────────────────────────────────────────────────

    def get_name(self) -> str:
        return str(self.name)

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        return {"query": criteria.to_cesmd_params(), "filters": criteria.esm_client_filters()}

    async def fetch_data_async(self, criteria: Dict[str, Any]):
        import asyncio

        return await asyncio.to_thread(self.fetch_data_sync, criteria)

    @result_decorator
    def fetch_data_sync(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        query = dict(criteria.get("query") or {})
        query.update({"rettype": "metadata", "format": "json", "groupby": "event"})
        try:
            response = self.session.get(
                f"{self.base_url}{RECORDS_PATH}", params=query, timeout=self.timeout
            )
        except requests.RequestException as exc:
            raise ProviderError(self.name, exc, f"CESMD baglanti hatasi: {exc}") from exc
        # "Veri yok": 204 veya 404 ("No data found").
        if response.status_code in (204, 404):
            return pd.DataFrame()
        if response.status_code >= 400:
            raise ProviderError(
                self.name, None, f"CESMD records HTTP {response.status_code}: {response.text[:200]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ProviderError(self.name, exc, "CESMD yaniti JSON degil") from exc
        raw = records_to_frame(payload)
        if raw.empty:
            return raw
        mapped = self.column_mapper.map_columns(raw)
        # Repi ortak semada yok; filtre icin gecici eklenir (indeks korunur).
        mapped["REPI(km)"] = pd.to_numeric(raw["rec_epidist"], errors="coerce")
        filtered = apply_client_filters(mapped, criteria.get("filters") or {})
        filtered = filtered.drop(columns=["REPI(km)"])
        logger.info("CESMD'den %d kayit alindi (%d filtre sonrasi).", len(mapped), len(filtered))
        return filtered.reset_index(drop=True)

    # ──────────────────────────────────────────────────────────────
    # IWaveformDownloader sozlesmesi
    # ──────────────────────────────────────────────────────────────

    def _resolve_email(self, override: str | None = None) -> str:
        email = (override or self.email or os.environ.get(EMAIL_ENV) or "").strip()
        if not email:
            raise ProviderError(
                self.name,
                None,
                "CESMD indirmesi icin kayitli e-posta gerekli: "
                f"{REGISTER_URL} adresinde ucretsiz kayit olup e-postayi "
                f"{EMAIL_ENV} ortam degiskeniyle veya email= argumaniyla verin",
            )
        return email

    @staticmethod
    def dataset_params(filename: str, event_id: str, email: str, **kwargs: Any) -> Dict[str, Any]:
        """Anahtar ve indirme seceneklerinden dataset sorgusu."""
        network, _station, epidist = parse_file_key(filename)
        status = str(kwargs.get("file_status") or "Acc")
        params: Dict[str, Any] = {
            "eventid": event_id,
            "netid": network,
            "rettype": "dataset",
            "download": "R" if status in RAW_STATUSES else "P",
            "groupby": "event",
            "email": email,
            "nodata": 404,
        }
        if epidist is not None:
            params["minepidist"] = round(max(epidist - EPIDIST_TOLERANCE_KM, 0.0), 3)
            params["maxepidist"] = round(epidist + EPIDIST_TOLERANCE_KM, 3)
        return params

    @result_decorator
    def download_single_waveforms(self, filename: str, **kwargs: Any) -> bool:
        """Bir kaydin ZIP'ini indirir ve acar.

        ``filename`` aramadaki ``FILE_NAME_H1`` degeridir (``AG.ISTASYON~REPI``);
        ``event_id`` CESMD/USGS deprem kimligidir. Ayni Repi penceresine birden
        fazla istasyon duserse yalniz istasyon kodunu iceren dosyalar saklanir.
        """
        event_id = kwargs.pop("event_id", None)
        if not event_id:
            raise ProviderError(self.name, None, "CESMD indirmesi icin event_id gerekli")
        email = self._resolve_email(kwargs.get("email"))
        params = self.dataset_params(filename, str(event_id), email, **kwargs)
        try:
            response = self.session.get(
                f"{self.base_url}{RECORDS_PATH}", params=params, timeout=max(self.timeout, 180)
            )
        except requests.RequestException as exc:
            raise ProviderError(self.name, exc, f"CESMD indirme hatasi: {exc}") from exc
        if response.status_code in (204, 404):
            raise ProviderError(self.name, None, f"CESMD'de {filename} icin dalga formu yok")
        if response.status_code >= 400:
            raise ProviderError(
                self.name, None, f"CESMD dataset HTTP {response.status_code}: {response.text[:200]}"
            )
        if response.content[:4] != b"PK\x03\x04":
            raise ProviderError(self.name, None, f"CESMD ZIP dondurmedi: {response.text[:200]}")
        _network, station, _epi = parse_file_key(filename)
        safe_event = re.sub(r"[^A-Za-z0-9_-]", "_", str(event_id))
        safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", filename.partition("~")[0])
        # Canli dogrulandi: dis ZIP metadata.json (e-posta iceren istek adresi dahil) ve
        # icinde asil kayitlari tasiyan ikinci bir ZIP verir; "asc2" ic ZIP'i de acar.
        extracted = self.file_manager.store_download(
            response.content, safe_event, f"cesmd_{safe_event}_{safe_name}.zip", "asc2"
        )
        extracted = self._keep_acceleration_files(extracted)
        extracted = self._keep_station_files(extracted, station)
        if not extracted:
            raise ProviderError(self.name, None, f"No files extracted for {filename}")
        return True

    @staticmethod
    def _is_acceleration_file(path: str) -> bool:
        """Ivme kaydi mi? Hiz/yer degistirme (``.vel``/``.dis``), spektrum (V3) ve
        metadata.json atilir. Eski CSMIP/NSMP ``.v1``/``.v2`` dosyalari ivmeyi icerir."""
        name = os.path.basename(path).lower()
        if name == "metadata.json" or name.endswith((".json", ".zip")):
            return False
        if re.search(r"\.v3c?$", name):
            return False
        return not re.search(r"\.(vel|dis)\.v\d", name)

    @classmethod
    def _keep_acceleration_files(cls, paths: List[str]) -> List[str]:
        """Ivme disindaki dosyalari diskten siler (metadata.json istek adresinde e-posta tasir)."""
        kept = []
        for path in paths:
            if cls._is_acceleration_file(path):
                kept.append(path)
            elif os.path.isfile(path):
                os.remove(path)
        return kept

    @staticmethod
    def _keep_station_files(paths: List[str], station: str) -> List[str]:
        """Birden fazla istasyon ayni pencereye dustuyse yalniz ilgili dosyalari tutar."""
        matching = [p for p in paths if station.lower() in os.path.basename(p).lower()]
        if not matching or len(matching) == len(paths):
            return list(paths)
        for path in paths:
            if path not in matching and os.path.isfile(path):
                os.remove(path)
        return matching

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
            raise ProviderError(self.name, None, "; ".join(failures) or "CESMD indirmesi basarisiz")
        return {"total": len(filenames), "downloaded": downloaded, "failures": failures}
