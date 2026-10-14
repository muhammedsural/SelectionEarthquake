"""ESM (Engineering Strong-Motion database) veri saglayicisi.

ESM (https://esm-db.eu, ORFEUS/INGV) Avrupa, Turkiye ve Orta Dogu'daki
kuvvetli yer hareketi kayitlarini islenmis halde sunar. Iki herkese acik web
servisi kullanilir (anahtar gerekmez; yalniz kisitli/ambargolu veri icin
POST + imzali token gerekir, burada desteklenmez):

- ``esmws/flatfile/1/query``: kayit basina bir satir; deprem, istasyon,
  mesafe (Repi, Rhyp, Rjb, Rrup) ve yer hareketi parametreleri (PGA, PGV,
  PGD, T90, Arias). Noktali virgulle ayrilmis CSV doner.
- ``esmws/eventdata/1/query``: bir kaydin dalga formlari (ascii, mseed, sac
  ZIP olarak; islem turu CV/MP/AP, veri turu ACC/VEL/DIS/SA/SD/FAS).

Sunucu tarafinda desteklenen filtreler (tarih, buyukluk, derinlik, deprem
kutusu, Vs30, Rjb, ag/istasyon, faylanma turu) sorguya eklenir; digerleri
(Rrup, Repi, Rhyp, PGA/PGV/PGD, daire) yanit geldikten sonra uygulanir.
Buyukluk sinirlari sunucuda herhangi bir buyukluk turune gore uygulandigi
icin istemci tarafinda secilen buyukluge (oncelik Mw) gore yeniden uygulanir.
"""

from __future__ import annotations

import io
import logging
import math
from typing import Any, Dict, List, Type

import numpy as np
import pandas as pd
import requests

from ..core.error_handle import ProviderError
from ..enums.enums import ProviderName
from ..processing.criteria import SearchCriteria
from ..processing.mappers import IColumnMapper
from ..processing.result_handle import result_decorator
from .afad.afad_file_manager import AfadFileManager
from .interfaces import IDataFetcher, IWaveformDownloader

logger = logging.getLogger(__name__)

ESM_BASE_URL = "https://esm-db.eu"
FLATFILE_PATH = "/esmws/flatfile/1/query"
EVENTDATA_PATH = "/esmws/eventdata/1/query"

# Flatfile'dan istenen alanlar; servis bir alani tanimazsa butun alanlari
# dondurebilir, ayristirma iki durumda da ayni calisir.
FLATFILE_FIELDS = (
    "esm_event_id", "event_time", "ev_latitude", "ev_longitude", "ev_depth_km",
    "fm_type_code", "mw", "emec_mw", "ms", "ml", "mb",
    "es_strike", "es_dip", "es_rake",
    "network_code", "station_code", "location_code", "channel_code",
    "st_latitude", "st_longitude", "preferred_vs30_m_s",
    "epi_dist", "hyp_dist", "jb_dist", "rup_dist",
    "u_hp", "v_hp",
    "rotD50_pga", "u_pga", "v_pga",
    "rotD50_pgv", "u_pgv", "v_pgv",
    "rotD50_pgd", "u_pgd", "v_pgd",
    "u_T90", "v_T90", "u_ia", "v_ia",
)

# Uygulamalarin AFAD'a gore verdigi indirme secenekleri -> ESM eventdata.
EXPORT_FORMATS = {"asc2": "ascii", "ascii": "ascii", "asc": "ascii", "mseed": "mseed", "sac": "sac"}
FILE_STATUS = {
    # file_status: (processing-type, data-type)
    "RawAcc": ("CV", "ACC"),
    "Acc": ("MP", "ACC"),
    "Vel": ("MP", "VEL"),
    "Disp": ("MP", "DIS"),
    "ResSpecAcc": ("MP", "SA"),
    "ResSpecDisp": ("MP", "SD"),
    "FFT": ("MP", "FAS"),
}

# Istemci tarafi aralik filtreleri: (alt sinir, ust sinir, kolon).
CLIENT_RANGES = (
    ("min_magnitude", "max_magnitude", "MAGNITUDE"),
    ("min_depth", "max_depth", "HYPO_DEPTH(km)"),
    ("min_vs30", "max_vs30", "VS30(m/s)"),
    ("min_Rjb", "max_Rjb", "RJB(km)"),
    ("min_Rrup", "max_Rrup", "RRUP(km)"),
    ("min_Repi", "max_Repi", "REPI(km)"),
    ("min_Rhyp", "max_Rhyp", "RHYP(km)"),
    ("min_pga", "max_pga", "PGA(cm2/sec)"),
    ("min_pgv", "max_pgv", "PGV(cm/sec)"),
    ("min_pgd", "max_pgd", "PGD(cm)"),
)


def parse_flatfile(text: str) -> pd.DataFrame:
    """ESM flatfile CSV'sini (noktali virgul, bos alan = eksik) DataFrame'e cevirir."""
    if not text or not text.strip():
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(text), sep=";", dtype=str, keep_default_na=False)
    df.columns = [str(column).strip().lower() for column in df.columns]
    return df.replace({"": None})


class ESMProvider(IDataFetcher, IWaveformDownloader):
    """ESM flatfile aramasi ve eventdata ile dalga formu indirme."""

    def __init__(
        self,
        column_mapper: Type[IColumnMapper],
        timeout: int = 60,
        base_url: str = ESM_BASE_URL,
        session: Any | None = None,
        file_manager: AfadFileManager | None = None,
        **_: Any,
    ) -> None:
        self.name = ProviderName.ESM.value
        self.column_mapper = column_mapper
        self.timeout = timeout
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.file_manager = file_manager or AfadFileManager(base_dir="ESM_events")

    # ──────────────────────────────────────────────────────────────
    # IDataFetcher sozlesmesi
    # ──────────────────────────────────────────────────────────────

    def get_name(self) -> str:
        return str(self.name)

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        return {"query": criteria.to_esm_params(), "filters": criteria.esm_client_filters()}

    async def fetch_data_async(self, criteria: Dict[str, Any]):
        import asyncio

        return await asyncio.to_thread(self.fetch_data_sync, criteria)

    @result_decorator
    def fetch_data_sync(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        query = dict(criteria.get("query") or {})
        query.setdefault("include-fields", ",".join(FLATFILE_FIELDS))
        try:
            response = self.session.get(
                f"{self.base_url}{FLATFILE_PATH}", params=query, timeout=self.timeout
            )
        except requests.RequestException as exc:
            raise ProviderError(self.name, exc, f"ESM baglanti hatasi: {exc}") from exc
        # FDSN benzeri servisler "veri yok" icin 204 (bazen 404) dondurur.
        if response.status_code in (204, 404):
            return pd.DataFrame()
        if response.status_code >= 400:
            raise ProviderError(
                self.name, None, f"ESM flatfile HTTP {response.status_code}: {response.text[:200]}"
            )
        raw = parse_flatfile(response.text)
        if raw.empty:
            return raw
        mapped = self.column_mapper.map_columns(raw)
        # Repi/Rhyp ortak semada yok; filtre icin gecici olarak eklenir (indeks korunur).
        mapped["REPI(km)"] = _number(raw.get("epi_dist"))
        mapped["RHYP(km)"] = _number(raw.get("hyp_dist"))
        filtered = apply_client_filters(mapped, criteria.get("filters") or {})
        filtered = filtered.drop(columns=["REPI(km)", "RHYP(km)"])
        logger.info("ESM'den %d kayit alindi (%d filtre sonrasi).", len(mapped), len(filtered))
        return filtered.reset_index(drop=True)

    # ──────────────────────────────────────────────────────────────
    # IWaveformDownloader sozlesmesi
    # ──────────────────────────────────────────────────────────────

    @result_decorator
    def download_single_waveforms(self, filename: str, **kwargs: Any) -> bool:
        """Bir kaydin dalga formlarini indirir.

        ``filename`` aramadaki ``FILE_NAME_H1`` degeridir: ``AG.ISTASYON.KONUM.KANAL``
        (kanal iki harf, ornegin HN/HG; uc bilesen birlikte iner). ``event_id`` ESM
        deprem kimligidir.
        """
        event_id = kwargs.pop("event_id", None)
        if not event_id:
            raise ProviderError(self.name, None, "ESM indirmesi icin event_id gerekli")
        params = self.eventdata_params(filename, str(event_id), **kwargs)
        try:
            response = self.session.get(
                f"{self.base_url}{EVENTDATA_PATH}", params=params, timeout=max(self.timeout, 120)
            )
        except requests.RequestException as exc:
            raise ProviderError(self.name, exc, f"ESM indirme hatasi: {exc}") from exc
        if response.status_code in (204, 404):
            raise ProviderError(self.name, None, f"ESM'de {filename} icin dalga formu yok")
        if response.status_code >= 400:
            raise ProviderError(
                self.name, None, f"ESM eventdata HTTP {response.status_code}: {response.text[:200]}"
            )
        safe_event = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(event_id))
        safe_name = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in filename)
        extension = "zip" if response.content[:4] == b"PK\x03\x04" else params["format"]
        extracted = self.file_manager.store_download(
            response.content, safe_event, f"esm_{safe_event}_{safe_name}.zip",
            params["format"], raw_filename=f"{safe_name}.{extension}",
        )
        if not extracted:
            raise ProviderError(self.name, None, f"No files extracted for {filename}")
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
            raise ProviderError(self.name, None, "; ".join(failures) or "ESM indirmesi basarisiz")
        return {"total": len(filenames), "downloaded": downloaded, "failures": failures}

    @staticmethod
    def eventdata_params(filename: str, event_id: str, **kwargs: Any) -> Dict[str, Any]:
        """FILE_NAME_H1 ve AFAD tarzi indirme seceneklerinden eventdata sorgusu."""
        parts = (str(filename).split(".") + ["", "", "", ""])[:4]
        network, station, location, channel = (part.strip() for part in parts)
        if not network or not station:
            raise ProviderError(ProviderName.ESM.value, None, f"Gecersiz ESM kayit adi: {filename}")
        export_type = str(kwargs.get("export_type") or "ascii").lower()
        status = str(kwargs.get("file_status") or "Acc")
        processing, data_type = FILE_STATUS.get(status, FILE_STATUS["Acc"])
        return {
            "eventid": event_id,
            "network": network,
            "station": station,
            "location": location or "--",
            "channel": f"{channel[:2]}?" if channel else "*",
            "format": EXPORT_FORMATS.get(export_type, "ascii"),
            "processing-type": kwargs.get("processing_type") or processing,
            "data-type": kwargs.get("data_type") or data_type,
        }


def _number(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(series, errors="coerce")


def apply_client_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    """Sunucunun uygulamadigi (veya baska buyukluk turuyle uyguladigi) sinirlar."""
    if df.empty or not filters:
        return df
    mask = pd.Series(True, index=df.index)
    for min_key, max_key, column in CLIENT_RANGES:
        low, high = filters.get(min_key), filters.get(max_key)
        if (low is None and high is None) or column not in df.columns:
            continue
        values = _number(df[column])
        if low is not None:
            mask &= values >= low
        if high is not None:
            mask &= values <= high
    mechanisms = filters.get("mechanisms") or []
    if mechanisms and "MECHANISM" in df.columns:
        mask &= df["MECHANISM"].isin(mechanisms)
    circle = filters.get("circle")
    if circle:
        lat0, lon0, radius = circle
        lat = np.radians(_number(df["HYPO_LAT"]))
        lon = np.radians(_number(df["HYPO_LON"]))
        a = (np.sin((lat - math.radians(lat0)) / 2) ** 2
             + math.cos(math.radians(lat0)) * np.cos(lat) * np.sin((lon - math.radians(lon0)) / 2) ** 2)
        mask &= 2 * 6371.0088 * np.arcsin(np.sqrt(a)) <= radius
    return df[mask.fillna(False)]
