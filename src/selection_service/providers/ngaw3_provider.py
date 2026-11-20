"""NGA-West3 (nga-gm.org) veri saglayicisi.

NGA-West3 kuvvetli yer hareketi veritabani (https://nga-gm.org) kayit meta verisini ve
ivme zaman serilerini bir REST API ile sunar. Kullanilan uc noktalar:

- ``GET /users/login`` (Basic Auth): 24 saat gecerli Bearer token. Hesap gerekir
  (ucretsiz kayit: https://nga-gm.org). Kullanici adi/sifre ``username=`` ve
  ``password=`` argumanlariyla veya ``NGAGM_USERNAME`` / ``NGAGM_PASSWORD`` ortam
  degiskenleriyle verilir; sifre diske yazilmaz ve cache anahtarina girmez.
- ``GET /flatfile``: olay + istasyon + saha + yol + RotD50 siddet olculeri tek tabloda
  (``where``, ``select``, ``limit``, ``page``).
- ``POST /unscaled-motions`` (``format=json``): ivme zaman serileri (g). JSON olarak
  cekilen zaman serileri dokumana gore aylik 500 hareketlik ZIP kotasina sayilmaz.

Lisans (https://nga-gm.org/pages/license): veri her kullanicinin kendi hesabiyla
cekilir, kutuphane NGA-West3 verisini paketlemez ve yeniden dagitmaz. Kullanim icin
Buckreis vd. (2023), DOI 10.34948/G4RP4K atfi onerilir; turev urunler verinin NGA
projesince onaylanmadigini belirtmelidir. Indirme limitlerini asmak icin birden fazla
hesap acmak yasaktir.

Not: alan adlari ``https://nga-gm.org/schema`` sayfasindan alinmistir; canli hesapla
dogrulama icin ``docs/ngaw3.md`` dosyasina bakin.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Dict, List

import numpy as np
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

NGAW3_BASE_URL = "https://nga-gm.org"
USERNAME_ENV = "NGAGM_USERNAME"
PASSWORD_ENV = "NGAGM_PASSWORD"
FILE_KEY_PREFIX = "NGAW3_MID"
PAGE_SIZE = 1000
DEFAULT_MAX_ROWS = 20000
TOKEN_TTL_S = 23 * 3600  # token 24 saat gecerli; erken yenilenir

# /flatfile yanitinda istenen alanlar (schema sayfasindan; birincil anahtarlar sunucu tarafindan eklenir).
SELECT_FIELDS = (
    "motion_id,event_id,comcat_id,datetime,magnitude,magnitude_type,hypocenter_latitude,"
    "hypocenter_longitude,hypocenter_depth,rake,mechanism_based_on_Rake,network_code,station_code,"
    "station_name,station_latitude,station_longitude,vs30,rjb,rrup,epicentral_distance,"
    "pga_rotd50,pgv_rotd50,hpass_fc_h1,hpass_fc_h2"
)


def make_file_key(motion_id: Any) -> str:
    """Arama satirindaki indirme anahtari: ``NGAW3_MID<motion_id>``."""
    return f"{FILE_KEY_PREFIX}{int(float(motion_id))}"


def parse_file_key(filename: str) -> int:
    """``NGAW3_MID123`` anahtarindan motion_id'yi ayirir."""
    match = re.fullmatch(rf"{FILE_KEY_PREFIX}(\d+)", str(filename).strip())
    if not match:
        raise ProviderError(ProviderName.NGAW3.value, None, f"Gecersiz NGA-West3 kayit adi: {filename}")
    return int(match.group(1))


def payload_to_frame(payload: Any) -> pd.DataFrame:
    """Flatfile yanitini (sutun bazli sozluk veya satir listesi) DataFrame'e cevirir.

    Sunucu hatalari HTTP 200 ile *metin* olarak doner (dokuman: "Limits and Errors").
    """
    if isinstance(payload, str):
        raise ProviderError(ProviderName.NGAW3.value, None, f"NGA-West3 hatasi: {payload[:300]}")
    if payload in (None, [], {}):
        return pd.DataFrame()
    if isinstance(payload, dict):
        if payload.get("success") is False:
            raise ProviderError(
                ProviderName.NGAW3.value, None, f"NGA-West3 hatasi: {str(payload.get('errorMessage'))[:300]}"
            )
        return pd.DataFrame(payload)
    return pd.DataFrame(payload)


def at2_text(title: str, accel_g: np.ndarray, dt: float) -> str:
    """Ivme dizisini (g) NGA-West3 / PEER AT2 bicimine yazar (satirda 5 deger)."""
    values = np.asarray(accel_g, dtype=float)
    lines = [
        "NGA-WEST3 GROUND MOTION DATABASE RECORD (downloaded with earthquake-selection)",
        title,
        "ACCELERATION TIME SERIES IN UNITS OF G",
        f"NPTS= {len(values):6d}, DT= {dt:8.4f} SEC",
    ]
    for start in range(0, len(values), 5):
        lines.append("".join(f"{value:15.7E}" for value in values[start:start + 5]))
    return "\n".join(lines) + "\n"


class NGAW3Provider(IDataFetcher, IWaveformDownloader):
    """NGA-West3 flatfile aramasi ve JSON ivme zaman serisi indirme."""

    def __init__(
        self,
        column_mapper: IColumnMapper,
        timeout: int = 120,
        base_url: str = NGAW3_BASE_URL,
        session: Any | None = None,
        file_manager: AfadFileManager | None = None,
        username: str | None = None,
        password: str | None = None,
        max_rows: int = DEFAULT_MAX_ROWS,
        **_: Any,
    ) -> None:
        self.name = ProviderName.NGAW3.value
        self.column_mapper = column_mapper
        self.timeout = timeout
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.file_manager = file_manager or AfadFileManager(base_dir="NGAW3_events")
        self._username = username
        self._password = password
        self.max_rows = max_rows
        self._token: str | None = None
        self._token_time = 0.0

    # ──────────────────────────────────────────────────────────────
    # Kimlik dogrulama
    # ──────────────────────────────────────────────────────────────

    def _credentials(self) -> tuple[str, str]:
        user = (self._username or os.environ.get(USERNAME_ENV) or "").strip()
        password = self._password or os.environ.get(PASSWORD_ENV) or ""
        if not user or not password:
            raise ProviderError(
                self.name,
                None,
                "NGA-West3 icin nga-gm.org hesabi gerekli: ucretsiz kayit olup kullanici adi ve sifreyi "
                f"username=/password= argumanlariyla veya {USERNAME_ENV}/{PASSWORD_ENV} ortam "
                "degiskenleriyle verin",
            )
        return user, password

    def _login(self) -> str:
        user, password = self._credentials()
        try:
            response = self.session.get(
                f"{self.base_url}/users/login",
                headers={"Accept": "application/json"},
                auth=(user, password),
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise ProviderError(self.name, exc, f"NGA-West3 baglanti hatasi: {exc}") from exc
        if response.status_code in (401, 403):
            raise ProviderError(self.name, None, "NGA-West3 girisi reddedildi: kullanici adi veya sifre hatali")
        if response.status_code >= 400:
            raise ProviderError(self.name, None, f"NGA-West3 giris HTTP {response.status_code}")
        try:
            token = response.json()["token"]
        except (ValueError, KeyError, TypeError) as exc:
            raise ProviderError(self.name, exc, "NGA-West3 giris yaniti token icermiyor") from exc
        self._token, self._token_time = str(token), time.monotonic()
        return self._token

    def _headers(self) -> Dict[str, str]:
        if self._token is None or time.monotonic() - self._token_time > TOKEN_TTL_S:
            self._login()
        return {"Accept": "application/json", "Authorization": f"Bearer {self._token}"}

    # ──────────────────────────────────────────────────────────────
    # IDataFetcher sozlesmesi
    # ──────────────────────────────────────────────────────────────

    def get_name(self) -> str:
        return str(self.name)

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        params = criteria.to_ngaw3_params()
        params["dates"] = criteria.ngaw3_date_range()
        return {"query": params, "filters": criteria.esm_client_filters()}

    async def fetch_data_async(self, criteria: Dict[str, Any]):
        import asyncio

        return await asyncio.to_thread(self.fetch_data_sync, criteria)

    def _get_page(self, where: str, page: int) -> pd.DataFrame:
        params = {"select": SELECT_FIELDS, "where": where, "limit": PAGE_SIZE, "page": page}
        try:
            response = self.session.get(
                f"{self.base_url}/flatfile", params=params, headers=self._headers(), timeout=self.timeout
            )
        except requests.RequestException as exc:
            raise ProviderError(self.name, exc, f"NGA-West3 baglanti hatasi: {exc}") from exc
        if response.status_code == 401:
            self._token = None  # suresi dolmus token: bir sonraki istekte yeniden giris
            raise ProviderError(self.name, None, "NGA-West3 oturumu gecersiz (HTTP 401)")
        if response.status_code >= 400:
            raise ProviderError(
                self.name, None, f"NGA-West3 flatfile HTTP {response.status_code}: {response.text[:200]}"
            )
        try:
            return payload_to_frame(response.json())
        except ValueError as exc:
            raise ProviderError(self.name, exc, "NGA-West3 yaniti JSON degil") from exc

    @result_decorator
    def fetch_data_sync(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        query = dict(criteria.get("query") or {})
        where = query.get("where") or "public_time_series=1"
        frames: List[pd.DataFrame] = []
        total, page = 0, 1
        while total < self.max_rows:
            frame = self._get_page(where, page)
            if frame.empty:
                break
            frames.append(frame)
            total += len(frame)
            if len(frame) < PAGE_SIZE:
                break
            page += 1
        if not frames:
            return pd.DataFrame()
        if total >= self.max_rows:
            logger.warning("NGA-West3 sonuclari %d satirda kesildi; kriterleri daraltin.", self.max_rows)
        raw = pd.concat(frames, ignore_index=True).drop_duplicates(subset="motion_id")
        mapped = self.column_mapper.map_columns(raw)
        mapped["REPI(km)"] = pd.to_numeric(raw.get("epicentral_distance"), errors="coerce")
        start, end = query.get("dates") or (None, None)
        if start or end:
            # Sunucu tarihi saat dilimli ("...Z") veya dilimsiz verebilir; ikisi de UTC'ye cevrilir.
            when = pd.to_datetime(raw.get("datetime"), errors="coerce", utc=True, format="mixed")
            keep = pd.Series(True, index=raw.index)
            if start:
                keep &= (when >= pd.Timestamp(start, tz="UTC")) | when.isna()
            if end:
                keep &= (when < pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)) | when.isna()
            mapped = mapped[keep.to_numpy()]
        filtered = apply_client_filters(mapped, criteria.get("filters") or {})
        filtered = filtered.drop(columns=["REPI(km)"])
        logger.info("NGA-West3'ten %d kayit alindi (%d filtre sonrasi).", len(raw), len(filtered))
        return filtered.reset_index(drop=True)

    # ──────────────────────────────────────────────────────────────
    # IWaveformDownloader sozlesmesi
    # ──────────────────────────────────────────────────────────────

    @result_decorator
    def download_single_waveforms(self, filename: str, **kwargs: Any) -> bool:
        """Bir hareketin ivme zaman serilerini JSON olarak ceker, AT2 dosyalarina yazar.

        ``filename`` aramadaki ``FILE_NAME_H1`` degeridir (``NGAW3_MID<motion_id>``).
        Dosyalar ``NGAwest3_MID<id>_<azimut>.AT2`` (yatay) ve ``..._UP.AT2`` (dusey) adiyla
        olay klasorune yazilir.
        """
        motion_id = parse_file_key(filename)
        folder_id = re.sub(r"[^A-Za-z0-9_-]", "_", str(kwargs.get("event_id") or f"motion_{motion_id}"))
        body = {"checked_motion_ids": [motion_id], "format": "json", "component": "acc_h1,acc_h2,acc_v"}
        try:
            response = self.session.post(
                f"{self.base_url}/unscaled-motions", json=body, headers=self._headers(), timeout=self.timeout
            )
        except requests.RequestException as exc:
            raise ProviderError(self.name, exc, f"NGA-West3 indirme hatasi: {exc}") from exc
        if response.status_code == 401:
            self._token = None
        try:
            payload = response.json()
        except ValueError as exc:
            raise ProviderError(self.name, exc, f"NGA-West3 indirme yaniti JSON degil (HTTP {response.status_code})") from exc
        if response.status_code >= 400 or not isinstance(payload, dict) or payload.get("success") is False:
            message = payload.get("errorMessage") if isinstance(payload, dict) else payload
            raise ProviderError(self.name, None, f"NGA-West3 indirme HTTP {response.status_code}: {str(message)[:200]}")
        series = [item for item in payload.get("output") or [] if isinstance(item, dict)]
        if not series:
            raise ProviderError(self.name, None, f"NGA-West3'te {filename} icin zaman serisi yok")
        folder = self.file_manager.ensure_event_dir(folder_id)
        written = 0
        for item in series:
            rate = float(item.get("sampling_rate") or 0.0)
            if rate <= 0:
                continue
            dt = 1.0 / rate
            for key, label in (("acc_h1", item.get("azimuth_h1")), ("acc_h2", item.get("azimuth_h2")), ("acc_v", "UP")):
                values = item.get(key)
                if not values or label is None:
                    continue
                tag = label if label == "UP" else f"{int(round(float(label))) % 360:03d}"
                name = f"NGAwest3_MID{motion_id}_{tag}.AT2"
                title = f"motion_id= {motion_id}, {key}, {name}"
                with open(os.path.join(folder, name), "w", encoding="utf-8") as handle:
                    handle.write(at2_text(title, np.asarray(values, dtype=float), dt))
                written += 1
        if not written:
            raise ProviderError(self.name, None, f"No files written for {filename}")
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
            raise ProviderError(self.name, None, "; ".join(failures) or "NGA-West3 indirmesi basarisiz")
        return {"total": len(filenames), "downloaded": downloaded, "failures": failures}
