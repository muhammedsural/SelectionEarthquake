"""
services/WaveformDownloadService.py

EarthquakeAPI'den çıkarılan dalga formu indirme sorumluluğu.

Sorumluluk:
  - Sonuç DataFrame'ini provider bazında grupla.
  - Her grup için supports_download() kontrolü yap.
  - Toplu (batch) veya tekil indirme başlat.

Bu sınıfın hiçbir sorgulama / strateji bilgisi yoktur.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from ..core.ErrorHandle import ProviderError
from ..processing.ResultHandle import Result
from ..providers.interfaces import supports_download
from ..services.ProviderRegistry import ProviderRegistry

logger = logging.getLogger(__name__)


class WaveformDownloadService:
    """Dalga formu indirme servisi.

    Bağımlılıklar dışarıdan enjekte edilir; test ortamında
    sahte (fake) registry kullanılabilir.

    Kullanım:
        registry = ProviderRegistry.build([ProviderName.AFAD])
        downloader = WaveformDownloadService(registry)

        result = downloader.download_batch(result_df, export_type="mseed")
        result = downloader.download_single("file.mseed", "12345", "AFAD.TK.KND")
    """

    def __init__(self, registry: ProviderRegistry) -> None:
        self._registry = registry

    # ── Toplu indirme ─────────────────────────────────────────────

    def download_batch(
        self,
        result_df: pd.DataFrame,
        **kwargs: Any,
    ) -> Result[bool, ProviderError]:
        """Sonuç DataFrame'indeki tüm kayıtları toplu indir.

        DataFrame'i PROVIDER sütununa göre gruplar; her grup için
        ilgili provider'ın download_waveforms_batch metodunu çağırır.
        Download desteklemeyen provider'lar (PEER vb.) atlanır.

        Args:
            result_df: run_sync / run_async çıktısındaki selected_df.
            **kwargs:  Provider'a iletilen parametreler
                       (export_type, file_status, batch_size, …).

        Returns:
            Result.ok(True)  — en az bir batch başarıyla tamamlandı.
            Result.fail(e)   — tüm provider'lar başarısız oldu veya
                               beklenmedik bir hata oluştu.
        """
        if "PROVIDER" not in result_df.columns:
            return Result.fail(
                ProviderError(
                    "DownloadService",
                    ValueError("result_df 'PROVIDER' sütunu içermiyor."),
                    "Download failed",
                )
            )

        try:
            attempted = 0
            successful = 0
            failures: list[str] = []

            for provider_name, group in result_df.groupby("PROVIDER"):
                provider = self._registry.get(str(provider_name))

                if provider is None:
                    logger.warning(
                        "Provider '%s' registry'de yok, atlanıyor.", provider_name
                    )
                    continue

                if not supports_download(provider):
                    logger.info(
                        "%s download desteklemiyor, atlanıyor.", provider_name
                    )
                    continue

                filenames = group["FILE_NAME_H1"].tolist()
                event_ids = (
                    group["EVENT"].tolist() if "EVENT" in group.columns else None
                )

                logger.info(
                    "%s için %d dosya toplu indirme başlıyor.",
                    provider_name, len(filenames),
                )
                attempted += 1
                provider_result = provider.download_waveforms_batch(
                    filenames=filenames,
                    event_ids=event_ids,
                    **kwargs,
                )
                if self._provider_result_success(provider_result):
                    successful += 1
                else:
                    failures.append(f"{provider_name}: {provider_result}")

            if attempted > 0 and successful == 0:
                return Result.fail(
                    ProviderError(
                        "DownloadService",
                        RuntimeError("; ".join(failures) or "No provider download succeeded"),
                        "Bulk download failed",
                    )
                )
            return Result.ok(True)

        except Exception as e:
            return Result.fail(ProviderError("DownloadService", e, "Bulk download failed"))

    # ── Tekil indirme ─────────────────────────────────────────────

    def download_single(
        self,
        filename: str,
        event_id: str,
        station_code: str,
        **kwargs: Any,
    ) -> Result[bool, ProviderError]:
        """Tek bir dalga formu dosyasını indir.

        station_code'un ilk segmentinden (nokta öncesi) provider adı çıkarılır.
        Örnek: "AFAD.TK.KND" → provider adı "AFAD"

        Args:
            filename:     İndirilecek dosyanın adı.
            event_id:     İlgili deprem olayının ID'si.
            station_code: Noktalı istasyon kodu (ör. "AFAD.TK.KND").
            **kwargs:     Provider'a iletilen ek parametreler.

        Returns:
            Result.ok(True)  — dosya başarıyla indirildi.
            Result.fail(e)   — provider bulunamadı, download desteklenmiyor
                               veya ağ / dosya sistemi hatası.
        """
        provider_name = station_code.split(".")[0]
        provider = self._registry.get(provider_name)

        if provider is None:
            return Result.fail(
                ProviderError(
                    "DownloadService",
                    ValueError(f"Provider '{provider_name}' bulunamadı (station: {station_code})."),
                    "Download failed",
                )
            )

        if not supports_download(provider):
            return Result.fail(
                ProviderError(
                    provider_name,
                    NotImplementedError(
                        f"{provider_name} dalga formu indirmeyi desteklemiyor."
                    ),
                    "Download not supported",
                )
            )

        try:
            provider_result = provider.download_single_waveforms(
                filename=filename,
                event_id=event_id,
                station_code=station_code,
                **kwargs,
            )
            if not self._provider_result_success(provider_result):
                return Result.fail(
                    ProviderError(provider_name, RuntimeError(str(provider_result)), "Single waveform download failed")
                )
            return Result.ok(True)

        except Exception as e:
            return Result.fail(
                ProviderError(provider_name, e, "Single waveform download failed")
            )

    def _provider_result_success(self, provider_result: Any) -> bool:
        if isinstance(provider_result, Result):
            if not provider_result.success:
                return False
            provider_result = provider_result.value

        if isinstance(provider_result, dict):
            downloaded = int(provider_result.get("downloaded", 0) or 0)
            total = int(provider_result.get("total", 0) or 0)
            failed_batches = [
                batch
                for batch in provider_result.get("batches", [])
                if batch.get("success") is False and not batch.get("retry_recovered")
            ]
            return downloaded > 0 and (total == 0 or downloaded >= total or not failed_batches)

        return provider_result is not False
