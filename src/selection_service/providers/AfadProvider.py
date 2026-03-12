"""
providers/AfadProvider.py  (refactored)

Değişiklikler (Adım 1 — ISP + LSP):
  - IDataProvider  → IDataFetcher + IWaveformDownloader
      AFAD hem veri çekmeyi hem de download'u destekler,
      dolayısıyla her iki interface'i de karşılar.
  - EarthquakeApi.download_waveforms içindeki PEER kontrolü artık
      supports_download() fonksiyonu ile yapılabilir (geriye uyumluluk için
      mevcut ProviderName.PEER kontrolü de hâlâ çalışır).
"""

import logging
import os
import time
from typing import Any, Dict, List, Type

import pandas as pd

from ..core.ErrorHandle import AfadEmptyResultError, AfadMappingError, ProviderError
from ..enums.Enums import ProviderName
from ..processing.Mappers import IColumnMapper
from ..processing.ResultHandle import Result, async_result_decorator, result_decorator
from ..processing.Selection import SearchCriteria
from ..providers.afad.AfadApiClient import AfadApiClient
from ..providers.afad.AfadFileManager import AfadFileManager
from ..providers.interfaces import IDataFetcher, IWaveformDownloader  # ← yeni


logger = logging.getLogger(__name__)


class AFADDataProvider(IDataFetcher, IWaveformDownloader):
    """AFAD (Tadas) veri sağlayıcı.

    IDataFetcher   → fetch_data_async / fetch_data_sync / map_criteria / get_name
    IWaveformDownloader → download_single_waveforms / download_waveforms_batch
    """

    def __init__(
        self,
        column_mapper: Type[IColumnMapper],
        timeout: int = 30,
        api_client: AfadApiClient | None = None,
        file_manager: AfadFileManager | None = None,
    ) -> None:
        self.name = ProviderName.AFAD.value
        self.column_mapper = column_mapper
        self.timeout = timeout
 
        # DIP: Dışarıdan injection desteklenir; verilmezse default oluşturulur.
        self.api_client = api_client or AfadApiClient(timeout=self.timeout)
        self.file_manager = file_manager or AfadFileManager(base_dir="Afad_events")
 
        self.mapped_df: pd.DataFrame | None = None
        self.response_df: pd.DataFrame | None = None

    # ──────────────────────────────────────────────────────────────
    # IDataFetcher sözleşmesi
    # ──────────────────────────────────────────────────────────────

    def get_name(self) -> str:
        return str(self.name)

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        return criteria.to_afad_params()

    @async_result_decorator
    async def fetch_data_async(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """Asenkron veri çekme.

        AfadEmptyResultError ve AfadMappingError doğrudan yeniden fırlatılır;
        diğer hatalar ProviderError'a sarılır.
        """
        try:
            data = await self.api_client.search_waveforms_async(criteria)
            return self._process_response_data(data)
        except (AfadEmptyResultError, AfadMappingError):
            raise
        except ProviderError:
            raise
        except Exception as e:
            logger.error("AFAD asenkron veri çekme sırasında beklenmedik hata: %s", e, exc_info=True)
            raise ProviderError(
                self.name, e, f"AFAD bağlantı/veri hatası: {e}"
            ) from e

    @result_decorator
    def fetch_data_sync(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """Senkron veri çekme.

        AfadEmptyResultError ve AfadMappingError doğrudan yeniden fırlatılır;
        bu sayede Pipeline katmanı hata türüne göre farklı mesaj üretebilir.
        Diğer beklenmedik hatalar ProviderError'a sarılır.
        """
        try:
            data = self.api_client.search_waveforms_sync(criteria)
            return self._process_response_data(data)
        except (AfadEmptyResultError, AfadMappingError):
            raise  # Zaten açıklayıcı — Pipeline'a olduğu gibi ilet
        except ProviderError:
            raise
        except Exception as e:
            logger.error("AFAD senkron veri çekme sırasında beklenmedik hata: %s", e, exc_info=True)
            raise ProviderError(
                self.name, e, f"AFAD bağlantı/veri hatası: {e}"
            ) from e

    # ──────────────────────────────────────────────────────────────
    # IWaveformDownloader sözleşmesi
    # ──────────────────────────────────────────────────────────────

    @result_decorator
    def download_single_waveforms(self, filename: str, **kwargs: Any) -> bool:
        """Tekil dalga formu indir, kaydet ve çıkar."""
        payload = self._prepare_download_payload([filename], **kwargs)
        event_id = kwargs.get("event_id", int(time.time()))
        station_code = kwargs.get("station_code", "unknown")

        content = self.api_client.download_waveform(payload)
        zip_name = f"waveforms_{event_id}_{station_code}.zip"
        zip_path = self.file_manager.save_zip(content, event_id, zip_name)
        self.file_manager.extract_zip(zip_path, kwargs.get("export_type", "asc2"))
        return True

    @result_decorator
    def download_waveforms_batch(
        self, filenames: List[str], **kwargs: Any
    ) -> Dict[str, Any]:
        """Toplu indirme: filenames'i batch'lere böler ve her batch'i indirir.

        Args:
            filenames: İndirilecek dosya isimleri.
            **kwargs:
                batch_size  (int):  Batch başına max dosya sayısı (≤10, default 10).
                event_id    (int | List[int]): Klasör yapısı için deprem ID'si.
                export_type (str):  'mseed' | 'asc2' | 'asd'  (default 'mseed').
                file_type   (str):  'ap' (default).
                file_status (str):  'Acc' (default).
                user_name   (str):  'GuestUser' (default).

        Returns:
            {'total': int, 'downloaded': int, 'batches': List[dict]}
        """
        batch_size = min(kwargs.get("batch_size", 10), 10)
        event_id = kwargs.get("event_id", [int(time.time())])
        export_type = kwargs.get("export_type", "mseed")

        results: Dict[str, Any] = {
            "total": len(filenames),
            "downloaded": 0,
            "batches": [],
        }

        batches = [
            filenames[i : i + batch_size]
            for i in range(0, len(filenames), batch_size)
        ]
        logger.info("%d dosya, %d parti halinde indirilecek.", len(filenames), len(batches))

        for idx, batch_files in enumerate(batches, 1):
            try:
                payload = self._prepare_download_payload(batch_files, **kwargs)
                logger.debug("Batch %d payload hazırlandı: %s", idx, payload)

                content = self.api_client.download_waveform(payload)
                logger.debug("Batch %d indirildi, boyut: %d bytes", idx, len(content))

                zip_name = f"part_{idx}.zip"
                zip_path = self.file_manager.save_zip(content, event_id, zip_name)
                logger.debug("Batch %d zip kaydedildi: %s", idx, zip_path)

                extracted = self.file_manager.extract_zip(zip_path, export_type)
                count = len(extracted)
                logger.debug("Batch %d çıkarıldı, %d dosya bulundu.", idx, count)

                results["downloaded"] += count
                results["batches"].append({"batch": idx, "count": count, "success": True})
                logger.info("Batch %d tamamlandı: %d dosya.", idx, count)

                if count < len(batch_files):
                    retry_count = self._handle_retry(
                        batch_files, extracted, event_id, **kwargs
                    )
                    results["downloaded"] += retry_count

                time.sleep(2)

            except Exception as e:
                logger.error("Batch %d başarısız: %s", idx, e, exc_info=True)
                results["batches"].append({"batch": idx, "success": False, "error": str(e)})

        return results

    # ──────────────────────────────────────────────────────────────
    # Ek public metotlar (IDataFetcher / IWaveformDownloader dışı)
    # ──────────────────────────────────────────────────────────────

    @result_decorator
    def get_event_details(self, event_ids: List[int]) -> pd.DataFrame:
        """Belirtilen deprem ID'leri için detay bilgisi çek."""
        all_details = []
        for event_id in event_ids:
            detail = self.api_client.get_event_details(event_id)
            if detail:
                all_details.append(detail[0] if isinstance(detail, list) else detail)
            time.sleep(0.1)
        return pd.DataFrame(all_details)

    # ──────────────────────────────────────────────────────────────
    # Private yardımcılar
    # ──────────────────────────────────────────────────────────────

    def _process_response_data(self, data: List[Dict]) -> pd.DataFrame:
        """API yanıtını standart DataFrame'e çevir (DRY — async + sync ortak).

        Boş yanıt durumlarını sessizce geçmek yerine sebebi açıklayan
        özel hata fırlatır; çağıran katman (Pipeline) bu hataları
        kullanıcıya anlamlı mesaj olarak iletir.

        Raises:
            AfadEmptyResultError: API sıfır kayıt döndürdüğünde.
            AfadMappingError: Mapper sonrası kayıt sayısı sıfıra düştüğünde.
        """
        raw_count = len(data) if data else 0
        logger.debug("AFAD API yanıtı: %d ham kayıt.", raw_count)

        if raw_count == 0:
            logger.warning(
                "AFAD API sıfır kayıt döndürdü. "
                "Arama kriteri (tarih, büyüklük, bölge) kontrol edilmeli."
            )
            raise AfadEmptyResultError(
                "AFAD API'den sonuç dönmedi.",
                criteria_hint="Tarih aralığını genişletin, büyüklük alt sınırını düşürün "
                              "veya bbox/çember arama alanını büyütün.",
            )

        self.response_df = pd.DataFrame(data)

        self.mapped_df = self.column_mapper.map_columns(df=self.response_df)
        self.mapped_df["PROVIDER"] = str(self.name)

        if "RSN" in self.mapped_df.columns:
            self.mapped_df["ENDPOINTSOURCE"] = (
                "https://tadas.afad.gov.tr/waveform-detail/"
                + self.mapped_df["RSN"].astype(str)
            )

        mapped_count = len(self.mapped_df)
        if mapped_count == 0:
            logger.error(
                "AFAD mapper sonrası 0 kayıt kaldı (ham kayıt: %d). "
                "API yanıt şeması değişmiş olabilir.",
                raw_count,
            )
            raise AfadMappingError(raw_count=raw_count)

        logger.info("AFAD'dan %d kayıt alındı (ham: %d).", mapped_count, raw_count)
        return self.mapped_df

    def _prepare_download_payload(
        self, filenames: List[str], **kwargs: Any
    ) -> Dict[str, Any]:
        """AFAD download API payload'unu hazırla."""
        return {
            "filename": filenames,
            "file_type": [kwargs.get("file_type", "ap")] * len(filenames),
            "file_status": kwargs.get("file_status", "Acc"),
            "export_type": kwargs.get("export_type", "mseed"),
            "user_name": kwargs.get("user_name", "GuestUser"),
            "call": "afad",
        }

    def _handle_retry(
        self,
        requested: List[str],
        extracted: List[str],
        event_id: Any,
        **kwargs: Any,
    ) -> int:
        """Eksik dosyalar için tek tek retry dene; başarıyla indirilen sayısını döndür."""
        missing = set(requested) - {os.path.basename(f) for f in extracted}
        if not missing:
            return 0

        logger.warning("Eksik dosyalar, retry başlatılıyor: %s", missing)
        recovered = 0
        for filename in missing:
            try:
                self.download_single_waveforms(filename, event_id=event_id, **kwargs)
                logger.info("Retry başarılı: %s indirildi.", filename)
                recovered += 1
            except Exception as e:
                logger.error("Retry başarısız: %s indirilemedi: %s", filename, e, exc_info=True)
        return recovered