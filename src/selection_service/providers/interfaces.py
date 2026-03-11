"""
providers/interfaces.py

IDataProvider tek monolitik interface'i, sorumluluk bazlı iki ayrı
Protocol'e bölünmüştür:

  IDataFetcher        — veri çekme & kriterleri dönüştürme
  IWaveformDownloader — dalga formu indirme

Bu ayrım iki SOLID ihlalini düzeltir:
  - ISP (Interface Segregation): PEER gibi download desteklemeyen
    provider'lar artık download metodlarını implement etmek zorunda değil.
  - LSP (Liskov Substitution): IDataFetcher bekleyen kod, download
    fırlatmayan PEER ile güvenle çalışır.

Geriye dönük uyumluluk:
  IDataProvider alias'ı korunmuştur; mevcut import'lar kırılmadan çalışır.
  İlerleyen sürümlerde kaldırılabilir.
"""

from __future__ import annotations

from typing import Any, Dict, List, Protocol, runtime_checkable

import pandas as pd

from ..core.ErrorHandle import ProviderError
from ..processing.ResultHandle import Result
from ..processing.Selection import SearchCriteria


# ──────────────────────────────────────────────────────────────────────────────
# 1. VERI ÇEKME INTERFACE'İ
# ──────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class IDataFetcher(Protocol):
    """
    Deprem kayıtlarını bir veri kaynağından çeken provider sözleşmesi.

    Tüm provider'lar (AFAD, PEER, FDSN, …) bu interface'i uygular.
    Download desteği olan provider'lar ek olarak IWaveformDownloader'ı da uygular.

    Protokol notları:
      - map_criteria  : SearchCriteria → provider'a özgü Dict dönüşümü.
      - fetch_*       : Dönüş tipi her zaman Result[DataFrame, ProviderError].
                        Başarısız durumda Result.fail() ile sarmalanmış hata dönülür,
                        exception fırlatılmaz.
    """

    def get_name(self) -> str:
        """Provider'ın benzersiz adı (ör. 'AFAD', 'PEER').

        Bu ad cache anahtarı ve log mesajlarında kullanılır;
        büyük/küçük harf tutarlılığına dikkat edin.
        """
        ...

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        """Ortak SearchCriteria modelini provider'a özgü parametrelere dönüştür.

        Args:
            criteria: Kullanıcının girdiği arama kriterleri.

        Returns:
            Provider API'sinin beklediği parametre dict'i.
        """
        ...

    async def fetch_data_async(
        self, criteria: Dict[str, Any]
    ) -> Result[pd.DataFrame, ProviderError]:
        """Veriyi asenkron olarak çek.

        Args:
            criteria: map_criteria() çıktısı (provider'a özgü dict).

        Returns:
            Result.ok(df)   — başarı; en az bir kayıt içeren DataFrame.
            Result.fail(e)  — hata; ProviderError ile sarmalanmış.
        """
        ...

    def fetch_data_sync(
        self, criteria: Dict[str, Any]
    ) -> Result[pd.DataFrame, ProviderError]:
        """Veriyi senkron olarak çek.

        Args:
            criteria: map_criteria() çıktısı (provider'a özgü dict).

        Returns:
            Result.ok(df)   — başarı; en az bir kayıt içeren DataFrame.
            Result.fail(e)  — hata; ProviderError ile sarmalanmış.
        """
        ...


# ──────────────────────────────────────────────────────────────────────────────
# 2. DALGA FORMU İNDİRME INTERFACE'İ
# ──────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class IWaveformDownloader(Protocol):
    """
    Dalga formu dosyalarını indiren provider sözleşmesi.

    Yalnızca download desteği olan provider'lar (şu an: AFAD) bu interface'i
    uygular. PEER ve FDSN bu interface'i uygulamak zorunda değildir.

    Tasarım kararı:
      - Her iki metot da Result[bool, ProviderError] döndürür;
        başarıda Result.ok(True), hata durumunda Result.fail(e).
      - Metotlar @result_decorator ile sarmalanabilir (bkz. AfadProvider).
    """

    def download_single_waveforms(
        self,
        filename: str,
        **kwargs: Any,
    ) -> Result[bool, ProviderError]:
        """Tek bir dalga formu dosyasını indir ve diske kaydet.

        Args:
            filename: İndirilecek dosyanın adı (ör. 'waveform_12345.mseed').
            **kwargs:
                event_id      (int,  optional): Klasör yapısı için deprem ID'si.
                station_code  (str,  optional): Dosya adı için istasyon kodu.
                file_type     (str,  optional): 'ap' (default).
                file_status   (str,  optional): 'Acc' (default).
                    Seçenekler: RawAcc | Acc | Vel | Disp |
                                ResSpecAcc | ResSpecVel | ResSpecDisp | FFT | Husid
                export_type   (str,  optional): 'mseed' (default).
                    Seçenekler: asc2 | mseed | asd
                user_name     (str,  optional): 'GuestUser' (default).

        Returns:
            Result.ok(True)  — dosya başarıyla indirildi ve çıkarıldı.
            Result.fail(e)   — ağ, dosya sistemi veya zip hatası.
        """
        ...

    def download_waveforms_batch(
        self,
        filenames: List[str],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Birden fazla dalga formu dosyasını toplu indir.

        Dosyalar batch_size'a göre gruplara ayrılır; her grup tek HTTP
        isteğiyle indirilir, ZIP açılır ve diske kaydedilir.

        Args:
            filenames: İndirilecek dosya adları listesi.
            **kwargs:
                batch_size    (int,  optional): Batch başına max dosya sayısı. ≤10, default 10.
                event_id      (int | List[int], optional): Deprem ID veya ID listesi.
                file_type     (str,  optional): 'ap' (default).
                file_status   (str,  optional): 'Acc' (default).
                export_type   (str,  optional): 'mseed' (default).
                user_name     (str,  optional): 'GuestUser' (default).

        Returns:
            {
              'total'     : int,           # istenen dosya sayısı
              'downloaded': int,           # başarıyla indirilen dosya sayısı
              'batches'   : List[dict],    # her batch'in detayı
            }
        """
        ...


# ──────────────────────────────────────────────────────────────────────────────
# 3. GERİYE DÖNÜK UYUMLULUK ALIAS'I
# ──────────────────────────────────────────────────────────────────────────────

# Mevcut import'lar (from ..providers.IProvider import IDataProvider) kırılmasın diye.
# Yeni kod IDataFetcher kullanmalıdır.
# TODO: v2.0'da bu alias kaldırılacak.
IDataProvider = IDataFetcher


# ──────────────────────────────────────────────────────────────────────────────
# 4. TYPE GUARD YARDIMCISI
# ──────────────────────────────────────────────────────────────────────────────

def supports_download(provider: IDataFetcher) -> bool:
    """Provider'ın dalga formu indirmeyi destekleyip desteklemediğini kontrol et.

    Kullanım örneği:
        if supports_download(provider):
            provider.download_waveforms_batch(filenames)
        else:
            logger.info(f"{provider.get_name()} download desteklemiyor, atlanıyor.")

    Args:
        provider: Kontrol edilecek IDataFetcher örneği.

    Returns:
        True  — provider aynı zamanda IWaveformDownloader protokolünü karşılıyor.
        False — provider yalnızca veri çekmeyi destekliyor.
    """
    return isinstance(provider, IWaveformDownloader)