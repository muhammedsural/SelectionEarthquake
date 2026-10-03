from typing import TypeVar

# Type variables for generic programming
T = TypeVar('T')
E = TypeVar('E', bound=Exception)

# Domain-specific errors


class PipelineError(Exception):
    """Base pipeline error"""
    pass


class ValidationError(PipelineError):
    """Validation failed"""
    pass


class NoDataError(PipelineError):
    """No data found error"""
    pass


class StrategyError(PipelineError):
    """Strategy application error"""
    pass


class ProviderError(PipelineError):
    """Data provider error"""
    def __init__(self, provider_name: str, original_error: Exception,
                 message: str = None):
        self.provider_name = provider_name
        self.original_error = original_error
        self.message = message or f"{provider_name} error: {original_error}"
        super().__init__(self.message)


class NetworkError(ProviderError):
    """Network related errors"""
    pass


class DataProcessingError(ProviderError):
    """Data processing errors"""
    pass


class AfadEmptyResultError(NoDataError):
    """AFAD API'den hiç kayıt dönmedi.

    Olası sebepler:
      - Arama kriterleri çok kısıtlayıcı (tarih aralığı, büyüklük sınırı, bbox)
      - Belirtilen bölgede/tarih aralığında kayıtlı deprem yok
      - API geçici olarak hizmet dışı

    Attributes:
        criteria_hint: Hangi kriterin dar olabileceğine dair ipucu (opsiyonel)
    """

    def __init__(self, message: str, criteria_hint: str | None = None) -> None:
        self.criteria_hint = criteria_hint
        full_msg = message
        if criteria_hint:
            full_msg += f" [Kriter ipucu: {criteria_hint}]"
        super().__init__(full_msg)


class AfadMappingError(DataProcessingError):
    """AFAD verisi geldi ancak kolon eşlemesi sonrası kayıt kalmadı.

    Olası sebepler:
      - API yanıt şeması değişmiş (kolon adları farklı)
      - Mapper yanlış yapılandırılmış
      - Tüm satırlar kritik bir kolonda None/NaN içeriyordu

    Attributes:
        raw_count: Mapper öncesi kayıt sayısı
    """

    def __init__(self, raw_count: int) -> None:
        self.raw_count = raw_count
        super().__init__(
            provider_name="AFAD",
            original_error=None,
            message=(
                f"AFAD: {raw_count} ham kayıt alındı ancak kolon eşlemesi "
                "sonrası hiç kayıt kalmadı. "
                "API yanıt şeması değişmiş veya mapper yanlış yapılandırılmış olabilir."
            ),
        )