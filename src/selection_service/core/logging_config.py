"""
core/LoggingConfig.py  (Adım 3 — LoggingConfig düzeltmesi)

Sorunlar ve düzeltmeler:

  1. devnull hack → KALDIRILDI
     Önceki kod bir StreamHandler oluşturup hemen os.devnull'a yönlendiriyordu.
     Bu hem mantık hatası (handler oluşturma + anında iptal etme) hem de
     kaynak sızıntısıydı (open() ile açılan dosya handle hiçbir zaman kapatılmıyordu).
     Düzeltme: log_to_console=False geçildiğinde StreamHandler hiç oluşturulmaz.

  2. Tekrar çağrı güvenliği → EKLENDİ
     logging.basicConfig() kök logger'a zaten handler varsa sessizce çıkar.
     Bu, modülün import sırasında veya test ortamında birden fazla çağrılmasında
     handler'ların çoğalmasına yol açabilir.
     Düzeltme: Kök logger'ın handler'ları temizlendikten sonra basicConfig çağrılır
     (force=True — Python 3.8+).

  3. Yapılandırma nesnesi → EKLENDİ
     LogConfig dataclass'ı ile tüm parametreler tek bir yerden yönetilir;
     ileride env değişkenlerinden veya YAML/TOML'dan yükleme kolaylaşır.
"""

import logging
import os
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from typing import List


# ──────────────────────────────────────────────────────────────────────────────
# Yapılandırma nesnesi
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class LogConfig:
    """Logging parametrelerini tutan yapılandırma nesnesi.

    Kullanım:
        cfg = LogConfig(log_level=logging.DEBUG, log_to_console=True)
        setup_logging(cfg)

    Ya da kısa yol (varsayılanlar):
        setup_logging()
    """
    log_level    : int  = logging.INFO
    log_dir      : str  = "logs"
    log_filename : str  = "selection.log"
    max_bytes    : int  = 5_000_000   # 5 MB
    backup_count : int  = 3
    log_to_console: bool = False       # True → stdout'a da yaz
    fmt: str = field(
        default="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        repr=False,
    )
    datefmt: str = field(default="%Y-%m-%d %H:%M:%S", repr=False)


# ──────────────────────────────────────────────────────────────────────────────
# Kurulum fonksiyonu
# ──────────────────────────────────────────────────────────────────────────────

def setup_logging(config: "LogConfig" = None) -> None:
    """Uygulama genelinde logging altyapısını kur.

    Args:
        config: LogConfig nesnesi. None geçilirse varsayılan ayarlar kullanılır.

    Davranış:
        - log_to_console=False (varsayılan): yalnızca dönen dosyaya yazar.
        - log_to_console=True : hem dosyaya hem stdout'a yazar.
        - force=True ile tekrar çağrıldığında mevcut handler'lar temizlenir;
          böylece test ortamında handler çoğalması önlenir.

    Örnek:
        # Sadece dosya (varsayılan):
        setup_logging()

        # Konsol + dosya, DEBUG seviyesinde:
        setup_logging(LogConfig(log_level=logging.DEBUG, log_to_console=True))

        # Özel klasör:
        setup_logging(LogConfig(log_dir="/var/log/selection", log_to_console=False))
    """
    cfg = config or LogConfig()

    # Log klasörünü oluştur
    os.makedirs(cfg.log_dir, exist_ok=True)
    log_file = os.path.join(cfg.log_dir, cfg.log_filename)

    formatter = logging.Formatter(fmt=cfg.fmt, datefmt=cfg.datefmt)

    # ── Dönen dosya handler'ı (her zaman aktif) ───────────────────
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=cfg.max_bytes,
        backupCount=cfg.backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    handlers: List[logging.Handler] = [file_handler]

    # ── Konsol handler'ı (isteğe bağlı) ───────────────────────────
    # Önceki kod: StreamHandler oluşturulup hemen devnull'a yönlendiriliyordu.
    # Düzeltme: log_to_console=False ise handler hiç oluşturulmaz.
    if cfg.log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        handlers.append(console_handler)

    # force=True: kök logger'ın mevcut handler'larını temizler,
    # tekrar çağrılmada çoğalmayı önler (Python 3.8+).
    logging.basicConfig(
        level=cfg.log_level,
        handlers=handlers,
        force=True,
    )

# Yeni Kullanım
# setup_logging()  # varsayılanlar
# setup_logging(LogConfig(log_level=logging.DEBUG, log_to_console=True))