"""Izlerin ESM benzeri ASCII dosyalarina yazilmasi.

Her bilesen icin bir dosya yazilir: ``ANAHTAR: deger`` baslik satirlari ve
satir basina bir ornek. Bicim ESM ``ascii`` ciktisiyla ayni anahtarlari
kullanir; boylece ESM dosyalarini okuyan uygulamalar FDSN kayitlarini da
okuyabilir.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np

from .mseed import Trace


def write_ascii(traces: Iterable[Trace], directory: str | Path, prefix: str,
                header: Dict[str, object] | None = None) -> List[Path]:
    """Her izi ``<prefix>_<KANAL>.ASC`` olarak yazar ve yollari dondurur."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    for trace in traces:
        lines = {
            "NETWORK": trace.network,
            "STATION_CODE": trace.station,
            "LOCATION": trace.location,
            "STREAM": trace.channel,
            "DATE_TIME_FIRST_SAMPLE_YYYYMMDD_HHMMSS": trace.starttime.strftime("%Y%m%d_%H%M%S.%f")[:-3],
            "SAMPLING_INTERVAL_S": f"{trace.delta:.6f}",
            "NDATA": len(trace.data),
            "UNITS": trace.units,
            **(header or {}),
            **trace.meta,
        }
        body = "\n".join(f"{value:.6e}" for value in np.asarray(trace.data, dtype=float))
        text = "\n".join(f"{key}: {value}" for key, value in lines.items()) + "\n" + body + "\n"
        path = directory / f"{prefix}_{trace.channel}.ASC"
        path.write_text(text, encoding="utf-8")
        paths.append(path)
    return paths
