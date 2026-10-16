"""miniSEED 2 okuyucu (yalniz numpy).

FDSN dataselect servisleri ham miniSEED dondurur. Bu modul kuvvetli yer
hareketi kayitlarinda kullanilan kodlamalari cozer: 16/32 bit tamsayi,
32/64 bit kayan nokta, Steim1 ve Steim2. Her kanal (AG.ISTASYON.KONUM.KANAL)
icin kayitlar zamana gore birlestirilir; kucuk bosluklar dogrusal
interpolasyonla doldurulur, cakisan ornekler atilir.

Desteklenmeyenler: miniSEED 3, Blockette 1000 olmayan kayitlar, GEOSCOPE ve
SRO gibi eski kodlamalar.
"""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple

import numpy as np

ENCODINGS = {1: "int16", 3: "int32", 4: "float32", 5: "float64", 10: "steim1", 11: "steim2"}


class MSeedError(ValueError):
    """miniSEED verisi cozulemedi."""


@dataclass
class Trace:
    """Tek kanalin surekli ornekleri.

    Tamsayi kodlanmis veri ham sayimdir (``units="counts"``); kayan nokta
    verinin birimi dosyada yazmaz (``"unknown"``).
    """

    network: str
    station: str
    location: str
    channel: str
    starttime: datetime
    sampling_rate: float
    data: np.ndarray
    units: str = "counts"
    meta: Dict[str, object] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return f"{self.network}.{self.station}.{self.location}.{self.channel}"

    @property
    def delta(self) -> float:
        return 1.0 / self.sampling_rate

    @property
    def endtime(self) -> datetime:
        return self.starttime + timedelta(seconds=(len(self.data) - 1) * self.delta)


@dataclass
class _Record:
    key: Tuple[str, str, str, str]
    start: datetime
    rate: float
    data: np.ndarray


def _sample_rate(factor: int, multiplier: int) -> float:
    if factor == 0:
        return 0.0
    if multiplier == 0:
        multiplier = 1
    if factor > 0 and multiplier > 0:
        return float(factor * multiplier)
    if factor > 0 > multiplier:
        return -factor / multiplier
    if factor < 0 < multiplier:
        return -multiplier / factor
    return 1.0 / (factor * multiplier)


def _byte_order(header: bytes) -> str:
    year_big = struct.unpack(">H", header[20:22])[0]
    if 1900 <= year_big <= 2100:
        return ">"
    year_little = struct.unpack("<H", header[20:22])[0]
    if 1900 <= year_little <= 2100:
        return "<"
    raise MSeedError("Kayit basligi okunamadi (yil alani gecersiz)")


def _signed(values: np.ndarray, bits: int) -> np.ndarray:
    values = values.astype(np.int64)
    sign = 1 << (bits - 1)
    return (values ^ sign) - sign


def _steim(data: bytes, count: int, order: str, level: int) -> np.ndarray:
    words = np.frombuffer(data[: len(data) // 64 * 64], dtype=f"{order}u4").reshape(-1, 16)
    diffs: List[int] = []
    first = last = None
    for index, frame in enumerate(words):
        control = int(frame[0])
        for slot in range(1, 16):
            nibble = (control >> (30 - 2 * slot)) & 0b11
            word = int(frame[slot])
            if index == 0 and slot in (1, 2):
                value = word - (1 << 32) if word & 0x80000000 else word
                if slot == 1:
                    first = value
                else:
                    last = value
                continue
            if nibble == 0:
                continue
            diffs.extend(_steim_word(word, nibble, level))
        if len(diffs) >= count:
            break
    if first is None:
        raise MSeedError("Steim cercevesi bos")
    if count == 0:
        return np.zeros(0, dtype=np.int64)
    deltas = np.asarray(diffs[1:count], dtype=np.int64)
    samples = np.concatenate(([first], first + np.cumsum(deltas)))
    # Ornekler 32 bit tamsayidir; kodlayici gibi tasmada sarilir.
    samples = (samples + (1 << 31)) % (1 << 32) - (1 << 31)
    if len(samples) != count:
        raise MSeedError(f"Steim: {count} ornek beklendi, {len(samples)} cozuldu")
    if last is not None and samples[-1] != last:
        raise MSeedError("Steim: son ornek ters entegrasyon sabitiyle uyusmuyor")
    return samples


def _steim_word(word: int, nibble: int, level: int) -> Iterable[int]:
    if nibble == 1:
        return _split(word, 8, 4)
    if level == 1:
        return _split(word, 16, 2) if nibble == 2 else _split(word, 32, 1)
    dnib = word >> 30
    if nibble == 2:
        layout = {1: (30, 1), 2: (15, 2), 3: (10, 3)}.get(dnib)
    else:
        layout = {0: (6, 5), 1: (5, 6), 2: (4, 7)}.get(dnib)
    if layout is None:
        raise MSeedError(f"Steim2: gecersiz alt kod {dnib}")
    return _split(word, *layout)


def _split(word: int, bits: int, count: int) -> List[int]:
    mask = (1 << bits) - 1
    sign = 1 << (bits - 1)
    values = []
    for position in range(count - 1, -1, -1):
        value = (word >> (position * bits)) & mask
        values.append(value - (1 << bits) if value & sign else value)
    return values


def _decode(payload: bytes, encoding: int, count: int, order: str) -> np.ndarray:
    name = ENCODINGS.get(encoding)
    if name is None:
        raise MSeedError(f"Desteklenmeyen miniSEED kodlamasi: {encoding}")
    if name == "steim1":
        return _steim(payload, count, order, 1)
    if name == "steim2":
        return _steim(payload, count, order, 2)
    dtype = np.dtype(name).newbyteorder(order)
    if len(payload) < count * dtype.itemsize:
        raise MSeedError("Kayit verisi ornek sayisindan kisa")
    return np.frombuffer(payload, dtype=dtype, count=count).astype(np.dtype(name))


def _records(raw: bytes) -> Iterable[_Record]:
    offset = 0
    while offset + 48 <= len(raw):
        header = raw[offset: offset + 48]
        if header[:6].strip(b" 0123456789") or header[6:7] not in (b"D", b"R", b"Q", b"M"):
            # Bosluk (ornegin servis hata metni) veya dolgu: bir sonraki 64 bayta gec.
            offset += 64
            continue
        order = _byte_order(header)
        (year, doy, hour, minute, second, _unused, tenth_ms, nsamp, factor, mult,
         activity, _io, _quality, nblock, correction, data_offset, blockette) = struct.unpack(
            order + "HHBBBBHHhhBBBBiHH", header[20:48])
        network = header[18:20].decode("ascii", "replace").strip()
        station = header[8:13].decode("ascii", "replace").strip()
        location = header[13:15].decode("ascii", "replace").strip()
        channel = header[15:18].decode("ascii", "replace").strip()

        encoding = word_order = None
        length = None
        microseconds = 0
        position = blockette
        for _ in range(nblock):
            if not position or offset + position + 4 > len(raw):
                break
            kind, following = struct.unpack(order + "HH", raw[offset + position: offset + position + 4])
            body = raw[offset + position: offset + position + 8]
            if kind == 1000:
                encoding, word_order, exponent = body[4], body[5], body[6]
                length = 1 << exponent
            elif kind == 1001:
                microseconds = struct.unpack("b", body[5:6])[0]
            position = following
        if length is None or encoding is None:
            raise MSeedError("Blockette 1000 olmayan miniSEED kaydi desteklenmiyor")
        if offset + length > len(raw):
            raise MSeedError("Son miniSEED kaydi eksik")

        start = (datetime(year, 1, 1, tzinfo=timezone.utc)
                 + timedelta(days=doy - 1, hours=hour, minutes=minute, seconds=second,
                             microseconds=tenth_ms * 100 + microseconds))
        if not activity & 0x02 and correction:
            start += timedelta(microseconds=correction * 100)
        data_order = ">" if word_order == 1 else "<"
        payload = raw[offset + data_offset: offset + length]
        data = _decode(payload, encoding, nsamp, data_order) if nsamp else np.zeros(0)
        offset += length
        if len(data) == 0:
            continue
        yield _Record((network, station, location, channel), start, _sample_rate(factor, mult), data)


def read_mseed(raw: bytes, max_gap_samples: int = 10) -> List[Trace]:
    """miniSEED baytlarini kanal basina tek :class:`Trace` olarak cozer.

    ``max_gap_samples`` ornekten uzun bosluklar kaydi ayri parcalara boler;
    daha kisa bosluklar dogrusal interpolasyonla doldurulur.
    """
    grouped: Dict[Tuple[str, str, str, str], List[_Record]] = {}
    for record in _records(bytes(raw)):
        grouped.setdefault(record.key, []).append(record)
    traces: List[Trace] = []
    for key, records in grouped.items():
        records.sort(key=lambda item: item.start)
        rate = records[0].rate
        if rate <= 0:
            continue
        units = "counts" if all(np.issubdtype(r.data.dtype, np.integer) for r in records) else "unknown"
        start = records[0].start
        pieces = [records[0].data.astype(np.float64)]
        expected = len(records[0].data)
        for record in records[1:]:
            offset = round((record.start - start).total_seconds() * rate)
            gap = offset - expected
            data = record.data.astype(np.float64)
            if gap > max_gap_samples or abs(record.rate - rate) > 1e-6 * rate:
                traces.append(_trace(key, start, rate, pieces, units))
                start, pieces, expected = record.start, [data], len(data)
                continue
            if gap > 0:
                previous = pieces[-1][-1]
                pieces.append(np.linspace(previous, data[0], gap + 2)[1:-1])
            elif gap < 0:
                data = data[-gap:]
            pieces.append(data)
            expected = offset + len(record.data)
        traces.append(_trace(key, start, rate, pieces, units))
    return traces


def align(traces: List[Trace], start: datetime | None = None, end: datetime | None = None) -> List[Trace]:
    """Izleri ortak zaman araligina kirpar.

    Veri merkezleri dataselect isteginde kayit parcalarini pencereye kirpmadan
    dondurebilir; bilesenler farkli anlarda baslar ve uzunluklari farklidir.
    Sonuc: her iz ``start``/``end`` (verildiyse) ve tum izlerin ortak oldugu
    aralikla sinirlanir, ayni ornek sayisina sahip olur. Ortak aralik yoksa bos
    liste doner.
    """
    if not traces:
        return []
    common_start = max([trace.starttime for trace in traces] + ([start] if start else []))
    common_end = min([trace.endtime for trace in traces] + ([end] if end else []))
    if common_end < common_start:
        return []
    aligned = []
    for trace in traces:
        offset = max(0, math.ceil((common_start - trace.starttime).total_seconds() * trace.sampling_rate - 1e-6))
        aligned.append((trace, offset))
    count = min(int(math.floor((common_end - trace.starttime).total_seconds() * trace.sampling_rate + 1e-6))
                - offset + 1 for trace, offset in aligned)
    if count <= 0:
        return []
    return [Trace(trace.network, trace.station, trace.location, trace.channel,
                  trace.starttime + timedelta(seconds=offset / trace.sampling_rate), trace.sampling_rate,
                  trace.data[offset: offset + count], trace.units, dict(trace.meta))
            for trace, offset in aligned]


def _trace(key, start, rate, pieces, units) -> Trace:
    network, station, location, channel = key
    data = np.concatenate(pieces) if pieces else np.zeros(0)
    return Trace(network, station, location, channel, start, rate, data, units)
