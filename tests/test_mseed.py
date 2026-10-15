import struct
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from selection_service.waveform import MSeedError, Trace, read_mseed, write_ascii
from selection_service.waveform.mseed import _sample_rate, _split

DATA = Path(__file__).parent / "data" / "mseed"


def expected_signal():
    # Fikstur dosyalari bu diziden uretildi (ObsPy 1.5.0, 512 bayt kayit).
    t = np.arange(3000)
    return (np.sin(t / 7.0) * 20000 + np.sin(t / 0.9) * 300 + (t % 13) * 5).astype(np.int32)


@pytest.mark.parametrize("name", ["steim1", "steim2", "int32"])
def test_integer_encodings_match_source(name):
    east, north = read_mseed((DATA / f"{name}.mseed").read_bytes())
    signal = expected_signal()
    assert east.id == "TK.4614.00.HNE" and north.id == "TK.4614.00.HNN"
    assert np.array_equal(east.data, signal) and np.array_equal(north.data, -signal)
    assert east.sampling_rate == 100.0 and east.delta == 0.01 and east.units == "counts"
    assert east.starttime == datetime(2023, 2, 6, 1, 17, 34, 123400, tzinfo=timezone.utc)
    assert east.endtime == datetime(2023, 2, 6, 1, 18, 4, 113400, tzinfo=timezone.utc)


def test_int16_float_and_little_endian():
    (east, _) = read_mseed((DATA / "int16.mseed").read_bytes())
    assert np.array_equal(east.data, (expected_signal() // 2).astype(np.int16))
    (vertical,) = read_mseed((DATA / "float32.mseed").read_bytes())
    assert np.allclose(vertical.data, (expected_signal() / 3.0).astype(np.float32))
    assert vertical.units == "unknown"
    (little,) = read_mseed((DATA / "float64_le.mseed").read_bytes())
    assert np.allclose(little.data, vertical.data)


def test_large_records():
    (trace,) = read_mseed((DATA / "steim2_4096.mseed").read_bytes())
    assert np.array_equal(trace.data, expected_signal())


def test_records_are_merged_gaps_filled_and_overlaps_trimmed():
    raw = (DATA / "int32.mseed").read_bytes()
    records = [raw[i:i + 512] for i in range(0, len(raw), 512)]
    east = [record for record in records if record[15:18] == b"HNE"]
    # Ikinci kaydi atla: bosluk 10 ornekten uzun -> iki parca.
    split = read_mseed(east[0] + east[2])
    assert len(split) == 2
    # Ayni kaydi iki kez ver: cakisan ornekler atilir.
    (doubled,) = read_mseed(east[0] + east[0] + east[1])
    count0 = struct.unpack(">H", east[0][30:32])[0]
    count1 = struct.unpack(">H", east[1][30:32])[0]
    assert len(doubled.data) == count0 + count1
    # Kisa bosluk interpolasyonla dolar.
    shifted = bytearray(east[1])
    tenth = struct.unpack(">H", shifted[28:30])[0]
    assert tenth + 500 < 10000
    shifted[28:30] = struct.pack(">H", tenth + 500)  # 0.05 s = 5 ornek ileri
    (filled,) = read_mseed(east[0] + bytes(shifted))
    assert len(filled.data) == count0 + count1 + 5


def test_non_record_bytes_are_skipped_and_errors_raised():
    raw = (DATA / "int32.mseed").read_bytes()
    assert len(read_mseed(b"X" * 64 + raw)) == 2
    with pytest.raises(MSeedError):
        read_mseed(raw[:600])  # yarim kayit
    broken = bytearray(raw[:512])
    broken[52] = 99  # Blockette 1000 kodlama alani
    with pytest.raises(MSeedError, match="kodlamasi"):
        read_mseed(bytes(broken))
    no_blockette = bytearray(raw[:512])
    no_blockette[39] = 0
    with pytest.raises(MSeedError, match="Blockette 1000"):
        read_mseed(bytes(no_blockette))
    bad_year = bytearray(raw[:512])
    bad_year[20:22] = b"\x00\x00"
    with pytest.raises(MSeedError, match="yil"):
        read_mseed(bytes(bad_year))


def test_steim_last_sample_check():
    raw = bytearray((DATA / "steim1.mseed").read_bytes()[:512])
    raw[64 + 8: 64 + 12] = struct.pack(">i", 123456789)  # ters entegrasyon sabiti
    with pytest.raises(MSeedError, match="son ornek"):
        read_mseed(bytes(raw))


def test_sample_rate_and_bit_helpers():
    assert _sample_rate(100, 1) == 100 and _sample_rate(1, -10) == 0.1
    assert _sample_rate(-10, 1) == 0.1 and _sample_rate(-10, -10) == 0.01
    assert _sample_rate(0, 1) == 0 and _sample_rate(20, 0) == 20
    assert _split(0xFF01_7F80, 8, 4) == [-1, 1, 127, -128]


def test_write_ascii_round_trip(tmp_path):
    trace = Trace("TK", "4614", "", "HNE", datetime(2023, 2, 6, 1, 17, 34, 500000, tzinfo=timezone.utc),
                  100.0, np.array([1.5, -2.25, 0.0]), "cm/s^2", {"SENSITIVITY": 2.0})
    (path,) = write_ascii([trace], tmp_path / "out", "rec", header={"EVENT_ID": "e1"})
    assert path.name == "rec_HNE.ASC"
    lines = path.read_text().splitlines()
    header = dict(line.split(": ", 1) for line in lines if ": " in line)
    assert header["STREAM"] == "HNE" and header["NDATA"] == "3" and header["UNITS"] == "cm/s^2"
    assert header["DATE_TIME_FIRST_SAMPLE_YYYYMMDD_HHMMSS"] == "20230206_011734.500"
    assert header["EVENT_ID"] == "e1" and header["SENSITIVITY"] == "2.0"
    assert [float(value) for value in lines[-3:]] == [1.5, -2.25, 0.0]
