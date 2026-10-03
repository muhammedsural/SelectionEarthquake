"""Provider'lar arası tekrar eden deprem/kayıt tespiti.

AFAD ve PEER aynı depremi farklı adlarla (ve biraz farklı konum/büyüklükle)
döndürebilir. Ortak bir olay kimliği olmadığından eşleştirme sezgiseldir:

* Olay eşleşmesi: aynı ``YEAR``, büyüklük farkı ``max_mag_diff`` içinde ve
  episantr mesafesi ``max_event_distance_km`` içinde. Yalnızca FARKLI
  provider'ların olayları eşlenir ve eşleşme karşılıklı en yakın olay ile
  sınırlıdır; böylece aynı provider'daki artçı şoklar birleşmez.
* Kayıt tekrarı: aynı olay grubunda, farklı provider'lardan gelen ve istasyon
  konumu ``max_station_distance_km`` içinde olan kayıtlar aynı kayıt sayılır.

Hatalı birleştirmenin bedeli düşüktür (olay başına 3 kayıt sınırı biraz daha
sıkı uygulanır); eksik birleştirmenin bedeli ise aynı depremin sınırı
aşmasıdır. Bu nedenle eşikler bilinçli olarak cömerttir.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

EVENT_GROUP_COLUMN = "EVENT_GROUP"

DEFAULT_MAX_EVENT_DISTANCE_KM = 50.0
DEFAULT_MAX_MAG_DIFF = 0.5
DEFAULT_MAX_STATION_DISTANCE_KM = 1.0

_EARTH_RADIUS_KM = 6371.0088


def valid_coordinates(lat: pd.Series, lon: pd.Series) -> pd.Series:
    """Geçerli koordinat maskesi (NaN, -999 ve (0, 0) yer tutucuları hariç)."""
    lat = pd.to_numeric(lat, errors="coerce")
    lon = pd.to_numeric(lon, errors="coerce")
    in_range = lat.between(-90, 90) & lon.between(-180, 180)
    placeholder = (lat == 0) & (lon == 0)
    return in_range & ~placeholder & lat.notna() & lon.notna()


def haversine_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    """Büyük daire mesafesi (km); girdiler derece, numpy yayınlama destekli."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = (
        np.sin((lat2 - lat1) / 2) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * _EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def _unique_events(df: pd.DataFrame) -> pd.DataFrame:
    """Her (PROVIDER, EVENT) için tek satır: yıl, büyüklük, konum."""
    cols = ["PROVIDER", "EVENT", "YEAR", "MAGNITUDE", "HYPO_LAT", "HYPO_LON"]
    events = df[cols].drop_duplicates(["PROVIDER", "EVENT"]).copy()
    for col in ("YEAR", "MAGNITUDE", "HYPO_LAT", "HYPO_LON"):
        events[col] = pd.to_numeric(events[col], errors="coerce")
    events["valid"] = valid_coordinates(events["HYPO_LAT"], events["HYPO_LON"])
    events["valid"] &= events["YEAR"].notna() & events["MAGNITUDE"].notna()
    return events[events["valid"]].reset_index(drop=True)


def _best_matches(
    left: pd.DataFrame, right: pd.DataFrame, max_km: float, max_dm: float
) -> Dict[int, int]:
    """``left`` olaylarının en yakın uygun ``right`` olayı (indeks -> indeks)."""
    if left.empty or right.empty:
        return {}
    dist = haversine_km(
        left["HYPO_LAT"].to_numpy()[:, None], left["HYPO_LON"].to_numpy()[:, None],
        right["HYPO_LAT"].to_numpy()[None, :], right["HYPO_LON"].to_numpy()[None, :],
    )
    same_year = left["YEAR"].to_numpy()[:, None] == right["YEAR"].to_numpy()[None, :]
    close_mag = (
        np.abs(left["MAGNITUDE"].to_numpy()[:, None] - right["MAGNITUDE"].to_numpy()[None, :])
        <= max_dm
    )
    eligible = same_year & close_mag & (dist <= max_km)
    dist = np.where(eligible, dist, np.inf)
    matches: Dict[int, int] = {}
    for i in range(dist.shape[0]):
        j = int(np.argmin(dist[i]))
        if np.isfinite(dist[i, j]):
            matches[i] = j
    return matches


def assign_event_groups(
    df: pd.DataFrame,
    max_event_distance_km: float = DEFAULT_MAX_EVENT_DISTANCE_KM,
    max_mag_diff: float = DEFAULT_MAX_MAG_DIFF,
) -> pd.Series:
    """Her satır için olay grubu kimliği döndür.

    Eşleşen provider'lar arası olaylar aynı ``G<n>`` grubunu paylaşır; diğer
    olaylar ``<PROVIDER>|<EVENT>`` olarak kalır (adlar provider'lar arasında
    çakışmasın diye).
    """
    required = {"PROVIDER", "EVENT", "YEAR", "MAGNITUDE", "HYPO_LAT", "HYPO_LON"}
    base = df["PROVIDER"].astype(str) + "|" + df["EVENT"].astype(str)
    if not required <= set(df.columns) or df["PROVIDER"].nunique() < 2:
        return base

    events = _unique_events(df)
    parent: Dict[Tuple[str, str], Tuple[str, str]] = {}

    def find(node):
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    providers = list(events["PROVIDER"].unique())
    by_provider = {p: events[events["PROVIDER"] == p].reset_index(drop=True) for p in providers}
    for a_idx, a in enumerate(providers):
        for b in providers[a_idx + 1:]:
            left, right = by_provider[a], by_provider[b]
            forward = _best_matches(left, right, max_event_distance_km, max_mag_diff)
            backward = _best_matches(right, left, max_event_distance_km, max_mag_diff)
            for i, j in forward.items():
                if backward.get(j) == i:  # karşılıklı en yakın
                    ra = (left.at[i, "PROVIDER"], str(left.at[i, "EVENT"]))
                    rb = (right.at[j, "PROVIDER"], str(right.at[j, "EVENT"]))
                    parent[find(ra)] = find(rb)

    members: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
    for node in list(parent):
        members.setdefault(find(node), []).append(node)
    labels: Dict[Tuple[str, str], str] = {}
    for n, nodes in enumerate(sorted(m for m in members.values() if len(m) > 1), 1):
        for node in nodes:
            labels[node] = f"G{n}"

    keys = list(zip(df["PROVIDER"].astype(str), df["EVENT"].astype(str)))
    return pd.Series(
        [labels.get(k, f"{k[0]}|{k[1]}") for k in keys], index=df.index, name=EVENT_GROUP_COLUMN
    )


def _unit_vectors(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    """Derece cinsinden koordinatları birim küre üzerindeki 3B noktalara çevir."""
    lat, lon = np.radians(lat), np.radians(lon)
    return np.column_stack(
        (np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat))
    )


def find_duplicate_records(
    df: pd.DataFrame,
    group_col: str = EVENT_GROUP_COLUMN,
    max_station_distance_km: float = DEFAULT_MAX_STATION_DISTANCE_KM,
) -> pd.Series:
    """Başka provider'daki bir kaydın tekrarı olan satırlar için True.

    Aynı olay grubunda ve istasyon konumu ``max_station_distance_km`` içindeki
    kayıtlardan, DataFrame'de ilk görünen provider'ınki korunur. Provider'lar
    sırayla işlenir; her biri, önceki provider'ların korunan kayıtlarından
    kurulan bir KD-ağacında sorgulanır (grup başına O(n log n)).
    """
    duplicate = pd.Series(False, index=df.index)
    needed = {group_col, "PROVIDER", "STATION_LAT", "STATION_LON"}
    if not needed <= set(df.columns):
        return duplicate

    valid = valid_coordinates(df["STATION_LAT"], df["STATION_LON"])
    order = {p: n for n, p in enumerate(df["PROVIDER"].astype(str).unique())}
    # Birim küredeki kiriş uzunluğu: 2 * sin(açı / 2)
    chord = 2 * np.sin(max_station_distance_km / _EARTH_RADIUS_KM / 2)

    candidates = df[valid]
    for _, rows in candidates.groupby(group_col):
        if rows["PROVIDER"].nunique() < 2:
            continue
        rank = rows["PROVIDER"].astype(str).map(order).to_numpy()
        points = _unit_vectors(
            pd.to_numeric(rows["STATION_LAT"]).to_numpy(),
            pd.to_numeric(rows["STATION_LON"]).to_numpy(),
        )
        index = rows.index.to_numpy()
        kept = np.zeros(len(rows), dtype=bool)
        for r in sorted(set(rank)):
            current = rank == r
            if kept.any():
                tree = cKDTree(points[kept])
                distance, _ = tree.query(points[current], distance_upper_bound=chord)
                is_dup = np.isfinite(distance)
            else:
                is_dup = np.zeros(int(current.sum()), dtype=bool)
            duplicate.loc[index[current][is_dup]] = True
            kept[np.flatnonzero(current)[~is_dup]] = True
    return duplicate
