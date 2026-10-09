"""Strateji interface'i ve ortak taban sinif."""

from abc import ABC
import math
from typing import Any, Dict, List, Protocol, Sequence, Tuple
import pandas as pd
from ...core.config import (
    SCORING_MAP,
)
from ..criteria import SearchCriteria, SelectionConfig


class ISelectionStrategy(Protocol):
    """Seçim stratejisi interface'i"""
    
    def select_and_score(self, df: pd.DataFrame, criteria: SearchCriteria) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Kayıtları seç ve puanla"""
        ...
        
    def get_name(self) -> str:
        """Strateji adı"""
        ...

MAGNITUDE_COLUMN = SCORING_MAP["magnitude"]["column"]
# Varsayilan siralamada kullanilan mesafe kolonlari (oncelik sirasiyla).
# Bir kayitta ilk dolu olan kullanilir: Rjb, yoksa Rrup, yoksa Repi.
DISTANCE_COLUMNS = (
    SCORING_MAP["rjb"]["column"],
    SCORING_MAP["rrup"]["column"],
    SCORING_MAP["repi"]["column"],
)


def _distance_series(df: pd.DataFrame) -> pd.Series:
    """Her kayit icin ilk dolu mesafe degeri (Rjb > Rrup > Repi); yoksa +inf."""
    distance = pd.Series(float("nan"), index=df.index, dtype="float64")
    for column in DISTANCE_COLUMNS:
        if column in df.columns:
            distance = distance.fillna(pd.to_numeric(df[column], errors="coerce"))
    return distance.fillna(float("inf"))


def order_candidates(
    df: pd.DataFrame, primary: Sequence[Tuple[str, bool]] = ()
) -> pd.DataFrame:
    """Adaylari deterministik sirala.

    ``primary`` verilen (kolon, artan_mi) anahtarlariyla siralar; esitlikler
    (veya ``primary`` bossa tum siralama) varsayilan duzenle cozulur:
    once buyuk ``MAGNITUDE``, sonra kucuk mesafe (Rjb > Rrup > Repi).
    Eksik buyukluk/mesafe en sona duser; tum anahtarlar esitse orijinal
    sira korunur.
    """
    if df.empty:
        return df
    mag_key = "__ORDER_MAG__"
    dist_key = "__ORDER_DIST__"
    pos_key = "__ORDER_POS__"
    work = df.copy()
    if MAGNITUDE_COLUMN in work.columns:
        work[mag_key] = pd.to_numeric(work[MAGNITUDE_COLUMN], errors="coerce").fillna(
            float("-inf")
        )
    else:
        work[mag_key] = float("-inf")
    work[dist_key] = _distance_series(work)
    work[pos_key] = range(len(work))
    columns = [col for col, _ in primary] + [mag_key, dist_key, pos_key]
    ascending = [asc for _, asc in primary] + [False, True, True]
    work = work.sort_values(columns, ascending=ascending)
    # Konumsal secim: index tekrarli olsa bile dogru calisir.
    return df.iloc[work[pos_key].to_numpy()]


class BaseSelectionStrategy(ISelectionStrategy, ABC):
    """Temel seçim stratejisi"""
    
    def __init__(self, config: SelectionConfig):
        self.config = config

    @staticmethod
    def _event_key(record: pd.Series) -> Any:
        """Olay başına kayıt sınırı için anahtar.

        Birden fazla provider'dan gelen veride ``EVENT_GROUP`` aynı depremin
        farklı adlarını tek olay sayar; yoksa ``EVENT`` kullanılır.
        """
        group = record.get("EVENT_GROUP")
        if group is not None and not pd.isna(group) and str(group) != "":
            return group
        return record.get("EVENT", "")

    def _gaussian_score(self, value: float, target: float, sigma: float) -> float:
        """Çan Eğrisi (Gaussian) Puanlama Fonksiyonu. Hedef değere tam isabet = 1.0 puan. Uzaklaştıkça puan yumuşak bir şekilde düşer.
            Gaussian Formülü: e^(-(x-u)^2 / (2*sigma^2))
        Args:
            value (float): _description_
            target (float): _description_
            sigma (float): _description_

        Returns:
            float: _description_
        """
        if value is None or target is None or pd.isna(value):
            return 0.0
        # Çan eğrisi formülü
        diff = value - target
        return math.exp(- (diff * diff) / (2 * sigma * sigma))

    def _categorical_score(self, record_val: str, target_list: list) -> float:
        """Metinsel eşleşme puanı (Mekanizma vb için)"""
        if not record_val or not target_list:
            return 0.0
        
        record_val_str = str(record_val)
        # Tam eşleşme
        if any(t == record_val_str for t in target_list):
            return 1.0
        # Kısmi eşleşme (Örn: "Reverse" arıyoruz, kayıt "Reverse-Oblique")
        if any(t in record_val_str for t in target_list):
            return 0.7
        return 0.0

    def _calculate_total_score(self, record: pd.Series, criteria: SearchCriteria) -> float:
        """
        DİNAMİK PUANLAMA MOTORU
        Config'deki tüm parametreleri tarar, kullanıcı ne girdiyse ona göre puanlar.
        """
        score, _ = self._calculate_score_breakdown(record, criteria)
        return score

    def _calculate_score_breakdown(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """Return total score and criterion-level contribution details."""
        total_weighted_score = 0.0
        total_active_weight = 0.0
        breakdown: List[Dict[str, Any]] = []
        
        # Config'deki tüm parametreler üzerinde dönüyoruz (Magnitude, Rjb, Rrup, Vs30...)
        for key, config in SCORING_MAP.items():
            
            # 1. Bu parametre için bir hedef (Target) var mı?
            # Kullanıcı target girmediyse veya min-max aralığı vermediyse bu parametreyi ELİMİNE ET.
            if key == 'mechanism':
                # Mekanizma özel durumu: liste boşsa geç
                mechanism_targets = criteria.get_mechanism_targets()
                if not mechanism_targets:
                    continue
                target_val = mechanism_targets
            else:
                target_val = criteria.get_effective_target(key)
                if target_val is None:
                    continue

            # 2. DataFrame'de bu veri var mı?
            col_name = config['column']
            if col_name not in record or pd.isna(record[col_name]):
                # Kullanıcı hedef istemiş ama veri setinde (örneğin PEER'de) bu kolon yoksa puanlamaya katma
                breakdown.append({
                    "criterion": key,
                    "column": col_name,
                    "status": "missing",
                    "target": target_val,
                    "value": None,
                    "weight": criteria.weights.get_weight(key),
                    "raw_score": 0.0,
                    "weighted_score": 0.0,
                })
                continue

            # 3. Ağırlığı al
            weight = criteria.weights.get_weight(key)
            if weight <= 0:
                breakdown.append({
                    "criterion": key,
                    "column": col_name,
                    "status": "inactive_weight",
                    "target": target_val,
                    "value": record[col_name],
                    "weight": weight,
                    "raw_score": 0.0,
                    "weighted_score": 0.0,
                })
                continue

            # 4. Puanı Hesapla
            score = 0.0
            sigma = None
            if config['type'] == 'numeric':
                sigma = criteria.get_sigma(key)
                score = self._gaussian_score(record[col_name], target_val, sigma)
            
            elif config['type'] == 'categorical':
                score = self._categorical_score(record[col_name], target_val)

            # 5. Toplama Ekle
            total_weighted_score += score * weight
            total_active_weight += weight
            breakdown.append({
                "criterion": key,
                "column": col_name,
                "status": "active",
                "target": target_val,
                "value": record[col_name],
                "weight": weight,
                "sigma": sigma,
                "raw_score": score,
                "weighted_score": score * weight,
            })

        # 6. Normalizasyon (0-100 arası)
        # Eğer hiçbir kriter girilmediyse 0 döndür
        if total_active_weight == 0:
            return 0.0, breakdown
            
        return (total_weighted_score / total_active_weight) * 100.0, breakdown
    
    def select_and_score(self, df: pd.DataFrame, criteria: SearchCriteria) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """ Kayıtları puanla ve seç. 

        Args:
            df (pd.DataFrame): Puanlanacak veri seti
            criteria (SearchCriteria): Kullanıcının girdiği arama kriterleri ve ağırlıklar

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Seçilen kayıtlar ve tüm kayıtların puanlı hali
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        scored_df = df.copy()
        
        # Vektörize işlem yerine apply kullanıyoruz (karmaşık mantık için daha güvenli)
        # Performans gerekirse numpy ile vektörize edilebilir.
        score_results = scored_df.apply(
            lambda row: self._calculate_score_breakdown(row, criteria), axis=1
        )
        scored_df['SCORE'] = score_results.apply(lambda item: item[0])
        scored_df['SCORE_BREAKDOWN'] = score_results.apply(lambda item: item[1])
        
        # Hicbir kriter icin hedef yoksa (or. yalnizca min_magnitude) skor
        # anlamsizdir (hepsi 0): min_score uygulanmaz, varsayilan siralama
        # (buyuk MAGNITUDE, sonra kucuk mesafe) kullanilir.
        selected_df, scored_df = self._apply_selection_rules_with_reasons(
            scored_df, has_targets=criteria.has_scoring_targets()
        )
        return selected_df, scored_df
    
    def _apply_selection_rules(self, df_scored: pd.DataFrame) -> pd.DataFrame:
        """Seçim kurallarını uygula"""
        selected, _ = self._apply_selection_rules_with_reasons(df_scored)
        return selected

    def _apply_selection_rules_with_reasons(
        self, df_scored: pd.DataFrame, has_targets: bool = True
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Apply TBDY selection limits and annotate every record with a reason.

        ``has_targets=False`` ise ``min_score`` esigi uygulanmaz ve adaylar
        varsayilan duzende (buyuk MAGNITUDE, sonra kucuk mesafe) siralanir.
        """
        df_scored = df_scored.copy()
        df_scored["SELECTION_STATUS"] = "not_evaluated"
        df_scored["SELECTION_REASON"] = ""

        if not has_targets:
            return self._select_in_order(order_candidates(df_scored), df_scored)

        filtered_df = df_scored[df_scored['SCORE'] >= self.config.min_score]
        if filtered_df.empty:
            df_scored.loc[:, "SELECTION_STATUS"] = "rejected"
            df_scored.loc[:, "SELECTION_REASON"] = (
                f"score_below_min_score:{self.config.min_score}"
            )
            return pd.DataFrame(), df_scored

        below_min_mask = df_scored["SCORE"] < self.config.min_score
        df_scored.loc[below_min_mask, "SELECTION_STATUS"] = "rejected"
        df_scored.loc[below_min_mask, "SELECTION_REASON"] = (
            f"score_below_min_score:{self.config.min_score}"
        )
        
        sorted_df = order_candidates(filtered_df, [("SCORE", False)])
        return self._select_in_order(sorted_df, df_scored)

    def _select_in_order(
        self, sorted_df: pd.DataFrame, df_scored: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Siralanmis adaylara num_records ve cesitlilik limitlerini uygula."""
        selected_records = []
        selected_indices = []
        station_counts = {}
        event_counts = {}
        
        for idx, record in sorted_df.iterrows():
            if len(selected_records) >= self.config.num_records:
                df_scored.at[idx, "SELECTION_STATUS"] = "rejected"
                df_scored.at[idx, "SELECTION_REASON"] = (
                    f"num_records_limit:{self.config.num_records}"
                )
                break
            
            station = record.get('STATION', '')
            event = self._event_key(record)
            
            if station_counts.get(station, 0) >= self.config.max_per_station:
                df_scored.at[idx, "SELECTION_STATUS"] = "rejected"
                df_scored.at[idx, "SELECTION_REASON"] = (
                    f"max_per_station:{self.config.max_per_station}"
                )
                continue

            if event_counts.get(event, 0) >= self.config.max_per_event:
                df_scored.at[idx, "SELECTION_STATUS"] = "rejected"
                df_scored.at[idx, "SELECTION_REASON"] = (
                    f"max_per_event:{self.config.max_per_event}"
                )
                continue
            
            selected_records.append(record)
            selected_indices.append(idx)
            station_counts[station] = station_counts.get(station, 0) + 1
            event_counts[event] = event_counts.get(event, 0) + 1
        
        df_scored.loc[selected_indices, "SELECTION_STATUS"] = "selected"
        df_scored.loc[selected_indices, "SELECTION_REASON"] = "selected"

        remaining_mask = df_scored["SELECTION_STATUS"].eq("not_evaluated")
        df_scored.loc[remaining_mask, "SELECTION_STATUS"] = "rejected"
        df_scored.loc[remaining_mask, "SELECTION_REASON"] = (
            f"num_records_limit:{self.config.num_records}"
        )

        selected_df = df_scored.loc[selected_indices].copy()
        return selected_df, df_scored
        
    def get_name(self) -> str:
        return str(self.config.design_code.value)
