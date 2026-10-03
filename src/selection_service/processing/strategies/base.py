"""Strateji interface'i ve ortak taban sinif."""

from abc import ABC
import math
from typing import Any, Dict, List, Protocol, Tuple
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

class BaseSelectionStrategy(ISelectionStrategy, ABC):
    """Temel seçim stratejisi"""
    
    def __init__(self, config: SelectionConfig):
        self.config = config

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
        
        selected_df, scored_df = self._apply_selection_rules_with_reasons(scored_df)
        return selected_df, scored_df
    
    def _apply_selection_rules(self, df_scored: pd.DataFrame) -> pd.DataFrame:
        """Seçim kurallarını uygula"""
        selected, _ = self._apply_selection_rules_with_reasons(df_scored)
        return selected

    def _apply_selection_rules_with_reasons(
        self, df_scored: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Apply TBDY selection limits and annotate every record with a reason."""
        df_scored = df_scored.copy()
        df_scored["SELECTION_STATUS"] = "not_evaluated"
        df_scored["SELECTION_REASON"] = ""

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
        
        sorted_df = filtered_df.sort_values('SCORE', ascending=False)
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
            event = record.get('EVENT', '')
            
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
