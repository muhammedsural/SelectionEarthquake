"""
providers/PeerProvider.py  (refactored)

Değişiklikler (Adım 1 — ISP + LSP):
  - IDataProvider  → IDataFetcher  (yalnızca veri çekme sözleşmesi)
  - download_single_waveforms()   → KALDIRILDI
      PEER NGA-West2 düz dosya tabanlı bir veri seti; download protokolü
      bu provider için anlamsız. LSP ihlalini ortadan kaldırmak için
      metod tamamen silindi.
      İhtiyaç varsa: supports_download(peer_provider) → False döner,
      çağıran kod güvenle bu durumu yönetebilir.
  - asyncio.get_event_loop()  →  asyncio.get_running_loop()
      Python 3.10+ DeprecationWarning düzeltmesi.
"""

import asyncio
import logging
from functools import partial
from typing import Any, Dict, Type

import numpy as np
import pandas as pd

from ..core.Config import (
    MECHANISM_MAP,
    REVERSE_MECHANISM_MAP,
    convert_mechanism_to_text,
)
from ..core.ErrorHandle import DataProcessingError, ProviderError
from ..enums.Enums import ProviderName
from ..processing.Mappers import IColumnMapper
from ..processing.ResultHandle import async_result_decorator, result_decorator
from ..processing.Selection import SearchCriteria
from ..providers.interfaces import IDataFetcher          # ← yeni interface
from ..utility.path_utils import load_csv


logger = logging.getLogger(__name__)


class PeerWest2Provider(IDataFetcher):
    """PEER NGA-West2 veri sağlayıcı.

    Yalnızca IDataFetcher uygular; dalga formu indirme desteği yoktur.
    Download gereken senaryolarda supports_download(provider) → False döner
    ve çağıran kod bu durumu yönetir (bkz. EarthquakeApi.download_waveforms).
    """

    def __init__(self, column_mapper: Type[IColumnMapper], **kwargs: Any) -> None:
        self.column_mapper = column_mapper
        self.name = ProviderName.PEER.value
        self.flatfile_df = load_csv("NGA-West2_flatfile.csv")
        self.mapped_df: pd.DataFrame | None = None
        self.response_df: pd.DataFrame | None = None

    # ──────────────────────────────────────────────────────────────
    # IDataFetcher sözleşmesi
    # ──────────────────────────────────────────────────────────────

    def get_name(self) -> str:
        return str(self.name)

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        """SearchCriteria → PEER filtre parametrelerine dönüştür."""
        return criteria.to_peer_params()

    @async_result_decorator
    async def fetch_data_async(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """NGA-West2 verilerini asenkron olarak çek.

        CSV okuma CPU-bound olduğundan thread pool'a atılır;
        event loop bloke edilmez.
        """
        try:
            # get_running_loop: Python 3.10+ uyumlu (get_event_loop deprecated)
            loop = asyncio.get_running_loop()
            self.mapped_df = await loop.run_in_executor(
                None,
                partial(self.column_mapper.map_columns, self.flatfile_df.copy()),
            )
            filtered_df = await loop.run_in_executor(
                None,
                partial(self._apply_filters, self.mapped_df, criteria),
            )
            filtered_df["PROVIDER"] = str(self.name)
            return self._normalize_mechanism(filtered_df)
        except Exception as e:
            raise ProviderError(
                self.name, e, f"PEER async data fetch failed: {e}"
            )

    @result_decorator
    def fetch_data_sync(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """NGA-West2 verilerini senkron olarak çek."""
        try:
            df = self.flatfile_df.copy()
            self.mapped_df = self.column_mapper.map_columns(df=df)
            self.mapped_df = self._apply_filters(self.mapped_df, criteria)
            self.mapped_df["PROVIDER"] = str(self.name)
            self.mapped_df = self._normalize_mechanism(self.mapped_df)
            logger.info("PEER'dan %d kayıt alındı.", len(self.mapped_df))
            return self.mapped_df
        except Exception as e:
            raise ProviderError(
                self.name, e, f"PEER sync data fetch failed: {e}"
            )

    # ──────────────────────────────────────────────────────────────
    # Private yardımcılar
    # ──────────────────────────────────────────────────────────────

    def _normalize_mechanism(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sayısal mekanizma sütununu metin karşılığına çevir."""
        if df["MECHANISM"].dtype in (np.int64, np.float64, int, float):
            return convert_mechanism_to_text(df)
        return df

    def _apply_filters(
        self,
        df: pd.DataFrame,
        criteria: Dict[str, Any],
    ) -> pd.DataFrame:
        """Arama kriterlerine göre DataFrame'i filtrele.

        Her kriter None ise o filtre uygulanmaz; bu sayede kısmi
        kriterlerle çalışmak mümkündür.
        """
        if df.empty:
            return df

        try:
            filtered = df.copy()

            # --- Numeric range filters (DRY: helper ile) ---
            numeric_filters: list[tuple[str, str, str]] = [
                # (criteria_min_key, criteria_max_key, df_column)
                ("min_magnitude",  "max_magnitude",  "MAGNITUDE"),
                ("min_Rjb",        "max_Rjb",        "RJB(km)"),
                ("min_Rrup",       "max_Rrup",        "RRUP(km)"),
                ("min_vs30",       "max_vs30",        "VS30(m/s)"),
                ("min_depth",      "max_depth",       "HYPO_DEPTH(km)"),
                ("min_pga",        "max_pga",         "PGA(cm2/sec)"),
                ("min_pgv",        "max_pgv",         "PGV(cm/sec)"),
                ("min_pgd",        "max_pgd",         "PGD(cm)"),
            ]
            for min_key, max_key, col in numeric_filters:
                filtered = self._apply_range(filtered, criteria, min_key, max_key, col)

            # --- Kategorik filtre ---
            mechanisms = self._mechanism_filter_values(filtered, criteria)
            if mechanisms:
                filtered = filtered[
                    filtered["MECHANISM"].isin(mechanisms)
                ]

            return filtered

        except Exception as e:
            raise DataProcessingError(self.name, e, "Filter application failed")

    @staticmethod
    def _mechanism_filter_values(
        df: pd.DataFrame,
        criteria: Dict[str, Any],
    ) -> list[Any]:
        """Return mechanism filter values compatible with the DataFrame dtype."""
        values = list(criteria.get("mechanisms") or [])
        fault_type = criteria.get("fault_type")
        if fault_type is not None:
            values.append(fault_type)
        if not values or "MECHANISM" not in df.columns:
            return []

        normalized: list[Any] = []
        for value in values:
            normalized.append(value)
            if isinstance(value, str) and value in REVERSE_MECHANISM_MAP:
                normalized.append(REVERSE_MECHANISM_MAP[value])
            elif isinstance(value, (int, float, np.integer, np.floating)):
                normalized.append(MECHANISM_MAP.get(int(value), "Unknown"))

        return list(dict.fromkeys(normalized))

    @staticmethod
    def _apply_range(
        df: pd.DataFrame,
        criteria: Dict[str, Any],
        min_key: str,
        max_key: str,
        col: str,
    ) -> pd.DataFrame:
        """Tek bir sayısal aralık filtresi uygula (None-safe)."""
        if col not in df.columns:
            return df
        min_val = criteria.get(min_key)
        max_val = criteria.get(max_key)
        if min_val is not None:
            df = df[df[col] >= min_val]
        if max_val is not None:
            df = df[df[col] <= max_val]
        return df
