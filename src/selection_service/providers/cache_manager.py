import hashlib
import os
import time
import json
import pandas as pd
from typing import Optional, Any

class CacheManager:
    def __init__(self, cache_dir: str = ".cache", expiry_hours: int = 24):
        self.cache_dir = cache_dir
        self.expiry_seconds = expiry_hours * 3600
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

    def _generate_key(self, provider_name: str, criteria: Any) -> str:
        """
        Kriterlerden benzersiz bir dosya adı üretir.
        Hem Pydantic modeli hem de Dictionary (dict) destekler.
        """
        try:
            if hasattr(criteria, 'model_dump_json'):
                # Pydantic v2
                criteria_str = criteria.model_dump_json()
            elif isinstance(criteria, dict):
                # Dictionary ise, anahtarları sırala ki hash her zaman aynı çıksın
                # default=str, datetime objelerini stringe çevirmek için gereklidir
                criteria_str = json.dumps(criteria, sort_keys=True, default=str)
            else:
                # Fallback
                criteria_str = str(criteria)
                
            raw_key = f"{provider_name}_{criteria_str}"
            return hashlib.md5(raw_key.encode()).hexdigest()
        except Exception as e:
            print(f"[CACHE KEY ERROR] Hash üretilemedi: {e}")
            # Hata durumunda timestamp kullanarak unique ama cachelenemeyen bir key üret
            # (Sistemi kırmamak için)
            return f"error_{int(time.time())}"

    def get(self, provider_name: str, criteria: Any) -> Optional[pd.DataFrame]:
        key = self._generate_key(provider_name, criteria)
        file_path = os.path.join(self.cache_dir, f"{key}.parquet")
        
        if not os.path.exists(file_path):
            return None

        # --- ZAMAN AŞIMI KONTROLÜ ---
        try:
            file_mod_time = os.path.getmtime(file_path)
            if (time.time() - file_mod_time) > self.expiry_seconds:
                print(f"[CACHE] {provider_name} verisi zaman aşımına uğradı. Siliniyor...")
                os.remove(file_path)
                return None
        except OSError:
            return None
        # ----------------------------

        try:
            # engine='pyarrow' veya 'fastparquet' kullanılabilir
            return pd.read_parquet(file_path, engine='pyarrow')
        except Exception as e:
            print(f"[CACHE READ ERROR] Okuma hatası ({key}): {e}")
            return None

    def set(self, provider_name: str, criteria: Any, df: pd.DataFrame):
        if df is None or df.empty:
            return
            
        key = self._generate_key(provider_name, criteria)
        file_path = os.path.join(self.cache_dir, f"{key}.parquet")
        
        try:
            # index=False önemli, gereksiz index sütunu oluşmasın
            df.to_parquet(file_path, engine='pyarrow', index=False)
            # print(f"[CACHE SAVED] {provider_name} -> {key}.parquet") 
        except Exception as e:
            print(f"[CACHE WRITE ERROR] Yazma hatası ({file_path}): {e}")