# providers/afad/AfadFileManager.py
import os
import zipfile
from typing import List
from selection_service.core.ErrorHandle import ProviderError

class AfadFileManager:
    """Dosya ve ZIP işlemlerini yöneten sınıf"""
    
    def __init__(self, base_dir: str = "Afad_events"):
        self.base_dir = base_dir

    def ensure_event_dir(self, event_id: int) -> str:
        path = os.path.join(self.base_dir, f"event_{event_id}")
        os.makedirs(path, exist_ok=True)
        return path

    def save_zip(self, content: bytes, event_id: int, filename: str) -> str:
        folder = self.ensure_event_dir(event_id)
        path = os.path.join(folder, filename)
        with open(path, 'wb') as f:
            f.write(content)
        return path

    def extract_zip(self, zip_path: str, export_type: str = "asc2") -> List[str]:
        """Güvenli zip çıkarma ve iç içe zip kontrolü"""
        extracted_files = []
        target_dir = os.path.dirname(zip_path)
        
        try:
            if not os.path.isfile(zip_path) or not zipfile.is_zipfile(zip_path):
                raise ProviderError("AFAD", None, "Downloaded file is not a valid ZIP archive")

            with zipfile.ZipFile(zip_path, 'r') as zf:
                if zf.testzip() is not None:
                    raise ProviderError("AFAD", None, "Corrupted ZIP file")

                for file_info in zf.infolist():
                    if file_info.is_dir():
                        continue
                    self._ensure_safe_member(file_info.filename)
                    # Dosya isminden station_id ayıklama ve klasörleme mantığı buraya eklenebilir
                    # Şimdilik basic extract yapıyoruz, Station klasörlemesi Provider içinde yönetilebilir
                    # veya buraya taşınabilir.
                    zf.extract(file_info, target_dir)
                    extracted_path = os.path.join(target_dir, file_info.filename)
                    
                    # İç içe zip kontrolü
                    if file_info.filename.endswith('.zip') and export_type in ["asc", "asc2"]:
                         extracted_files.extend(self._extract_nested_zip(extracted_path, target_dir))
                    else:
                        extracted_files.append(extracted_path)

            if not extracted_files:
                raise ProviderError("AFAD", None, "ZIP archive did not contain extractable files")

        except Exception as e:
            raise ProviderError("AFAD", e, f"Extraction failed: {zip_path}")
        finally:
            # İşlem bitince zip'i sil
            if os.path.exists(zip_path):
                os.remove(zip_path)
                
        return extracted_files

    def _extract_nested_zip(self, nested_zip_path: str, target_dir: str) -> List[str]:
        files = []
        try:
            if not zipfile.is_zipfile(nested_zip_path):
                raise ProviderError("AFAD", None, "Nested archive is not a valid ZIP")
            with zipfile.ZipFile(nested_zip_path, 'r') as nz:
                if nz.testzip() is not None:
                    raise ProviderError("AFAD", None, "Corrupted nested ZIP file")
                for name in nz.namelist():
                    self._ensure_safe_member(name)
                nz.extractall(target_dir)
                files = [
                    os.path.join(target_dir, n)
                    for n in nz.namelist()
                    if not n.endswith("/")
                ]
            os.remove(nested_zip_path)
        except Exception as e:
            raise ProviderError("AFAD", e, f"Nested extraction failed: {nested_zip_path}")
        return files

    def _ensure_safe_member(self, member_name: str) -> None:
        normalized = os.path.normpath(member_name)
        if os.path.isabs(normalized) or normalized.startswith(".."):
            raise ProviderError("AFAD", None, f"Unsafe ZIP member path: {member_name}")
