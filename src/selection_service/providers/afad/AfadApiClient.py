# providers/afad/AfadApiClient.py
import inspect
import aiohttp
import requests
from typing import Dict, Any, Optional
from selection_service.core.ErrorHandle import NetworkError

class AfadApiClient:
    """AFAD API isteklerini yöneten istemci sınıfı"""
    
    BASE_URL = "https://ivmeservis.afad.gov.tr"
    PROCESS_URL = "https://ivmeprocessguest.afad.gov.tr"
    
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://tadas.afad.gov.tr',
            'Referer': 'https://tadas.afad.gov.tr/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Username': 'GuestUser',
            'IsGuest': 'true'
        }

    async def search_waveforms_async(self, criteria: Dict[str, Any]) -> Dict:
        url = f"{self.BASE_URL}/Waveforms/GetWaveforms"
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                request = session.post(url, json=criteria, timeout=self.timeout)
                if inspect.isawaitable(request):
                    request = await request
                async with request as response:
                    if response.status == 200:
                        response = await response.json()
                        if response is None or len(response) == 0:
                            raise NetworkError("AFAD", Exception("Empty response"), "AFAD returned empty data")
                        return response
                    error_text = await response.text()
                    raise NetworkError("AFAD", Exception(f"HTTP {response.status}: {error_text}"))
        except Exception as e:
            raise NetworkError("AFAD", e, "Async search failed")

    def search_waveforms_sync(self, criteria: Dict[str, Any]) -> Dict:
        url = f"{self.BASE_URL}/Waveforms/GetWaveforms"
        try:
            response = requests.post(url, json=criteria, headers=self.headers, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            raise NetworkError("AFAD", Exception(f"HTTP {response.status_code}: {response.text}"))
        except Exception as e:
            raise NetworkError("AFAD", e, "Sync search failed")

    def download_waveform(self, payload: Dict[str, Any]) -> bytes:
        """Dalga formu indirme isteği
            payload örneği:
            {
                "EventId": 12345,
                "StationId": 67890,
                "ExportType": "asc2"
            }
        """
        url = f"{self.PROCESS_URL}/ExportData"
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=50)
            response.raise_for_status()
            return response.content
        except Exception as e:
            raise NetworkError("AFAD", e, "Download request failed")

    def get_event_details(self, event_id: int) -> Optional[Dict]:
        url = f"{self.BASE_URL}/Event/GetEventById/{event_id}"
        # Event detail için referer değişiyor, kopyasını alıp güncelle
        headers = self.headers.copy()
        headers['Referer'] = f'https://tadas.afad.gov.tr/event-detail/{event_id}'
        
        try:
            response = requests.get(url, headers=headers, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            raise NetworkError("AFAD", e, f"Event detail fetch failed for {event_id}")
