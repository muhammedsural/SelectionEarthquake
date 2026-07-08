# tests/test_AfadApiClient.py

import pytest
import asyncio
import aiohttp
import requests
from unittest.mock import MagicMock, patch, AsyncMock, Mock
from selection_service.providers.afad.AfadApiClient import AfadApiClient
from selection_service.core.ErrorHandle import NetworkError


class TestAfadApiClientInit:
    """AfadApiClient Initialization Tests"""

    def test_init_default_timeout(self):
        """Test initialization with default timeout"""
        client = AfadApiClient()
        assert client.timeout == 30
        assert client.BASE_URL == "https://ivmeservis.afad.gov.tr"
        assert client.PROCESS_URL == "https://ivmeprocessguest.afad.gov.tr"

    def test_init_custom_timeout(self):
        """Test initialization with custom timeout"""
        client = AfadApiClient(timeout=60)
        assert client.timeout == 60

    def test_init_headers(self):
        """Test that headers are properly initialized"""
        client = AfadApiClient()
        assert client.headers['Accept'] == 'application/json, text/plain, */*'
        assert client.headers['Content-Type'] == 'application/json'
        assert client.headers['Username'] == 'GuestUser'
        assert client.headers['IsGuest'] == 'true'
        assert 'User-Agent' in client.headers
        assert 'Origin' in client.headers
        assert 'Referer' in client.headers


class TestSearchWaveformsAsync:
    """Async waveform search tests"""

    # @pytest.mark.asyncio
    # async def test_search_waveforms_async_success(self):
    #     """Test successful async search"""
    #     client = AfadApiClient()
    #     expected_response = {"status": "success", "data": [{"id": 1}]}
        
    #     with patch('aiohttp.ClientSession') as mock_session_class:
    #         mock_response = AsyncMock()
    #         mock_response.status = 200
    #         mock_response.json = AsyncMock(return_value=expected_response)
            
    #         mock_ctx_mgr = AsyncMock()
    #         mock_ctx_mgr.__aenter__.return_value = mock_response
    #         mock_ctx_mgr.__aexit__.return_value = None
            
    #         mock_session = AsyncMock()
    #         mock_session.post.return_value = mock_ctx_mgr
            
    #         mock_session_class.return_value.__aenter__.return_value = mock_session
    #         mock_session_class.return_value.__aexit__.return_value = None
            
    #         result = await client.search_waveforms_async({"magnitude": 5.0})
            
    #         assert result == expected_response

    @pytest.mark.asyncio
    async def test_search_waveforms_async_http_error_401(self):
        """Test async search with HTTP 401 error"""
        client = AfadApiClient()
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 401
            mock_response.text = AsyncMock(return_value="Unauthorized")
            
            mock_ctx_mgr = AsyncMock()
            mock_ctx_mgr.__aenter__.return_value = mock_response
            mock_ctx_mgr.__aexit__.return_value = None
            
            mock_session = AsyncMock()
            mock_session.post.return_value = mock_ctx_mgr
            
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            with pytest.raises(NetworkError):
                await client.search_waveforms_async({"magnitude": 5.0})

    @pytest.mark.asyncio
    async def test_search_waveforms_async_http_error_500(self):
        """Test async search with HTTP 500 error"""
        client = AfadApiClient()
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 500
            mock_response.text = AsyncMock(return_value="Server Error")
            
            mock_ctx_mgr = AsyncMock()
            mock_ctx_mgr.__aenter__.return_value = mock_response
            mock_ctx_mgr.__aexit__.return_value = None
            
            mock_session = AsyncMock()
            mock_session.post.return_value = mock_ctx_mgr
            
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            with pytest.raises(NetworkError):
                await client.search_waveforms_async({})

    @pytest.mark.asyncio
    async def test_search_waveforms_async_timeout_exception(self):
        """Test async search with timeout exception"""
        client = AfadApiClient(timeout=1)
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session.post.side_effect = asyncio.TimeoutError("Timeout")
            
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            with pytest.raises(NetworkError) as exc_info:
                await client.search_waveforms_async({})
            
            assert "Async search failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_search_waveforms_async_connection_error(self):
        """Test async search with connection error"""
        client = AfadApiClient()
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session.post.side_effect = aiohttp.ClientError("Connection failed")
            
            mock_session_class.return_value.__aenter__.return_value = mock_session
            mock_session_class.return_value.__aexit__.return_value = None
            
            with pytest.raises(NetworkError) as exc_info:
                await client.search_waveforms_async({})
            
            assert "Async search failed" in str(exc_info.value)


class TestSearchWaveformsSync:
    """Sync waveform search tests"""

    def test_search_waveforms_sync_success(self):
        """Test successful sync search"""
        client = AfadApiClient()
        expected_response = {"status": "success", "data": [{"id": 1}]}
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = expected_response
            mock_post.return_value = mock_response
            
            result = client.search_waveforms_sync({"magnitude": 5.0})
            
            assert result == expected_response
            mock_post.assert_called_once()
            # Verify correct URL was called
            call_args = mock_post.call_args
            assert "GetWaveforms" in call_args[0][0]
            assert call_args.kwargs["json"] == {"magnitude": 5.0}

    def test_search_waveforms_sync_sends_fault_type_payload(self):
        """AFAD faultType filtresi HTTP payload icinde korunmali."""
        client = AfadApiClient()
        expected_response = [{"eventID": 1}]
        criteria = {
            "minMagnitude": 6.0,
            "maxMagnitude": 7.0,
            "faultType": "SS",
        }

        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = expected_response
            mock_post.return_value = mock_response

            result = client.search_waveforms_sync(criteria)

            assert result == expected_response
            assert mock_post.call_args.kwargs["json"] == criteria

    def test_search_waveforms_sync_http_error_400(self):
        """Test sync search with HTTP 400 error"""
        client = AfadApiClient()
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 400
            mock_response.text = "Bad Request"
            mock_post.return_value = mock_response
            
            with pytest.raises(NetworkError) as exc_info:
                client.search_waveforms_sync({})
            
            assert "Sync search failed" in str(exc_info.value)

    def test_search_waveforms_sync_http_error_403(self):
        """Test sync search with HTTP 403 error"""
        client = AfadApiClient()
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 403
            mock_response.text = "Forbidden"
            mock_post.return_value = mock_response
            
            with pytest.raises(NetworkError):
                client.search_waveforms_sync({})

    def test_search_waveforms_sync_timeout(self):
        """Test sync search with timeout"""
        client = AfadApiClient(timeout=5)
        
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.Timeout("Connection timeout")
            
            with pytest.raises(NetworkError) as exc_info:
                client.search_waveforms_sync({})
            
            assert "Sync search failed" in str(exc_info.value)

    def test_search_waveforms_sync_connection_error(self):
        """Test sync search with connection error"""
        client = AfadApiClient()
        
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.ConnectionError("Connection refused")
            
            with pytest.raises(NetworkError) as exc_info:
                client.search_waveforms_sync({})
            
            assert "Sync search failed" in str(exc_info.value)

    def test_search_waveforms_sync_request_exception(self):
        """Test sync search with general request exception"""
        client = AfadApiClient()
        
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.RequestException("General error")
            
            with pytest.raises(NetworkError):
                client.search_waveforms_sync({})


class TestDownloadWaveform:
    """Waveform download tests"""

    def test_download_waveform_success(self):
        """Test successful waveform download"""
        client = AfadApiClient()
        expected_content = b"binary_waveform_data"
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.content = expected_content
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response
            
            payload = {
                "EventId": 12345,
                "StationId": 67890,
                "ExportType": "asc2"
            }
            result = client.download_waveform(payload)
            
            assert result == expected_content
            mock_post.assert_called_once()
            # Verify correct URL was called
            call_args = mock_post.call_args
            assert "ExportData" in call_args[0][0]

    def test_download_waveform_http_error_not_found(self):
        """Test waveform download with 404 error"""
        client = AfadApiClient()
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
            mock_post.return_value = mock_response
            
            with pytest.raises(NetworkError) as exc_info:
                client.download_waveform({"EventId": 99999})
            
            assert "Download request failed" in str(exc_info.value)

    def test_download_waveform_timeout(self):
        """Test waveform download with timeout"""
        client = AfadApiClient(timeout=50)
        
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.Timeout("Request timeout")
            
            with pytest.raises(NetworkError) as exc_info:
                client.download_waveform({})
            
            assert "Download request failed" in str(exc_info.value)

    def test_download_waveform_connection_error(self):
        """Test waveform download with connection error"""
        client = AfadApiClient()
        
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.ConnectionError("Network down")
            
            with pytest.raises(NetworkError):
                client.download_waveform({})

    def test_download_waveform_large_file(self):
        """Test downloading large waveform file"""
        client = AfadApiClient()
        large_content = b"x" * (10 * 1024 * 1024)  # 10MB
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.content = large_content
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response
            
            result = client.download_waveform({"EventId": 1})
            
            assert len(result) == len(large_content)
            assert result == large_content


class TestGetEventDetails:
    """Event details retrieval tests"""

    def test_get_event_details_success(self):
        """Test successful event details retrieval"""
        client = AfadApiClient()
        expected_data = {
            "id": 12345,
            "magnitude": 6.5,
            "latitude": 37.5,
            "longitude": 35.5
        }
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = expected_data
            mock_get.return_value = mock_response
            
            result = client.get_event_details(12345)
            
            assert result == expected_data
            mock_get.assert_called_once()
            # Verify correct URL
            call_args = mock_get.call_args
            assert "GetEventById/12345" in call_args[0][0]
            # Verify referer was updated
            headers = call_args[1]['headers']
            assert "event-detail/12345" in headers['Referer']

    def test_get_event_details_not_found(self):
        """Test event details with 404 error"""
        client = AfadApiClient()
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response
            
            result = client.get_event_details(99999)
            
            assert result is None

    def test_get_event_details_server_error(self):
        """Test event details with server error"""
        client = AfadApiClient()
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_get.return_value = mock_response
            
            result = client.get_event_details(12345)
            
            assert result is None

    def test_get_event_details_timeout(self):
        """Test event details with timeout"""
        client = AfadApiClient(timeout=10)
        
        with patch('requests.get') as mock_get:
            mock_get.side_effect = requests.Timeout("Request timeout")
            
            with pytest.raises(NetworkError) as exc_info:
                client.get_event_details(12345)
            
            assert "Event detail fetch failed" in str(exc_info.value)
            assert "12345" in str(exc_info.value)

    def test_get_event_details_connection_error(self):
        """Test event details with connection error"""
        client = AfadApiClient()
        
        with patch('requests.get') as mock_get:
            mock_get.side_effect = requests.ConnectionError("Connection refused")
            
            with pytest.raises(NetworkError) as exc_info:
                client.get_event_details(12345)
            
            assert "Event detail fetch failed for 12345" in str(exc_info.value)

    def test_get_event_details_json_decode_error(self):
        """Test event details with JSON decode error"""
        client = AfadApiClient()
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_get.return_value = mock_response
            
            with pytest.raises(NetworkError) as exc_info:
                client.get_event_details(12345)
            
            assert "Event detail fetch failed" in str(exc_info.value)

    def test_get_event_details_multiple_events(self):
        """Test retrieving details for multiple events"""
        client = AfadApiClient()
        
        event_ids = [1001, 1002, 1003]
        expected_results = [
            {"id": 1001, "magnitude": 5.0},
            {"id": 1002, "magnitude": 6.0},
            {"id": 1003, "magnitude": 5.5}
        ]
        
        with patch('requests.get') as mock_get:
            # Setup different responses for each call
            mock_responses = []
            for expected in expected_results:
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                mock_resp.json.return_value = expected
                mock_responses.append(mock_resp)
            
            mock_get.side_effect = mock_responses
            
            results = [client.get_event_details(eid) for eid in event_ids]
            
            assert results == expected_results
            assert mock_get.call_count == 3


class TestAfadApiClientEdgeCases:
    """Edge cases and integration tests"""

    def test_timeout_propagation(self):
        """Verify timeout is properly passed to requests"""
        client = AfadApiClient(timeout=45)
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {}
            mock_post.return_value = mock_response
            
            client.search_waveforms_sync({})
            
            # Verify timeout parameter
            call_kwargs = mock_post.call_args[1]
            assert call_kwargs['timeout'] == 45

    def test_download_request_timeout_propagation(self):
        """Verify download timeout is set to 50"""
        client = AfadApiClient(timeout=30)
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.content = b"data"
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response
            
            client.download_waveform({})
            
            # Download should use hardcoded 50 second timeout
            call_kwargs = mock_post.call_args[1]
            assert call_kwargs['timeout'] == 50

    def test_headers_consistency(self):
        """Verify headers don't get modified after initialization"""
        client = AfadApiClient()
        original_headers = client.headers.copy()
        
        # Simulate a search that might modify headers
        with patch('requests.post'):
            try:
                client.search_waveforms_sync({})
            except:
                pass
        
        # Original headers should remain unchanged via the copy made for get_event_details
        # But search_waveforms_sync shouldn't modify instance headers
        assert client.headers == original_headers

    def test_get_event_details_headers_preservation(self):
        """Verify get_event_details doesn't modify instance headers"""
        client = AfadApiClient()
        original_referer = client.headers['Referer']
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {}
            mock_get.return_value = mock_response
            
            client.get_event_details(123)
            
            # Original headers should be unchanged
            assert client.headers['Referer'] == original_referer

    def test_empty_criteria_handling(self):
        """Test handling of empty search criteria"""
        client = AfadApiClient()
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = []
            mock_post.return_value = mock_response
            
            result = client.search_waveforms_sync({})
            
            assert result == []

    def test_special_characters_in_payload(self):
        """Test handling of special characters in payload"""
        client = AfadApiClient()
        
        payload = {
            "EventId": 12345,
            "Description": "Test with ñ, é, ü characters"
        }
        
        with patch('requests.post') as mock_post:
            mock_response = MagicMock()
            mock_response.content = b"data"
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response
            
            result = client.download_waveform(payload)
            
            # Should handle special characters without error
            assert result == b"data"
            mock_post.assert_called_once()

    # @pytest.mark.asyncio
    # async def test_async_session_context_manager_cleanup(self):
    #     """Verify async session is properly closed"""
    #     client = AfadApiClient()
        
    #     with patch('aiohttp.ClientSession') as mock_session_class:
    #         mock_response = AsyncMock()
    #         mock_response.status = 200
    #         mock_response.json = AsyncMock(return_value={})
            
    #         mock_ctx_mgr = AsyncMock()
    #         mock_ctx_mgr.__aenter__.return_value = mock_response
    #         mock_ctx_mgr.__aexit__.return_value = None
            
    #         mock_session = AsyncMock()
    #         mock_session.post.return_value = mock_ctx_mgr
            
    #         session_ctx = AsyncMock()
    #         session_ctx.__aenter__.return_value = mock_session
    #         session_ctx.__aexit__.return_value = None
            
    #         mock_session_class.return_value = session_ctx
            
    #         await client.search_waveforms_async({})
            
    #         # Verify context manager was entered and exited
    #         session_ctx.__aenter__.assert_called_once()
    #         session_ctx.__aexit__.assert_called_once()
