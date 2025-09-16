"""Basit test dosyası - pytest-cov hatasını test etmek için"""

def test_basic_import():
    """Temel import testi"""
    try:
        # Basit import testi
        import sys
        sys.path.insert(0, './src')
        from selection_service import Config
        assert True
    except ImportError as e:
        assert False, f"Import hatası: {e}"

def test_basic_arithmetic():
    """Temel aritmetik testi"""
    assert 1 + 1 == 2

def test_list_operations():
    """Liste operasyonları testi"""
    test_list = [1, 2, 3]
    test_list.append(4)
    assert len(test_list) == 4
    assert test_list[-1] == 4