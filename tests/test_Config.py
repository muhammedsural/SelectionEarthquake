import pytest
import pandas as pd
from selection_service.core.Config import convert_mechanism_to_text, MECHANISM_MAP
from selection_service.core.Config import convert_mechanism_to_numeric, REVERSE_MECHANISM_MAP
from selection_service.core.Config import get_mechanism_text, MECHANISM_MAP
from selection_service.core.Config import get_mechanism_numeric, REVERSE_MECHANISM_MAP

def test_convert_mechanism_to_text_basic():
    df = pd.DataFrame({'MECHANISM': [0, 1, 2, 3, 4, 5, -999]})
    result = convert_mechanism_to_text(df)
    expected = [MECHANISM_MAP[v] for v in [0, 1, 2, 3, 4, 5, -999]]
    assert result['MECHANISM'].tolist() == expected

def test_convert_mechanism_to_text_with_unknown():
    df = pd.DataFrame({'MECHANISM': [42, -999]})
    result = convert_mechanism_to_text(df)
    assert result['MECHANISM'].iloc[0] == 'Unknown'
    assert result['MECHANISM'].iloc[1] == 'Unknown'

def test_convert_mechanism_to_text_custom_column():
    df = pd.DataFrame({'MECH': [0, 1, 2]})
    result = convert_mechanism_to_text(df, mechanism_col='MECH')
    expected = [MECHANISM_MAP[v] for v in [0, 1, 2]]
    assert result['MECH'].tolist() == expected

def test_convert_mechanism_to_text_does_not_modify_original():
    df = pd.DataFrame({'MECHANISM': [0, 1]})
    df_copy = df.copy()
    _ = convert_mechanism_to_text(df)
    assert df.equals(df_copy)

def test_convert_mechanism_to_numeric_basic():
        mechanisms = ['StrikeSlip', 'Normal', 'Reverse', 'Reverse/Oblique', 'Normal/Oblique', 'Oblique', 'Unknown']
        df = pd.DataFrame({'MECHANISM': mechanisms})
        result = convert_mechanism_to_numeric(df)
        expected = [REVERSE_MECHANISM_MAP[m] for m in mechanisms]
        assert result['MECHANISM'].tolist() == expected

def test_convert_mechanism_to_numeric_with_unknown():
        df = pd.DataFrame({'MECHANISM': ['NotAType', 'Unknown']})
        result = convert_mechanism_to_numeric(df)
        assert result['MECHANISM'].iloc[0] == -999
        assert result['MECHANISM'].iloc[1] == -999

def test_convert_mechanism_to_numeric_custom_column():
        mechanisms = ['StrikeSlip', 'Normal', 'Reverse']
        df = pd.DataFrame({'MECH': mechanisms})
        result = convert_mechanism_to_numeric(df, mechanism_col='MECH')
        expected = [REVERSE_MECHANISM_MAP[m] for m in mechanisms]
        assert result['MECH'].tolist() == expected

def test_convert_mechanism_to_numeric_does_not_modify_original():
        df = pd.DataFrame({'MECHANISM': ['StrikeSlip', 'Normal']})
        df_copy = df.copy()
        _ = convert_mechanism_to_numeric(df)
        assert df.equals(df_copy)


@pytest.mark.parametrize("numeric_value,expected", [
            (0, 'StrikeSlip'),
            (1, 'Normal'),
            (2, 'Reverse'),
            (3, 'Reverse/Oblique'),
            (4, 'Normal/Oblique'),
            (5, 'Oblique'),
            (-999, 'Unknown'),
            (42, 'Unknown'),
            (None, 'Unknown'),
        ])
def test_get_mechanism_text(numeric_value, expected):
            assert get_mechanism_text(numeric_value) == expected


@pytest.mark.parametrize("text_value,expected", [
                ('StrikeSlip', 0),
                ('Normal', 1),
                ('Reverse', 2),
                ('Reverse/Oblique', 3),
                ('Normal/Oblique', 4),
                ('Oblique', 5),
                ('Unknown', -999),
                ('NotAType', -999),
                ('', -999),
                (None, -999),
            ])
def test_get_mechanism_numeric(text_value, expected):
        assert get_mechanism_numeric(text_value) == expected



