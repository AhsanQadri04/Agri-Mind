# tests/test_farm_efficiency_tool.py

import pytest
import pandas as pd
from skills.farm_efficiency_tool import calculate_resource_efficiency

# Setup: Create a small sample DataFrame matching your farm_resources schema
@pytest.fixture(scope="module")
def sample_farm_resources(tmp_path, monkeypatch):
    # Create sample parquet file in a temporary directory
    df = pd.DataFrame([
        {
            "farm_id": "F-TEST",
            "irrigation_hours_per_week": 20,
            "fertilizer_kg_available": 100,
            "neighboring_farms": ["F-A","F-B"],
            "tractor_hours": 5,
            "harvester_hours": 2
        }
    ])
    file_path = tmp_path / "farm_resources_test.parquet"
    df.to_parquet(file_path)
    # Monkeypatch the module’s dataset path or DataFrame load
    import skills.farm_efficiency_tool as tool_mod
    tool_mod._FARM_RESOURCES_DF = df  # override the in-memory DataFrame
    return df

def test_calculate_resource_efficiency_valid(sample_farm_resources):
    result = calculate_resource_efficiency("F-TEST")
    assert isinstance(result, dict)
    assert result["status"] == "success"
    assert result["farm_id"] == "F-TEST"
    assert "efficiency_score" in result
    # For our example: efficiency_score = 100 / (20+5+2+1) = 100 / 28 ≈ 3.571
    expected = 100 / (20 + 5 + 2 + 1)
    assert pytest.approx(result["efficiency_score"], rel=1e-3) == expected
    assert "details" in result
    assert result["details"]["irrigation_hours_per_week"] == 20
    assert result["details"]["fertilizer_kg_available"] == 100

def test_calculate_resource_efficiency_invalid_farm():
    result = calculate_resource_efficiency("NON_EXISTENT")
    assert isinstance(result, dict)
    assert result["status"] == "error"
    assert "error_message" in result

