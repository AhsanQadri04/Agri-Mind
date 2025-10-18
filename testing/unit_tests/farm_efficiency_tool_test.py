# tests/test_farm_efficiency_tool.py

import pandas as pd
import pytest
from farm_swarm.skills.farm_efficiency_tool import calculate_resource_efficiency
import os
from dotenv import load_dotenv
load_dotenv()

# Use sample data or load a small portion of your real dataset
@pytest.fixture(scope="function")
def sample_data():
    # Copy or load the real parquet file
    df = pd.read_parquet(os.getenv("FRP_CLEAN"))
    return df

def test_calculate_efficiency_valid_farm(sample_data):
    # Pick a known farm_id from sample_data
    farm_id = sample_data["farm_id"].iloc[0]
    result = calculate_resource_efficiency(farm_id)
    # Validate fields
    assert result["status"] == "success"
    assert result["farm_id"] == farm_id
    assert "efficiency_score" in result
    # Efficiency should be positive
    assert result["efficiency_score"] > 0
    details = result["details"]
    assert details["irrigation_hours_per_week"] == sample_data.loc[sample_data["farm_id"]==farm_id, "irrigation_hours_per_week"].iloc[0]
    # Other fields exist
    assert "fertilizer_kg_available" in details

def test_calculate_efficiency_invalid_farm(sample_data):
    result = calculate_resource_efficiency("NON_EXISTENT_FARM_ID")
    assert result["status"] == "error"
    assert "error_message" in result

