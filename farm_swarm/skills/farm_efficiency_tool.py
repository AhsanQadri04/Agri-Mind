# skills/farm_efficiency_tool.py

from typing import Dict, List
import pandas as pd
import os
from google.adk.tools.function_tool import FunctionTool  # hypothetical import based on ADK docs
# Note: Confirm correct import path: ADK docs show tools are in `google.adk.tools` :contentReference[oaicite:2]{index=2}

from dotenv import load_dotenv
load_dotenv()

# Load the dataset once (or through a shared utility)
_FARM_RESOURCES_DF = pd.read_parquet(os.getenv("FRP_CLEAN"))

def calculate_resource_efficiency(farm_id: str) -> Dict:
    """
    Calculates a resource efficiency ratio for the given farm.
    Args:
        farm_id (str): The ID of the farm, e.g., "F-001".
    Returns:
        Dict: {
            "status": "success",
            "farm_id": <farm_id>,
            "efficiency_score": <float>,
            "details": { ... }
        }
        On error:
        {
            "status": "error",
            "error_message": <str>
        }
    """
    try:
        df = _FARM_RESOURCES_DF
        row = df[df["farm_id"] == farm_id]
        if row.empty:
            return {"status": "error", "error_message": f"No farm found for id {farm_id}"}

        # extract values
        irrigation = int(row["irrigation_hours_per_week"].iloc[0])
        fertilizer = int(row["fertilizer_kg_available"].iloc[0])
        tractor = int(row["tractor_hours"].iloc[0])
        harvester = int(row["harvester_hours"].iloc[0])

        # Example heuristic: efficiency_score = (fertilizer / (irrigation + tractor + harvester + 1))
        efficiency_score = fertilizer / float(irrigation + tractor + harvester + 1)

        details = {
            "irrigation_hours_per_week": irrigation,
            "fertilizer_kg_available": fertilizer,
            "tractor_hours": tractor,
            "harvester_hours": harvester,
            "efficiency_formula": "fertilizer / (irrigation + tractor + harvester + 1)"
        }

        return {
            "status": "success",
            "farm_id": farm_id,
            "efficiency_score": efficiency_score,
            "details": details
        }

    except Exception as e:
        return {"status": "error", "error_message": str(e)}

# Wrap the function as a FunctionTool
calculate_resource_efficiency_tool = FunctionTool(
    func=calculate_resource_efficiency,
    require_confirmation=False
)


def compare_with_neighbors(farm_id: str) -> Dict:
    """
    Compares the given farm’s resource metrics with its neighboring farms.
    Args:
        farm_id (str): The ID of the farm.
    Returns:
        Dict: {
            "status": "success",
            "farm_id": <farm_id>,
            "neighbor_ids": [...],
            "comparison": {
                "this_farm": { ... },
                "neighbors_avg": { ... },
                "difference": { ... }
            }
        }
    """
    try:
        df = _FARM_RESOURCES_DF
        row = df[df["farm_id"] == farm_id]
        if row.empty:
            return {"status": "error", "error_message": f"No farm found for id {farm_id}"}

        neighbor_list = row["neighboring_farms"].iloc[0]
        if not isinstance(neighbor_list, list):
            # if stored as string, parse accordingly
            neighbor_list = eval(neighbor_list)

        this_vals = {
            "irrigation_hours_per_week": int(row["irrigation_hours_per_week"].iloc[0]),
            "fertilizer_kg_available": int(row["fertilizer_kg_available"].iloc[0]),
            "tractor_hours": int(row["tractor_hours"].iloc[0]),
            "harvester_hours": int(row["harvester_hours"].iloc[0]),
        }

        neighbor_rows = df[df["farm_id"].isin(neighbor_list)]
        if neighbor_rows.empty:
            return {"status": "error", "error_message": f"No neighbors found for farm id {farm_id}"}

        neighbors_avg = {
            "irrigation_hours_per_week": float(neighbor_rows["irrigation_hours_per_week"].mean()),
            "fertilizer_kg_available": float(neighbor_rows["fertilizer_kg_available"].mean()),
            "tractor_hours": float(neighbor_rows["tractor_hours"].mean()),
            "harvester_hours": float(neighbor_rows["harvester_hours"].mean()),
        }

        diff = {
            "irrigation_diff": this_vals["irrigation_hours_per_week"] - neighbors_avg["irrigation_hours_per_week"],
            "fertilizer_diff": this_vals["fertilizer_kg_available"] - neighbors_avg["fertilizer_kg_available"],
            "tractor_diff": this_vals["tractor_hours"] - neighbors_avg["tractor_hours"],
            "harvester_diff": this_vals["harvester_hours"] - neighbors_avg["harvester_hours"],
        }

        return {
            "status": "success",
            "farm_id": farm_id,
            "neighbor_ids": neighbor_list,
            "comparison": {
                "this_farm": this_vals,
                "neighbors_avg": neighbors_avg,
                "difference": diff
            }
        }

    except Exception as e:
        return {"status": "error", "error_message": str(e)}

compare_with_neighbors_tool = FunctionTool(
    func=compare_with_neighbors,
    require_confirmation=False
)
