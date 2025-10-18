import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


def cleanFarmResources():
    # Read Parquet file
    data = pd.read_parquet(os.getenv("FARM_RESOURCE_PARQUET"))

    if 'equipment_availability' in data.columns:
        data['tractor_hours'] = data['equipment_availability'].apply(
            lambda x: x.get('tractor_hours', 0) if isinstance(x, dict) else None
        )
        data['harvester_hours'] = data['equipment_availability'].apply(
            lambda x: x.get('harvester_hours', 0) if isinstance(x, dict) else None
        )

        # Optionally remove the original dict column
        data.drop(columns=['equipment_availability'], inplace=True)

    data.to_parquet(os.getenv("FRP_CLEAN"))
    print("Resource Data Cleaned and Saved!")


def cleanFarmSensor():
    # Read Parquet file
    data = pd.read_parquet(os.getenv("FARM_SENSOR_PARQUET"))

    # Rename columns
    rename_map = {
        'soil_moisture_%': 'soil_moisture_percent',
        'humidity_%': 'humidity_percent'
    }
    data.rename(columns=rename_map, inplace=True)

    print("Sensor Data Cleaned and Saved!")
    data.to_parquet(os.getenv("FSP_CLEAN"))


def cleanMarketPrice():
    # Read Parquet file
    data = pd.read_parquet(os.getenv("MARKET_PRICE_PARQUET"))

    # Convert date column to datetime
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'], errors='coerce')

    print("Market Data Cleaned and Saved!")
    data.to_parquet(os.getenv("MP_CLEAN"))


def cleanWeather():
    # Read Parquet file
    data = pd.read_parquet(os.getenv("WEATHER_PARQUET"))

    # Convert date column to datetime
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'], errors='coerce')

    # Rename humidity column
    if 'humidity_%' in data.columns:
        data.rename(columns={'humidity_%': 'humidity_percent'}, inplace=True)

    print("Weather Data Cleaned and Saved!")
    data.to_parquet(os.getenv("W_CLEAN"))

def main():
    cleanFarmResources()
    cleanFarmSensor()
    cleanMarketPrice()
    cleanWeather()

if __name__ == "__main__":
    main()