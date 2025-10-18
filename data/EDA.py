import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

OUTPUT = "C:/Programming/Innovista/output"

def dataInfo(path):
    head, extention = os.path.splitext(path)
    if extention == ".json":
        data = pd.read_json(path)
    elif extention == ".csv":
        data = pd.read_csv(path)
    elif extention == ".parquet":
        data = pd.read_parquet(path)
    else:
        print("This extention is not supported")

    print("OVERALL DATASET")
    print(f"\n\n{head} SAMPLE RECORDS")
    print(data.head(7))
    print(f"\n\n{head} INFO")
    print(data.info())
    data.to_parquet(f"{head}.parquet")
    print(f"{head} converted to parquet and saved!")

def sensorDataEDA():
    data = pd.read_parquet(os.getenv("FARM_SENSOR_PARQUET"))
    print("Unique Tehsils")
    print(data["tehsil"].unique())
    print("Unique Districts")
    print(data["district"].unique())
    print("Unique Pests")
    print(data["pest_detection"].unique())
    print("Max Soil Moisture")
    print(data["soil_moisture_%"].max())
    print("Max Temprature")
    print(data["temperature_c"].max())
    print("Max Humidity")
    print(data["humidity_%"].max())


def marketPriceEDA():
    data = pd.read_parquet(os.getenv("MARKET_PRICE_PARQUET"))
    print("Unique Locations")
    print(data["market_location"].unique())

def weatherEDA():
    data = pd.read_parquet(os.getenv("WEATHER_PARQUET"))
    print("Unique Tehsils")
    print(data["tehsil"].unique())
    print("Unique Districts")
    print(data["district"].unique())

def main():
    datasets = [
        os.getenv("FRP_CLEAN"),
        os.getenv("FSP_CLEAN"),
        os.getenv("MP_CLEAN"),
        os.getenv("W_CLEAN")
    ]

    for dataset in datasets:
        if not dataset:
            print("⚠️ Skipping — environment variable not set.")
            continue
        dataInfo(dataset)

    print("\nDATASET SPECIFIC EDA\nSENSOR DATA")
    sensorDataEDA()
    print("\n\MARKET DATA")
    marketPriceEDA()
    print("\n\nWEATHER DATA")
    weatherEDA()


if __name__ == "__main__":
    main()