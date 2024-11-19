import cdsapi
import pandas as pd
import numpy as np
from datetime import datetime
# Initialize the client
client = cdsapi.Client()

# Dataset and base request parameters
dataset = "derived-era5-land-daily-statistics"
base_request = {
    "variable": [
        "2m_dewpoint_temperature",
        "2m_temperature",
        "skin_temperature",
        "soil_temperature_level_1",
        "soil_temperature_level_2",
        "soil_temperature_level_3",
        "soil_temperature_level_4",
        "snow_albedo",
        "snow_cover",
        "snow_density",
        "snow_depth",
        "snow_depth_water_equivalent",
        "temperature_of_snow_layer",
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "surface_pressure",
        "leaf_area_index_high_vegetation",
        "leaf_area_index_low_vegetation",
        "forecast_albedo"
    ],
    "daily_statistic": "daily_mean",
    "time_zone": "utc+00:00",
    "frequency": "6_hourly"
}

# Load accident data
accident = pd.read_csv('Road Accident Data.csv')
sample_size = int((len(accident)/3) * 0.00001) 
print('***************************************')# 1% du total des data
print(sample_size)
print('***************************************')# 1% du total des data

accident = accident.sample(n=sample_size, random_state=42)

accident['Accident Date'] = pd.to_datetime(accident['Accident Date'], errors='coerce')
print('***************************************')# 1% du total des data
print(accident.head())
print('***************************************')# 1% du total des data

# Determine the bounding box for all points
latitudes = accident['Latitude']
longitudes = accident['Longitude']


# Diviser la zone globale en sous-zones
latitude_range = np.arange(latitudes.min(), latitudes.max(), 1)  # Par incréments de 1 degré
longitude_range = np.arange(longitudes.min(), longitudes.max(), 1)

# Générer des sous-zones (latitude_min, longitude_min, latitude_max, longitude_max)
sub_areas = [
    [lat, lon, lat + 1, lon + 1]
    for lat in latitude_range
    for lon in longitude_range
]

start_date = accident['Accident Date'].min().strftime('%Y-%m-%d')
end_date = accident['Accident Date'].max().strftime('%Y-%m-%d')
base_request["date"] = f"{start_date}/{end_date}"

# Faire une requête pour chaque sous-zone
for i, area in enumerate(sub_areas):
    base_request["area"] = area
    time_start = datetime.now()
    client.retrieve(dataset, base_request).download(f"data_sub_area_{i}.grib")
    time_end = datetime.now()
    print('***************************************')# 1% du total des data
    print(time_end - time_start, ' minutes ou secondes')
    print('***************************************')# 1% du total des data
    

# Retrieve all data in a single request
client.retrieve(dataset, base_request).download("combined_data.grib")
