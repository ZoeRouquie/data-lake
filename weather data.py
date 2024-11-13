import cdsapi
import pandas as pd
# Initialize the client
client = cdsapi.Client()

dataset = "derived-era5-land-daily-statistics"
request = {
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
    "frequency": "1_hourly"
}


accident = pd.read_csv('Road Accident Data.csv')
accident = accident.head(100)
accident['Accident Date'] = pd.to_datetime(accident['Accident Date'], errors='coerce')





for i, row in accident.iterrows():
    latitude, longitude = row['Latitude'], row['Longitude']
    
    area = [latitude + 0.1, longitude - 0.1, latitude - 0.1, longitude + 0.1]
    
    request = request.copy()
    request["day"] = row['Accident Date'].day
    request['month'] = row['Accident Date'].month
    request['year'] = row['Accident Date'].year
    
    request["area"] = area
    
    # Retrieve data for each location
    client.retrieve(dataset, request).download(f"data_point_{i}.grib")