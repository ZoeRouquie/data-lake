import cdsapi
import pandas as pd
import time

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

# Optionally sample a subset for testing (e.g., 1%)
sample_size = int((len(accident)/3) * 0.00001)
print('******************')
print(sample_size)
print('******************')

accident = accident.sample(n=sample_size, random_state=42)

# Convert the 'Accident Date' column to datetime
accident['Accident Date'] = pd.to_datetime(accident['Accident Date'], errors='coerce')

# Decompose the date into year, month, and day
accident['Year'] = accident['Accident Date'].dt.year
accident['Month'] = accident['Accident Date'].dt.month
accident['Day'] = accident['Accident Date'].dt.day.apply(lambda x: [x])  # Convert to a list

# Iterate over each row in the data
for idx, row in accident.iterrows():
    latitude = row['Latitude']
    longitude = row['Longitude']
    year = row['Year']
    month = row['Month']
    day = row['Day']

    if pd.isnull(latitude) or pd.isnull(longitude) or pd.isnull(year) or pd.isnull(month) or pd.isnull(day):
        print(f"Skipping row {idx} due to missing data.")
        continue

    # Format the area and date for the request
    area = [latitude + 0.5, longitude - 0.5, latitude - 0.5, longitude + 0.5]
    base_request["area"] = area
    base_request["year"] = year
    base_request["month"] = month
    base_request["day"] = day

    # Perform the request
    try:
        output_file = f"data_row_{idx}.grib"
        print(f"Requesting data for row {idx}: {area}, Year: {year}, Month: {month}, Day: {day}")
        print("**********************")
        print(base_request)
        print("**********************")
        
        client.retrieve(dataset, base_request).download(output_file)
    except Exception as e:
        print(f"Failed to retrieve data for row {idx}: {e}")

    # Wait for 1 minute before the next request
    print("Waiting for 1 minute before the next request...")
    time.sleep(60)

print("All requests completed.")
