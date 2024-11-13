import cdsapi



dataset = "reanalysis-era5-single-levels"
request = {
    "product_type": ["reanalysis"],
    "variable": [
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "2m_dewpoint_temperature",
        "2m_temperature",
        "mean_sea_level_pressure",
        "mean_wave_direction",
        "mean_wave_period",
        "sea_surface_temperature",
        "significant_height_of_combined_wind_waves_and_swell",
        "surface_pressure",
        "total_precipitation"
    ],
    "year": ["2023"],
    "month": ["04"],
    "day": ["03"],
    "time": ["08:00"],
    "data_format": "grib",
    "download_format": "unarchived",
    "area": [90, -180, -90, 180]
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()
yiguygyi
