import requests
import json
import csv
from datetime import datetime, timedelta
import os

def get_weather_forecast(latitude, longitude):
    # Base URL for the weather API
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Calculate dates for the last year
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    # Format dates as required by the API
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # Parameters for the API request
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "weather_code",
            "wind_speed_10m",
            "wind_direction_10m",
            "pressure_msl"
        ],
        "timezone": "auto"
    }
    
    try:
        # Make the API request
        print(f"Fetching weather data from {start_date_str} to {end_date_str}...")
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse the JSON response
        data = response.json()
        
        if 'hourly' not in data:
            print("Error: No hourly data found in response")
            print("API Response:", data)
            return
        
        # Create a directory for CSV files if it doesn't exist
        base_dir = f'weather_data_hourly_{latitude}_{longitude}'
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        
        # Process each hour's data
        current_date = None
        current_file = None
        current_writer = None
        
        for i in range(len(data["hourly"]["time"])):
            time = data["hourly"]["time"][i]
            date = time.split('T')[0]  # Extract date part
            
            # If we're on a new date, create a new file
            if date != current_date:
                if current_file:
                    current_file.close()
                
                filename = f'{base_dir}/weather_{date}.csv'
                current_file = open(filename, 'w', newline='', encoding='utf-8')
                current_writer = csv.writer(current_file)
                
                # Write header for the new file
                current_writer.writerow([
                    'Time',
                    'Temperature (°C)',
                    'Relative Humidity (%)',
                    'Precipitation (mm)',
                    'Weather Code',
                    'Wind Speed (km/h)',
                    'Wind Direction (°)',
                    'Pressure (hPa)'
                ])
                
                current_date = date
                print(f"Created file: {filename}")
            
            # Write the data row
            current_writer.writerow([
                time,
                data["hourly"]["temperature_2m"][i],
                data["hourly"]["relative_humidity_2m"][i],
                data["hourly"]["precipitation"][i],
                data["hourly"]["weather_code"][i],
                data["hourly"]["wind_speed_10m"][i],
                data["hourly"]["wind_direction_10m"][i],
                data["hourly"]["pressure_msl"][i]
            ])
        
        # Close the last file
        if current_file:
            current_file.close()
        
        print(f"\nAll weather data has been saved to directory: {base_dir}")
        print(f"Data range: {start_date_str} to {end_date_str}")
        print(f"Location: {latitude}°N, {longitude}°W")
        
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        print("API Response:", response.text if 'response' in locals() else "No response")
    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    # Coordinates for Las Palmas de Gran Canaria
    latitude = 28.0
    longitude = -15.75
    
    get_weather_forecast(latitude, longitude) 