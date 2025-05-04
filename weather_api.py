import requests
import json
import csv
from datetime import datetime, timedelta
import os

# Coordenadas de las Islas Canarias
CANARY_ISLANDS = {
    "Tenerife": {"latitude": 28.2916, "longitude": -16.6291},
    "Gran Canaria": {"latitude": 27.9545, "longitude": -15.5920},
    "Lanzarote": {"latitude": 29.0469, "longitude": -13.5899},
    "Fuerteventura": {"latitude": 28.3587, "longitude": -14.0537},
    "La Palma": {"latitude": 28.6829, "longitude": -17.7649},
    "La Gomera": {"latitude": 28.1026, "longitude": -17.1110},
    "El Hierro": {"latitude": 27.7406, "longitude": -18.0204}
}

def check_data_exists(island_name, latitude, longitude):
    """Verifica si los datos ya están descargados para una isla"""
    base_dir = f'weather_data_hourly_{island_name}_{latitude}_{longitude}'
    if not os.path.exists(base_dir):
        return False
    
    # Verificar si hay archivos CSV en el directorio
    csv_files = [f for f in os.listdir(base_dir) if f.endswith('.csv')]
    if not csv_files:
        return False
    
    # Verificar si tenemos datos para todo el rango de 10 años
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*10)
    
    # Convertir fechas a formato de archivo
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # Verificar si tenemos archivos para el rango completo
    start_file = f'weather_{start_date_str}.csv'
    end_file = f'weather_{end_date_str}.csv'
    
    return start_file in csv_files and end_file in csv_files

def get_weather_forecast(latitude, longitude, island_name):
    # Verificar si los datos ya existen
    if check_data_exists(island_name, latitude, longitude):
        print(f"\nLos datos para {island_name} ya están descargados. Saltando...")
        return
    
    # URL base para la API del clima
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Calcular fechas para los últimos 10 años
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*10)  # 10 años atrás
    
    # Formatear fechas según lo requiere la API
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # Parámetros para la solicitud a la API
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
        # Realizar la solicitud a la API
        print(f"\nObteniendo datos meteorológicos para {island_name}...")
        print(f"Desde {start_date_str} hasta {end_date_str}")
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        
        # Analizar la respuesta JSON
        data = response.json()
        
        if 'hourly' not in data:
            print(f"Error: No se encontraron datos horarios en la respuesta para {island_name}")
            print("Respuesta de la API:", data)
            return
        
        # Crear un directorio para archivos CSV si no existe
        base_dir = f'weather_data_hourly_{island_name}_{latitude}_{longitude}'
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        
        # Procesar los datos de cada hora
        current_date = None
        current_file = None
        current_writer = None
        
        for i in range(len(data["hourly"]["time"])):
            time = data["hourly"]["time"][i]
            date = time.split('T')[0]  # Extraer la parte de la fecha
            
            # Si estamos en una nueva fecha, crear un nuevo archivo
            if date != current_date:
                if current_file:
                    current_file.close()
                
                filename = f'{base_dir}/weather_{date}.csv'
                current_file = open(filename, 'w', newline='', encoding='utf-8')
                current_writer = csv.writer(current_file)
                
                # Escribir encabezado para el nuevo archivo
                current_writer.writerow([
                    'Time',
                    'Temperature',
                    'Humidity',
                    'Precipitation',
                    'WeatherCode',
                    'WindSpeed',
                    'WindDirection',
                    'Pressure'
                ])
                
                current_date = date
                print(f"Archivo creado: {filename}")
            
            # Escribir la fila de datos
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
        
        # Cerrar el último archivo
        if current_file:
            current_file.close()
        
        print(f"\nDatos meteorológicos para {island_name} guardados en el directorio: {base_dir}")
        print(f"Rango de datos: {start_date_str} a {end_date_str}")
        print(f"Ubicación: {latitude}°N, {longitude}°W")
        
    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud a la API para {island_name}: {e}")
        print("Respuesta de la API:", response.text if 'response' in locals() else "Sin respuesta")
    except Exception as e:
        print(f"Error al procesar los datos para {island_name}: {e}")

def main():
    # Obtener datos para cada isla
    for island_name, coords in CANARY_ISLANDS.items():
        get_weather_forecast(coords["latitude"], coords["longitude"], island_name)

if __name__ == "__main__":
    main() 