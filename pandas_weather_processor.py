import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime

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

def load_weather_data(base_dir):
    """Carga los datos meteorológicos desde archivos CSV"""
    # Verificar si el directorio existe
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"El directorio {base_dir} no existe")
    
    # Verificar si hay archivos CSV en el directorio
    csv_files = [f for f in os.listdir(base_dir) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError(f"No se encontraron archivos CSV en el directorio {base_dir}")
    
    # Cargar todos los archivos CSV
    dfs = []
    for file in csv_files:
        file_path = os.path.join(base_dir, file)
        df = pd.read_csv(file_path)
        dfs.append(df)
    
    # Combinar todos los DataFrames
    df = pd.concat(dfs, ignore_index=True)
    
    # Convertir la columna Time a datetime
    df['Time'] = pd.to_datetime(df['Time'])
    
    # Extraer componentes de fecha
    df['Hour'] = df['Time'].dt.hour
    df['Date'] = df['Time'].dt.date
    df['Year'] = df['Time'].dt.year
    df['Month'] = df['Time'].dt.month
    
    # Reemplazar valores nulos
    df = df.fillna(0)
    
    return df

def analyze_temperature(df):
    """Analiza las temperaturas por mes y año"""
    temp_stats = df.groupby(['Year', 'Month'])['Temperature'].agg([
        ('AvgTemperature', 'mean'),
        ('MinTemperature', 'min'),
        ('MaxTemperature', 'max')
    ]).reset_index()
    return temp_stats

def analyze_wind(df):
    """Analiza los patrones de viento"""
    wind_stats = df.groupby('WindDirection').agg({
        'WindSpeed': 'mean',
        'Time': 'count'
    }).rename(columns={'Time': 'Count'}).reset_index()
    return wind_stats

def analyze_precipitation(df):
    """Analiza los patrones de precipitación"""
    # Contar días únicos con precipitación
    rainy_days = df[df['Precipitation'] > 0].groupby(['Year', 'Month'])['Date'].nunique().reset_index()
    rainy_days.columns = ['Year', 'Month', 'RainyDays']
    
    # Sumar precipitación total por mes
    precip_stats = df.groupby(['Year', 'Month'])['Precipitation'].sum().reset_index()
    precip_stats.columns = ['Year', 'Month', 'TotalPrecipitation']
    
    # Unir los resultados
    precip_stats = precip_stats.merge(rainy_days, on=['Year', 'Month'], how='left')
    precip_stats['RainyDays'] = precip_stats['RainyDays'].fillna(0)
    
    return precip_stats

def analyze_pressure(df):
    """Analiza los patrones de presión atmosférica"""
    pressure_stats = df.groupby(['Year', 'Month'])['Pressure'].agg([
        ('AvgPressure', 'mean'),
        ('MinPressure', 'min'),
        ('MaxPressure', 'max')
    ]).reset_index()
    return pressure_stats

def create_visualizations(df, island_name):
    """Crea visualizaciones con Plotly para una isla específica"""
    # Crear directorio para los gráficos de la isla
    island_dir = os.path.join("graficos", island_name)
    if not os.path.exists(island_dir):
        os.makedirs(island_dir)
    
    # Crear columnas para los gráficos en Streamlit
    col1, col2 = st.columns(2)
    
    # Mostrar resultados de análisis en tablas
    st.subheader("Resultados del Análisis")
    
    # Tabla de temperaturas
    st.write("**Análisis de Temperaturas**")
    temp_stats = analyze_temperature(df)
    st.dataframe(temp_stats.style.format({
        'AvgTemperature': '{:.1f}°C',
        'MinTemperature': '{:.1f}°C',
        'MaxTemperature': '{:.1f}°C'
    }))
    
    # Tabla de precipitación
    st.write("**Análisis de Precipitación**")
    precip_stats = analyze_precipitation(df)
    st.dataframe(precip_stats.style.format({
        'TotalPrecipitation': '{:.1f} mm',
        'RainyDays': '{:.0f} días'
    }))
    
    # Tabla de presión
    st.write("**Análisis de Presión Atmosférica**")
    pressure_stats = analyze_pressure(df)
    st.dataframe(pressure_stats.style.format({
        'AvgPressure': '{:.1f} hPa',
        'MinPressure': '{:.1f} hPa',
        'MaxPressure': '{:.1f} hPa'
    }))
    
    # Tabla de viento
    st.write("**Análisis de Viento**")
    wind_stats = analyze_wind(df)
    st.dataframe(wind_stats.style.format({
        'WindSpeed': '{:.1f} km/h',
        'Count': '{:.0f}'
    }))
    
    with col1:
        # 1. Gráfico de temperatura por hora
        hourly_temp = df.groupby('Hour')['Temperature'].agg(['mean', 'min', 'max']).reset_index()
        fig_temp = go.Figure()
        fig_temp.add_trace(go.Scatter(x=hourly_temp['Hour'], y=hourly_temp['mean'], 
                                     name='Promedio', line=dict(color='blue')))
        fig_temp.add_trace(go.Scatter(x=hourly_temp['Hour'], y=hourly_temp['min'], 
                                     name='Mínima', line=dict(color='lightblue')))
        fig_temp.add_trace(go.Scatter(x=hourly_temp['Hour'], y=hourly_temp['max'], 
                                     name='Máxima', line=dict(color='red')))
        fig_temp.update_layout(title=f"Temperatura por Hora - {island_name}",
                             xaxis_title="Hora del Día",
                             yaxis_title="Temperatura (°C)")
        st.plotly_chart(fig_temp, use_container_width=True)
        fig_temp.write_html(os.path.join(island_dir, "temperatura_horaria.html"))
        
        # 2. Gráfico de humedad por hora
        hourly_humidity = df.groupby('Hour')['Humidity'].agg(['mean', 'min', 'max']).reset_index()
        fig_humidity = go.Figure()
        fig_humidity.add_trace(go.Scatter(x=hourly_humidity['Hour'], y=hourly_humidity['mean'], 
                                        name='Promedio', line=dict(color='green')))
        fig_humidity.add_trace(go.Scatter(x=hourly_humidity['Hour'], y=hourly_humidity['min'], 
                                        name='Mínima', line=dict(color='lightgreen')))
        fig_humidity.add_trace(go.Scatter(x=hourly_humidity['Hour'], y=hourly_humidity['max'], 
                                        name='Máxima', line=dict(color='darkgreen')))
        fig_humidity.update_layout(title=f"Humedad por Hora - {island_name}",
                                 xaxis_title="Hora del Día",
                                 yaxis_title="Humedad (%)")
        st.plotly_chart(fig_humidity, use_container_width=True)
        fig_humidity.write_html(os.path.join(island_dir, "humedad_horaria.html"))
        
        # 3. Gráfico de presión por hora
        hourly_pressure = df.groupby('Hour')['Pressure'].agg(['mean', 'min', 'max']).reset_index()
        fig_pressure = go.Figure()
        fig_pressure.add_trace(go.Scatter(x=hourly_pressure['Hour'], y=hourly_pressure['mean'], 
                                        name='Promedio', line=dict(color='purple')))
        fig_pressure.add_trace(go.Scatter(x=hourly_pressure['Hour'], y=hourly_pressure['min'], 
                                        name='Mínima', line=dict(color='plum')))
        fig_pressure.add_trace(go.Scatter(x=hourly_pressure['Hour'], y=hourly_pressure['max'], 
                                        name='Máxima', line=dict(color='darkviolet')))
        fig_pressure.update_layout(title=f"Presión por Hora - {island_name}",
                                 xaxis_title="Hora del Día",
                                 yaxis_title="Presión (hPa)")
        st.plotly_chart(fig_pressure, use_container_width=True)
        fig_pressure.write_html(os.path.join(island_dir, "presion_horaria.html"))
    
    with col2:
        # 4. Gráfico de precipitación mensual
        monthly_precip = df.groupby(['Year', 'Month'])['Precipitation'].sum().reset_index()
        fig_precip = px.bar(monthly_precip, x='Month', y='Precipitation', color='Year',
                           title=f"Precipitación Total por Mes - {island_name}",
                           labels={'Month': 'Mes', 'Precipitation': 'Precipitación (mm)'})
        st.plotly_chart(fig_precip, use_container_width=True)
        fig_precip.write_html(os.path.join(island_dir, "precipitacion_mensual.html"))
        
        # 5. Rosa de los vientos
        wind_rose = df.groupby('WindDirection').size().reset_index(name='Count')
        fig_wind = px.bar_polar(wind_rose, r='Count', theta='WindDirection',
                               title=f"Rosa de los Vientos - {island_name}",
                               template="plotly_dark")
        st.plotly_chart(fig_wind, use_container_width=True)
        fig_wind.write_html(os.path.join(island_dir, "rosa_vientos.html"))
        
        # 6. Estadísticas mensuales
        st.subheader(f"Estadísticas Mensuales - {island_name}")
        monthly_stats = df.groupby(['Year', 'Month']).agg({
            'Temperature': ['mean', 'min', 'max'],
            'Humidity': ['mean', 'min', 'max'],
            'Precipitation': 'sum',
            'Pressure': ['mean', 'min', 'max']
        }).reset_index()
        monthly_stats.columns = ['Año', 'Mes', 'Temp Prom', 'Temp Min', 'Temp Max',
                               'Humedad Prom', 'Humedad Min', 'Humedad Max',
                               'Precip Total', 'Presión Prom', 'Presión Min', 'Presión Max']
        st.dataframe(monthly_stats.style.format({
            'Temp Prom': '{:.1f}°C',
            'Temp Min': '{:.1f}°C',
            'Temp Max': '{:.1f}°C',
            'Humedad Prom': '{:.1f}%',
            'Humedad Min': '{:.1f}%',
            'Humedad Max': '{:.1f}%',
            'Precip Total': '{:.1f} mm',
            'Presión Prom': '{:.1f} hPa',
            'Presión Min': '{:.1f} hPa',
            'Presión Max': '{:.1f} hPa'
        }))
        
        # Guardar estadísticas en CSV
        monthly_stats.to_csv(os.path.join(island_dir, "estadisticas_mensuales.csv"), index=False)
    
    st.success(f"Gráficos guardados en la carpeta 'graficos/{island_name}'")

def process_island_data(island_name, coords):
    """Procesa los datos de una isla específica"""
    st.subheader(f"Análisis de Datos para {island_name}")
    
    # Directorio base donde se encuentran los archivos CSV
    base_dir = f'weather_data_hourly_{island_name}_{coords["latitude"]}_{coords["longitude"]}'
    
    try:
        # Cargar datos
        with st.spinner(f"Cargando datos meteorológicos para {island_name}..."):
            df = load_weather_data(base_dir)
        
        # Crear visualizaciones
        create_visualizations(df, island_name)
        
    except Exception as e:
        st.error(f"Error durante el procesamiento de {island_name}: {str(e)}")

def main():
    # Configurar la página de Streamlit
    st.set_page_config(page_title="Análisis Meteorológico Islas Canarias", layout="wide")
    st.title("Análisis Meteorológico de las Islas Canarias")
    
    try:
        # Selector de isla
        selected_island = st.sidebar.selectbox("Selecciona una isla", list(CANARY_ISLANDS.keys()))
        
        # Procesar datos para la isla seleccionada
        process_island_data(selected_island, CANARY_ISLANDS[selected_island])
            
    except Exception as e:
        st.error(f"Error durante el procesamiento: {str(e)}")

if __name__ == "__main__":
    main() 