from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

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

def create_spark_session():
    """Crea y configura una sesión de Spark"""
    spark = SparkSession.builder \
        .appName("WeatherAnalysis") \
        .master("local[*]") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    return spark

def load_weather_data(spark, base_dir):
    """Carga los datos meteorológicos desde archivos CSV"""
    # Verificar si el directorio existe
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"El directorio {base_dir} no existe")
    
    # Verificar si hay archivos CSV en el directorio
    csv_files = [f for f in os.listdir(base_dir) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError(f"No se encontraron archivos CSV en el directorio {base_dir}")
    
    # Definir el esquema para los datos
    schema = StructType([
        StructField("Time", StringType(), True),
        StructField("Temperature", DoubleType(), True),
        StructField("Humidity", DoubleType(), True),
        StructField("Precipitation", DoubleType(), True),
        StructField("WeatherCode", IntegerType(), True),
        StructField("WindSpeed", DoubleType(), True),
        StructField("WindDirection", DoubleType(), True),
        StructField("Pressure", DoubleType(), True)
    ])
    
    try:
        # Construir la ruta completa para los archivos CSV
        input_path = os.path.join(base_dir, "*.csv").replace("\\", "/")
        
        # Cargar todos los archivos CSV del directorio
        df = spark.read \
            .option("header", "true") \
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss") \
            .option("mode", "DROPMALFORMED") \
            .option("nullValue", "NA") \
            .option("nanValue", "NA") \
            .schema(schema) \
            .csv(input_path)
        
        # Convertir la columna Time a timestamp y extraer componentes de fecha
        df = df.withColumn("Time", to_timestamp(col("Time"))) \
               .withColumn("Hour", hour(col("Time"))) \
               .withColumn("Date", to_date(col("Time"))) \
               .withColumn("Year", year(col("Time"))) \
               .withColumn("Month", month(col("Time")))
        
        # Reemplazar valores nulos y NaN
        df = df.fillna(0, subset=["Temperature", "Humidity", "Precipitation", "WindSpeed", "Pressure"])
        df = df.fillna(0, subset=["WeatherCode", "WindDirection"])
        
        return df
    except Exception as e:
        print(f"Error al cargar los datos: {str(e)}")
        raise

def analyze_temperature(df):
    """Analiza las temperaturas por mes y año"""
    temp_stats = df.groupBy("Year", "Month") \
        .agg(
            avg("Temperature").alias("AvgTemperature"),
            min("Temperature").alias("MinTemperature"),
            max("Temperature").alias("MaxTemperature")
        ) \
        .orderBy("Year", "Month")
    
    return temp_stats

def analyze_wind(df):
    """Analiza los patrones de viento"""
    wind_stats = df.groupBy("WindDirection") \
        .agg(
            avg("WindSpeed").alias("AvgWindSpeed"),
            count("*").alias("Count")
        ) \
        .orderBy("Count", ascending=False)
    
    return wind_stats

def analyze_precipitation(df):
    """Analiza los patrones de precipitación"""
    # Primero, contar días únicos con precipitación
    rainy_days = df.filter(col("Precipitation") > 0) \
        .select("Date") \
        .distinct() \
        .groupBy("Year", "Month") \
        .count() \
        .withColumnRenamed("count", "RainyDays")
    
    # Luego, sumar la precipitación total por mes
    precip_stats = df.groupBy("Year", "Month") \
        .agg(sum("Precipitation").alias("TotalPrecipitation")) \
        .orderBy("Year", "Month")
    
    # Unir los resultados
    precip_stats = precip_stats.join(rainy_days, ["Year", "Month"], "left") \
        .fillna(0, subset=["RainyDays"]) \
        .orderBy("Year", "Month")
    
    return precip_stats

def analyze_pressure(df):
    """Analiza los patrones de presión atmosférica"""
    pressure_stats = df.groupBy("Year", "Month") \
        .agg(
            avg("Pressure").alias("AvgPressure"),
            min("Pressure").alias("MinPressure"),
            max("Pressure").alias("MaxPressure")
        ) \
        .orderBy("Year", "Month")
    
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
    temp_pd = temp_stats.toPandas()
    st.dataframe(temp_pd.style.format({
        'AvgTemperature': '{:.1f}°C',
        'MinTemperature': '{:.1f}°C',
        'MaxTemperature': '{:.1f}°C'
    }))
    
    # Tabla de precipitación
    st.write("**Análisis de Precipitación**")
    precip_stats = analyze_precipitation(df)
    precip_pd = precip_stats.toPandas()
    st.dataframe(precip_pd.style.format({
        'TotalPrecipitation': '{:.1f} mm',
        'RainyDays': '{:.0f} días'
    }))
    
    # Tabla de presión
    st.write("**Análisis de Presión Atmosférica**")
    pressure_stats = analyze_pressure(df)
    pressure_pd = pressure_stats.toPandas()
    st.dataframe(pressure_pd.style.format({
        'AvgPressure': '{:.1f} hPa',
        'MinPressure': '{:.1f} hPa',
        'MaxPressure': '{:.1f} hPa'
    }))
    
    # Tabla de viento
    st.write("**Análisis de Viento**")
    wind_stats = analyze_wind(df)
    wind_pd = wind_stats.toPandas()
    st.dataframe(wind_pd.style.format({
        'AvgWindSpeed': '{:.1f} km/h',
        'Count': '{:.0f}'
    }))
    
    with col1:
        # 1. Gráfico de temperatura por hora
        hourly_temp = df.groupBy("Hour").agg(avg("Temperature").alias("Temperature")).orderBy("Hour").toPandas()
        fig_temp = px.line(hourly_temp, x="Hour", y="Temperature", 
                          title=f"Temperatura Promedio por Hora - {island_name}",
                          labels={"Hour": "Hora del Día", "Temperature": "Temperatura (°C)"})
        st.plotly_chart(fig_temp, use_container_width=True)
        temp_html_path = os.path.join(island_dir, "temperatura_horaria.html")
        fig_temp.write_html(temp_html_path)
        with open(temp_html_path, 'r', encoding='utf-8') as f:
            st.components.v1.html(f.read(), height=500)
        
        # 2. Gráfico de humedad por hora
        hourly_humidity = df.groupBy("Hour").agg(avg("Humidity").alias("Humidity")).orderBy("Hour").toPandas()
        fig_humidity = px.line(hourly_humidity, x="Hour", y="Humidity",
                              title=f"Humedad Promedio por Hora - {island_name}",
                              labels={"Hour": "Hora del Día", "Humidity": "Humedad (%)"})
        st.plotly_chart(fig_humidity, use_container_width=True)
        humidity_html_path = os.path.join(island_dir, "humedad_horaria.html")
        fig_humidity.write_html(humidity_html_path)
        with open(humidity_html_path, 'r', encoding='utf-8') as f:
            st.components.v1.html(f.read(), height=500)
        
        # 3. Gráfico de presión por hora
        hourly_pressure = df.groupBy("Hour").agg(avg("Pressure").alias("Pressure")).orderBy("Hour").toPandas()
        fig_pressure = px.line(hourly_pressure, x="Hour", y="Pressure",
                              title=f"Presión Promedio por Hora - {island_name}",
                              labels={"Hour": "Hora del Día", "Pressure": "Presión (hPa)"})
        st.plotly_chart(fig_pressure, use_container_width=True)
        pressure_html_path = os.path.join(island_dir, "presion_horaria.html")
        fig_pressure.write_html(pressure_html_path)
        with open(pressure_html_path, 'r', encoding='utf-8') as f:
            st.components.v1.html(f.read(), height=500)
    
    with col2:
        # 4. Gráfico de precipitación mensual
        monthly_precip = df.groupBy("Year", "Month").agg(sum("Precipitation").alias("Precipitation")).orderBy("Year", "Month").toPandas()
        fig_precip = px.bar(monthly_precip, x="Month", y="Precipitation", color="Year",
                           title=f"Precipitación Total por Mes - {island_name}",
                           labels={"Month": "Mes", "Precipitation": "Precipitación (mm)"})
        st.plotly_chart(fig_precip, use_container_width=True)
        precip_html_path = os.path.join(island_dir, "precipitacion_mensual.html")
        fig_precip.write_html(precip_html_path)
        with open(precip_html_path, 'r', encoding='utf-8') as f:
            st.components.v1.html(f.read(), height=500)
        
        # 5. Rosa de los vientos
        wind_rose = df.groupBy("WindDirection").agg(count("*").alias("Count")).toPandas()
        fig_wind = px.bar_polar(wind_rose, r="Count", theta="WindDirection",
                               title=f"Rosa de los Vientos - {island_name}",
                               template="plotly_dark")
        st.plotly_chart(fig_wind, use_container_width=True)
        wind_html_path = os.path.join(island_dir, "rosa_vientos.html")
        fig_wind.write_html(wind_html_path)
        with open(wind_html_path, 'r', encoding='utf-8') as f:
            st.components.v1.html(f.read(), height=500)
        
        # 6. Estadísticas mensuales
        st.subheader(f"Estadísticas Mensuales - {island_name}")
        monthly_stats = df.groupBy("Year", "Month").agg(
            avg("Temperature").alias("Temp Prom"),
            avg("Humidity").alias("Humedad Prom"),
            sum("Precipitation").alias("Precip Total"),
            avg("Pressure").alias("Presión Prom")
        ).orderBy("Year", "Month").toPandas()
        st.dataframe(monthly_stats.style.format({
            'Temp Prom': '{:.1f}°C',
            'Humedad Prom': '{:.1f}%',
            'Precip Total': '{:.1f} mm',
            'Presión Prom': '{:.1f} hPa'
        }))
        
        # Guardar estadísticas en CSV
        monthly_stats.to_csv(os.path.join(island_dir, "estadisticas_mensuales.csv"), index=False)
    
    st.success(f"Gráficos guardados en la carpeta 'graficos/{island_name}'")

def process_island_data(spark, island_name, coords):
    """Procesa los datos de una isla específica"""
    st.subheader(f"Análisis de Datos para {island_name}")
    
    # Directorio base donde se encuentran los archivos CSV
    base_dir = f'weather_data_hourly_{island_name}_{coords["latitude"]}_{coords["longitude"]}'
    
    try:
        # Cargar datos
        with st.spinner(f"Cargando datos meteorológicos para {island_name}..."):
            df = load_weather_data(spark, base_dir)
        
        # Crear visualizaciones
        create_visualizations(df, island_name)
        
    except Exception as e:
        st.error(f"Error durante el procesamiento de {island_name}: {str(e)}")

def main():
    # Configurar la página de Streamlit
    st.set_page_config(page_title="Análisis Meteorológico Islas Canarias", layout="wide")
    st.title("Análisis Meteorológico de las Islas Canarias")
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    try:
        # Selector de isla
        selected_island = st.sidebar.selectbox("Selecciona una isla", list(CANARY_ISLANDS.keys()))
        
        # Procesar datos para la isla seleccionada
        process_island_data(spark, selected_island, CANARY_ISLANDS[selected_island])
            
    except Exception as e:
        st.error(f"Error durante el procesamiento: {str(e)}")
    finally:
        # Detener la sesión de Spark
        spark.stop()

if __name__ == "__main__":
    main() 