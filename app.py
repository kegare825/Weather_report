import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import sys
import glob

def setup_java_path():
    """Configurar la ruta de Java para Spark"""
    java_home = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.27.6-hotspot"
    if not os.path.exists(java_home):
        st.error("Java no encontrado en la ubicación esperada. Por favor, instale Java 11.")
        sys.exit(1)
    
    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = f"{os.environ['PATH']};{java_home}\\bin"
    os.environ["HADOOP_HOME"] = os.path.dirname(os.path.dirname(os.path.dirname(sys.executable)))

def create_spark_session():
    """Crea y configura una sesión de Spark"""
    spark = SparkSession.builder \
        .appName("WeatherAnalysis") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()
    return spark

def load_weather_data(spark, base_dir):
    """Carga los datos meteorológicos desde archivos CSV"""
    # Obtener lista de archivos CSV
    csv_files = glob.glob(f"{base_dir}/*.csv")
    if not csv_files:
        raise FileNotFoundError(f"No se encontraron archivos CSV en {base_dir}")
    
    # Definir esquema
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
    
    # Leer cada archivo CSV y unirlos
    dfs = []
    for file in csv_files:
        df = spark.read \
            .option("header", "true") \
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss") \
            .schema(schema) \
            .csv(file)
        dfs.append(df)
    
    # Unir todos los DataFrames
    df = dfs[0]
    for other_df in dfs[1:]:
        df = df.union(other_df)
    
    # Procesar fechas
    df = df.withColumn("Time", to_timestamp(col("Time"))) \
           .withColumn("Hour", hour(col("Time"))) \
           .withColumn("Date", to_date(col("Time"))) \
           .withColumn("Year", year(col("Time"))) \
           .withColumn("Month", month(col("Time")))
    
    return df

def main():
    st.set_page_config(page_title="Análisis Meteorológico", layout="wide")
    st.title("Análisis Meteorológico de Las Palmas de Gran Canaria")
    
    # Configurar Java y Spark
    setup_java_path()
    spark = create_spark_session()
    
    try:
        # Cargar datos
        base_dir = "weather_data_hourly_28.0_-15.75"
        df = load_weather_data(spark, base_dir)
        
        # Convertir a Pandas para visualización
        df_pd = df.toPandas()
        
        # Sidebar para filtros
        st.sidebar.header("Filtros")
        year = st.sidebar.selectbox("Año", sorted(df_pd["Year"].unique()))
        month = st.sidebar.selectbox("Mes", sorted(df_pd[df_pd["Year"] == year]["Month"].unique()))
        
        # Filtrar datos
        filtered_df = df_pd[(df_pd["Year"] == year) & (df_pd["Month"] == month)]
        
        # Crear columnas para los gráficos
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de temperatura por hora
            st.subheader("Temperatura por Hora")
            fig_temp = px.line(filtered_df.groupby("Hour")["Temperature"].mean().reset_index(),
                             x="Hour", y="Temperature",
                             title="Temperatura Promedio por Hora")
            st.plotly_chart(fig_temp, use_container_width=True)
            
            # Gráfico de humedad
            st.subheader("Humedad por Hora")
            fig_humidity = px.line(filtered_df.groupby("Hour")["Humidity"].mean().reset_index(),
                                 x="Hour", y="Humidity",
                                 title="Humedad Promedio por Hora")
            st.plotly_chart(fig_humidity, use_container_width=True)
        
        with col2:
            # Gráfico de precipitación
            st.subheader("Precipitación por Día")
            fig_precip = px.bar(filtered_df.groupby("Date")["Precipitation"].sum().reset_index(),
                              x="Date", y="Precipitation",
                              title="Precipitación Total por Día")
            st.plotly_chart(fig_precip, use_container_width=True)
            
            # Rosa de los vientos
            st.subheader("Rosa de los Vientos")
            wind_rose = filtered_df.groupby("WindDirection").size().reset_index(name="Count")
            fig_wind = px.bar_polar(wind_rose, r="Count", theta="WindDirection",
                                  title="Distribución de Direcciones del Viento",
                                  template="plotly_dark")
            st.plotly_chart(fig_wind, use_container_width=True)
        
        # Estadísticas en la parte inferior
        st.subheader("Estadísticas del Mes")
        col3, col4, col5, col6 = st.columns(4)
        
        with col3:
            st.metric("Temperatura Promedio", f"{filtered_df['Temperature'].mean():.1f}°C")
            st.metric("Temperatura Máxima", f"{filtered_df['Temperature'].max():.1f}°C")
            st.metric("Temperatura Mínima", f"{filtered_df['Temperature'].min():.1f}°C")
        
        with col4:
            st.metric("Humedad Promedio", f"{filtered_df['Humidity'].mean():.1f}%")
            st.metric("Humedad Máxima", f"{filtered_df['Humidity'].max():.1f}%")
            st.metric("Humedad Mínima", f"{filtered_df['Humidity'].min():.1f}%")
        
        with col5:
            st.metric("Precipitación Total", f"{filtered_df['Precipitation'].sum():.1f}mm")
            st.metric("Días con Lluvia", f"{(filtered_df['Precipitation'] > 0).sum()}")
            st.metric("Velocidad del Viento Promedio", f"{filtered_df['WindSpeed'].mean():.1f}km/h")
        
        with col6:
            st.metric("Presión Promedio", f"{filtered_df['Pressure'].mean():.1f}hPa")
            st.metric("Presión Máxima", f"{filtered_df['Pressure'].max():.1f}hPa")
            st.metric("Presión Mínima", f"{filtered_df['Pressure'].min():.1f}hPa")
    
    except Exception as e:
        st.error(f"Error durante el procesamiento: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 