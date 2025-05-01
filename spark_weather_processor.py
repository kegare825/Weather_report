from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, sum, count, hour, dayofmonth, month, year, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import os
import glob
import sys
import pandas as pd

def setup_java_path():
    """Set up Java path for Spark"""
    java_home = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.27.6-hotspot"
    if not os.path.exists(java_home):
        print("Java not found at expected location. Please install Java 11.")
        sys.exit(1)
    
    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = f"{os.environ['PATH']};{java_home}\\bin"
    
    # Verify Java is accessible
    try:
        import subprocess
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        print("Java version:", result.stderr.split('\n')[0])
    except Exception as e:
        print("Error verifying Java:", str(e))
        sys.exit(1)

def process_weather_data(base_dir, spark):
    # Get all CSV files in the directory
    csv_files = glob.glob(f"{base_dir}/*.csv")
    
    if not csv_files:
        print("No CSV files found in directory!")
        return
    
    # Define schema for the CSV files
    schema = StructType([
        StructField("Time", TimestampType(), True),
        StructField("Temperature (°C)", DoubleType(), True),
        StructField("Relative Humidity (%)", DoubleType(), True),
        StructField("Precipitation (mm)", DoubleType(), True),
        StructField("Weather Code", IntegerType(), True),
        StructField("Wind Speed (km/h)", DoubleType(), True),
        StructField("Wind Direction (°)", DoubleType(), True),
        StructField("Pressure (hPa)", DoubleType(), True)
    ])
    
    # Read all CSV files into a single DataFrame
    df = spark.read \
        .option("header", "true") \
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss") \
        .schema(schema) \
        .csv(csv_files)
    
    # Add date and time components
    df = df.withColumn("Date", to_date(col("Time"))) \
           .withColumn("Hour", hour(col("Time"))) \
           .withColumn("Year", year(col("Time"))) \
           .withColumn("Month", month(col("Time")))
    
    # 1. Basic statistics
    print("\nBasic Statistics:")
    basic_stats = df.select(
        avg("Temperature (°C)").alias("Average Temperature"),
        max("Temperature (°C)").alias("Maximum Temperature"),
        min("Temperature (°C)").alias("Minimum Temperature"),
        avg("Relative Humidity (%)").alias("Average Humidity"),
        sum("Precipitation (mm)").alias("Total Precipitation"),
        avg("Wind Speed (km/h)").alias("Average Wind Speed"),
        avg("Pressure (hPa)").alias("Average Pressure"),
        count("*").alias("Total Hours Analyzed")
    ).collect()[0]
    
    print(f"Average Temperature: {basic_stats['Average Temperature']:.2f}°C")
    print(f"Maximum Temperature: {basic_stats['Maximum Temperature']:.2f}°C")
    print(f"Minimum Temperature: {basic_stats['Minimum Temperature']:.2f}°C")
    print(f"Average Humidity: {basic_stats['Average Humidity']:.2f}%")
    print(f"Total Precipitation: {basic_stats['Total Precipitation']:.2f}mm")
    print(f"Average Wind Speed: {basic_stats['Average Wind Speed']:.2f}km/h")
    print(f"Average Pressure: {basic_stats['Average Pressure']:.2f}hPa")
    print(f"Total Hours Analyzed: {basic_stats['Total Hours Analyzed']}")
    
    # 2. Hourly averages
    print("\nAverage Temperature by Hour of Day:")
    hourly_stats = df.groupBy("Hour").agg(
        avg("Temperature (°C)").alias("Average Temperature"),
        avg("Relative Humidity (%)").alias("Average Humidity"),
        avg("Wind Speed (km/h)").alias("Average Wind Speed")
    ).orderBy("Hour")
    hourly_stats.show(24, truncate=False)
    
    # 3. Monthly statistics
    print("\nMonthly Statistics:")
    monthly_stats = df.groupBy("Year", "Month").agg(
        avg("Temperature (°C)").alias("Average Temperature"),
        max("Temperature (°C)").alias("Max Temperature"),
        min("Temperature (°C)").alias("Min Temperature"),
        sum("Precipitation (mm)").alias("Total Precipitation"),
        avg("Wind Speed (km/h)").alias("Average Wind Speed")
    ).orderBy("Year", "Month")
    monthly_stats.show(truncate=False)
    
    # 4. Wind analysis
    print("\nWind Analysis:")
    wind_stats = df.groupBy("Wind Direction (°)").agg(
        count("*").alias("Count"),
        avg("Wind Speed (km/h)").alias("Average Speed"),
        max("Wind Speed (km/h)").alias("Max Speed")
    ).orderBy(col("Count").desc())
    wind_stats.show(truncate=False)
    
    # 5. Weather patterns
    print("\nWeather Patterns by Hour:")
    weather_patterns = df.groupBy("Hour", "Weather Code").count() \
        .orderBy("Hour", col("count").desc())
    weather_patterns.show(truncate=False)
    
    # 6. Extreme weather events
    print("\nTop 10 Hottest Hours:")
    df.orderBy(col("Temperature (°C)").desc()).select("Time", "Temperature (°C)").show(10, truncate=False)
    
    print("\nTop 10 Coldest Hours:")
    df.orderBy(col("Temperature (°C)")).select("Time", "Temperature (°C)").show(10, truncate=False)
    
    print("\nTop 10 Windiest Hours:")
    df.orderBy(col("Wind Speed (km/h)").desc()).select("Time", "Wind Speed (km/h)").show(10, truncate=False)
    
    # 7. Precipitation analysis
    print("\nPrecipitation Analysis:")
    daily_precip = df.groupBy("Date").agg(
        sum("Precipitation (mm)").alias("Total_Precipitation"),
        sum((col("Precipitation (mm)") > 0).cast("integer")).alias("Hours_with_Rain")
    ).filter(col("Total_Precipitation") > 0) \
     .orderBy(col("Total_Precipitation").desc())
    daily_precip.show(truncate=False)
    
    # 8. Pressure trends
    print("\nPressure Trends by Month:")
    pressure_trends = df.groupBy("Year", "Month").agg(
        avg("Pressure (hPa)").alias("Average Pressure"),
        min("Pressure (hPa)").alias("Min Pressure"),
        max("Pressure (hPa)").alias("Max Pressure")
    ).orderBy("Year", "Month")
    pressure_trends.show(truncate=False)
    
    # 9. Yearly temperature by month
    print("\nAverage Temperature by Month for Each Year:")
    temp_by_month_year = df.groupBy("Year", "Month").agg(
        avg("Temperature (°C)").alias("Average Temperature")
    ).orderBy("Year", "Month")
    
    # Convert to pandas for better display
    temp_by_month_year_pd = temp_by_month_year.toPandas()
    temp_by_month_year_pd = temp_by_month_year_pd.pivot(
        index='Month',
        columns='Year',
        values='Average Temperature'
    ).round(2)
    
    # Add month names
    month_names = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April',
        5: 'May', 6: 'June', 7: 'July', 8: 'August',
        9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }
    temp_by_month_year_pd.index = temp_by_month_year_pd.index.map(month_names)
    
    print("\nTemperature by Month and Year (°C):")
    print(temp_by_month_year_pd)
    
    # Calculate and print yearly averages
    print("\nYearly Average Temperatures:")
    yearly_avg = df.groupBy("Year").agg(
        avg("Temperature (°C)").alias("Average Temperature")
    ).orderBy("Year")
    yearly_avg.show(truncate=False)

def main():
    # Set up Java path
    setup_java_path()
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WeatherAnalysis") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    try:
        # Get the base directory from the first weather data directory found
        base_dirs = [d for d in os.listdir('.') if d.startswith('weather_data_hourly_')]
        
        if not base_dirs:
            print("No weather data directories found!")
            return
        
        # Process each directory
        for base_dir in base_dirs:
            print(f"\nProcessing data from {base_dir}")
            process_weather_data(base_dir, spark)
            
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main() 