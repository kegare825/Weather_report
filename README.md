# Análisis Meteorológico de las Islas Canarias

Este proyecto analiza los datos meteorológicos de las Islas Canarias utilizando dos enfoques diferentes: uno basado en Apache Spark y otro en Pandas. Ambos scripts proporcionan visualizaciones interactivas y análisis detallados de los datos meteorológicos.

## Características

- **Dos implementaciones**: Spark y Pandas para el procesamiento de datos
- **Visualizaciones interactivas**: Gráficos generados con Plotly
- **Interfaz web**: Aplicación Streamlit para visualización interactiva
- **Análisis detallado**: Tablas con estadísticas detalladas
- **Exportación de datos**: Gráficos guardados en HTML y estadísticas en CSV

## Comparativa de Rendimiento

### Spark vs Pandas

| Característica | Spark | Pandas |
|----------------|-------|--------|
| **Velocidad de procesamiento** | Más rápido para grandes volúmenes de datos | Más rápido para conjuntos de datos pequeños |
| **Uso de memoria** | Distribuido, maneja grandes conjuntos de datos | Requiere toda la memoria en una máquina |
| **Escalabilidad** | Escala horizontalmente | Limitado a la capacidad de una máquina |
| **Complejidad** | Mayor complejidad de configuración | Más simple de implementar |
| **Recomendado para** | Grandes conjuntos de datos (>1GB) | Conjuntos de datos pequeños y medianos |

### Consideraciones de Uso

- **Spark**: Recomendado cuando:
  - Se trabaja con grandes volúmenes de datos
  - Se necesita procesamiento distribuido
  - Se requiere escalabilidad horizontal
  - Se dispone de un cluster de computación

- **Pandas**: Recomendado cuando:
  - Los datos caben en la memoria de una máquina
  - Se necesita una implementación rápida y simple
  - No se requiere procesamiento distribuido
  - Se trabaja con conjuntos de datos pequeños o medianos

## Requisitos

- Python 3.8+
- Dependencias listadas en `requirements.txt`
- Para la versión de Spark:
  - Apache Spark
  - Java 8+

## Instalación

1. Clonar el repositorio:
```bash
git clone [URL_DEL_REPOSITORIO]
cd Weather_report
```

2. Crear y activar un entorno virtual:
```bash
python -m venv venv
source venv/bin/activate  # En Linux/Mac
venv\Scripts\activate     # En Windows
```

3. Instalar dependencias:
```bash
pip install -r requirements.txt
```

## Uso

### Versión con Pandas
```bash
streamlit run pandas_weather_processor.py
```

### Versión con Spark
```bash
streamlit run spark_weather_processor.py
```

## Funcionalidades

### Análisis de Datos
- Temperaturas (promedio, mínima, máxima)
- Precipitación (total y días de lluvia)
- Presión atmosférica
- Patrones de viento

### Visualizaciones
1. **Gráficos horarios**:
   - Temperatura (promedio, mínima, máxima)
   - Humedad (promedio, mínima, máxima)
   - Presión atmosférica (promedio, mínima, máxima)

2. **Gráficos mensuales**:
   - Precipitación total
   - Rosa de los vientos

3. **Tablas de estadísticas**:
   - Análisis de temperaturas
   - Análisis de precipitación
   - Análisis de presión atmosférica
   - Análisis de viento
   - Estadísticas mensuales completas

### Exportación de Datos
Los resultados se guardan en:
- Gráficos interactivos en HTML en la carpeta `graficos/[nombre_isla]/`
- Estadísticas en CSV en la carpeta `graficos/[nombre_isla]/`

## Estructura del Proyecto

```
Weather_report/
├── graficos/                  # Gráficos generados
├── weather_data_hourly_*/     # Datos meteorológicos por isla
├── pandas_weather_processor.py # Script de procesamiento con Pandas
├── spark_weather_processor.py  # Script de procesamiento con Spark
├── requirements.txt           # Dependencias del proyecto
└── README.md                  # Este archivo
```

## Notas

- Los datos meteorológicos se descargan automáticamente para cada isla
- Los gráficos se generan de forma interactiva y se pueden guardar
- Las estadísticas se pueden exportar a CSV para análisis adicionales
- La elección entre Spark y Pandas depende del volumen de datos y los recursos disponibles 