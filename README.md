# **Pipeline ETL - Datos de Binance con Delta Lake**

## Descripción
Este es un pipeline completo de ingeniería de datos que extrae información financiera de la API de Binance, la procesa y la almacena en formato Delta Lake. Implementé una arquitectura de data lake organizada en tres capas (bronze, silver, gold) para gestionar los datos en diferentes estados de procesamiento.

## Qué funcionalidades implementé?
## Extracción de datos
- Conexión con múltiples endpoints de la API de Binance
- Datos temporales (klines) que se actualizan regularmente
- Soporte para extracción completa e incremental
- Manejo de errores y reintentos automáticos

## Procesamiento con Pandas
- Limpieza de datos: eliminación de duplicados y valores nulos
- Transformación de tipos de datos y formatos
- Creación de nuevas columnas y métricas
- Agregaciones y análisis de datos
- Particionamiento por fecha y hora

## Almacenamiento Delta Lake
- Datos crudos en capa bronze
- Datos procesados en capa silver
- Datos enriquecidos en capa gold
- Metadatos y logs de ejecución

## Estructura del Data Lake

```
data/
├── bronze/ # Datos crudos de la API
├── silver/ # Datos limpios y transformados
└── gold/ # Datos agregados para análisis
```


## Configuración e Instalación

### Prerequisitos
- Python 3.8 o superior
- Pip para gestionar dependencias
- Acceso a internet para conectar con la API de Binance

### 1. Crear y activar entorno virtual
``python -m venv venv``
``source venv/bin/activate``  # Linux/Mac
``venv\Scripts\activate``     # Windows

### 2. Instalar dependencias
``pip install -r requirements.txt``

### 3. Instalar paquetes
``pip install -e .``

### 4. Configurar variables (opcional)
Editar config/settings.py según necesidad

### 5. Ejecución
### 5.1. Pipeline Completo (Full Load)
``python scripts/run_full_pipeline.py``

### 5.2. Pipeline Incremental
``python scripts/run_incremental_pipeline.py``

## Transformaciones Implementadas
- Limpieza de datos: Eliminación de duplicados y valores nulos
- Conversión de tipos: Formateo de fechas y tipos de datos
- Renombrado de columnas: Normalización de nombres
- Agregaciones: Group by con funciones de agregación
- Creación de columnas: Nuevas métricas derivadas
- Particionamiento: Por fecha y hora para optimización

## Tecnologías Utilizadas
- Python + Pandas para procesamiento
- Delta Lake para almacenamiento eficiente
- Requests para consumir la API de Binance
- Logging para tracking de ejecuciones
- ydata-profiling para control de calidad

## Estructura del Proyecto
```
delta_lake_bucket/
├── config/           # Configuración y settings
├── data/            # Datos en formato Delta Lake
│   ├── bronze/      # Datos crudos
│   ├── silver/      # Datos procesados
│   └── gold/        # Datos enriquecidos
├── notebooks/       # Experimentación y análisis
├── scripts/         # Scripts de ejecución
├── src/            # Código fuente
│   ├── extract/     # Extracción de datos
│   ├── transform/   # Transformaciones
│   ├── load/        # Carga a Delta
│   └── utils/       # Utilidades
└── tests/          # Tests automatizados
```
## Notas:
- Los datos se almacenan localmente en formato Delta Lake
- Las ejecuciones incrementales evitan reprocesar datos existentes
- Los reportes de calidad ayudan a identificar problemas en los datos
- El código está modularizado para mejor mantenimiento
