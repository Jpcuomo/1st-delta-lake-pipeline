📊 Pipeline ETL - Datos de Binance con Delta Lake
📋 Descripción
Pipeline ETL completo para extracción, transformación y carga de datos financieros de la API de Binance. El proyecto implementa un data lake con arquitectura de medallón (bronze, silver, gold) almacenado en formato Delta Lake.

🎯 Objetivos Cumplidos
✅ Extracción de API: Datos de endpoints temporales (klines) y estáticos de Binance

✅ Almacenamiento Delta Lake: Datos crudos y procesados en formato Delta

✅ Procesamiento con Pandas: Transformaciones, limpieza y agregaciones

✅ Extracción incremental y full: Soporte para ambos modos de ingestión

✅ Arquitectura en capas: Bronze (crudo), Silver (procesado), Gold (enriquecido)

🏗️ Estructura del Data Lake
text
data/
├── bronze/    # Datos crudos de la API
├── silver/    # Datos limpios y transformados  
└── gold/      # Datos agregados para análisis
⚙️ Configuración e Instalación
1. Crear entorno virtual
bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
2. Instalar dependencias
bash
pip install -r requirements.txt
3. Configurar variables (opcional)
Editar config/settings.py según necesidad

🚀 Ejecución
Pipeline Completo (Full Load)
bash
python scripts/run_full_pipeline.py
Pipeline Incremental
bash
python scripts/run_incremental_pipeline.py
📊 Transformaciones Implementadas
Limpieza de datos: Eliminación de duplicados y valores nulos

Conversión de tipos: Formateo de fechas y tipos de datos

Renombrado de columnas: Normalización de nombres

Agregaciones: Group by con funciones de agregación

Creación de columnas: Nuevas métricas derivadas

Particionamiento: Por fecha y hora para optimización

🔧 Tecnologías Utilizadas
Python 3.8+ con Pandas para procesamiento

Delta Lake para almacenamiento

Requests para extracción de API

ydata-profiling para control de calidad

📁 Estructura del Proyecto
src/extract/ - Extracción de datos de API

src/transform/ - Transformaciones con Pandas

src/load/ - Carga a Delta Lake

src/quality/ - Control de calidad y profiling

config/ - Configuración y settings

notebooks/ - Exploración y prototipos

📋 Entregables
Pipeline de extracción full e incremental

Datos almacenados en formato Delta Lake

Transformaciones aplicadas con Pandas

Reportes de calidad de datos

Documentación y código modular

