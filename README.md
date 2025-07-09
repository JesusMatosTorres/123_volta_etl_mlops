# 123 Volta ETL MLOps  
  
Sistema integral de inteligencia de mercado de vehículos que combina extracción de datos, transformación y operaciones de machine learning para proporcionar capacidades automatizadas de predicción de precios de vehículos.  
  
## Descripción del Proyecto  
  
Este proyecto implementa una plataforma completa de ETL y MLOps que extrae datos de listados de vehículos desde múltiples sitios web de automoción españoles, los procesa a través de una arquitectura de medallas (bronze → silver → golden), y entrena modelos CatBoost para la predicción de precios. ScraperList.py:1-8   
  
## Arquitectura del Sistema  
  
El sistema opera como una plataforma de doble propósito que combina procesamiento de datos ETL con capacidades de entrenamiento y despliegue de modelos MLOps:  
  
### Componentes Principales  
  
#### 1. Pipeline ETL (123_volta_etl/)  
- **Web Scrapers**: Extracción de datos de sitios como Autoscout24, CarMarket, Carplus, Km0, MLeon, MotorEs ScraperList.py:4-5  
- **Orquestación**: Sistema de ejecución dinámica de tareas con timeout ExecutorTask.py:2-6   
- **Transformación de Datos**: Procesamiento de bronze a silver layer con validación y limpieza Bronze_to_Silver.py:46-381 
  
#### 2. Pipeline MLOps (123_volta_mlops/)  
- **Ingeniería de Características**: Construcción de patrones de entrenamiento PatternsBuilderTask.py:33-76  
- **Entrenamiento de Modelos**: Modelos CatBoost con validación cruzada ModelTask.py:25-57  
- **Modelos Bootstrap**: Generación de múltiples variantes de modelo BootstrapModelsTask.py:28-67  
  
### Arquitectura de Medallas  
  
| Capa | Descripción | Tablas Principales |  
|------|-------------|-------------------|  
| **Bronze** | Datos raw extraídos de web scrapers | `tb_123_*task` |  
| **Silver** | Datos limpios y validados | `tb_123_volta_etl_mlops_markets_canary` |  
| **Golden** | Datos preparados para ML | `ds_123_volta_etl_mlops_001_trainingpatterns` |  
  
## Instalación y Requisitos  
  
### Dependencias del Sistema  
  
El proyecto requiere las siguientes dependencias principales instaladas en un entorno Databricks:  
  
**Paquetes Python** (se instalan vía notebook de PipInstalls):  
- Apache Spark con Delta Lake  
- Pandas/NumPy para manipulación de datos  
- Selenium WebDriver con Firefox + Geckodriver  
- CatBoost para gradient boosting  
- MLflow para registro de modelos  
- SHAP para interpretación de modelos  
  
**Infraestructura**:  
- Databricks para cómputo y almacenamiento  
- Azure Blob Storage para artefactos de modelo  
- PostgreSQL para exportación de datos AutoscoutTask.py:29-53  
  
### Configuración del Entorno  
  
El sistema utiliza configuración basada en secretos de Databricks para credenciales de base de datos y configuraciones específicas del entorno. Volta_to_PZ_jdbc.py:40-46  
  
## Uso y Ejecución  
  
### Ejecución del Pipeline ETL  
  
Cada tarea del ETL puede ejecutarse de forma independiente:  
  
```bash  
# Ejecutar scraper individual  
python AutoscoutTask.py  
python CarMarketTask.py  
python CarplusTask.py  
```
AutoscoutTask.py:66-90  
  
### Ejecución del Pipeline MLOps  
  
Las tareas de machine learning siguen un flujo secuencial:  
  
1. **Construcción de Patrones**: PatternsBuilderTask.py:194-237  
2. **Entrenamiento de Modelos**: ModelTask.py:42-48 
3. **Modelos Bootstrap**: BootstrapModelsTask.py:25-30   
  
### Orquestación Completa  
  
El sistema utiliza una lista de tareas para la ejecución orquestada: ScraperList.py:4-5 
  
## Estructura del Proyecto  
  
```  
123_volta_etl_mlops/  
├── 123_volta_etl/              # Pipeline ETL  
│   ├── Utils/                  # Utilidades compartidas  
│   ├── xls/                    # Archivos de referencia  
│   ├── AutoscoutTask.py        # Scraper Autoscout24  
│   ├── CarMarketTask.py        # Scraper CarMarket  
│   ├── CarplusTask.py          # Scraper Carplus  
│   ├── Km0Task.py              # Scraper Km0  
│   ├── MLeonTask.py            # Scraper MLeon  
│   ├── MotorEsTask.py          # Scraper MotorEs  
│   ├── NewVehiclePriceTask.py  # Datos de precios de referencia  
│   ├── Bronze_to_Silver.py     # Transformación bronze → silver  
│   ├── ExecutorTask.py         # Orquestador de tareas  
│   ├── ScraperList.py          # Lista de tareas  
│   └── Volta_to_PZ_jdbc.py     # Exportación a PostgreSQL  
│  
└── 123_volta_mlops/            # Pipeline MLOps  
    ├── cat_boost_model/        # Modelos CatBoost  
    ├── catboost_info/          # Información de modelos  
    ├── new_vehicle_price_level/ # Niveles de precios  
    ├── BootstrapModelsTask.py  # Entrenamiento bootstrap  
    ├── GetDictionaryTask.py    # Creación de diccionarios  
    ├── ModelTask.py            # Entrenamiento principal  
    ├── ModelTest.py            # Validación y testing  
    ├── PatternsBuilderTask.py  # Ingeniería de características  
    ├── PipInstalls.ipynb       # Instalación de dependencias  
    └── constants.py            # Constantes del sistema  
```  
  
## Calidad de Datos y Validación  
  
El sistema implementa controles de calidad de datos en múltiples etapas:  
  
- **Capa Bronze**: Validación de datos raw, detección de duplicados  
- **Capa Silver**: Validación de reglas de negocio, estandarización de tipos de datos  
- **Capa Golden**: Validación de características preparadas para ML  
  
Las reglas de validación incluyen rangos de precios (€1,000 - €150,000), límites de kilómetros (< 1,000,000), años de registro (≥ 1990), y estandarización de tipos de combustible. Bronze_to_Silver.py:582-587  
  
## Tecnologías Utilizadas  
  
- **Apache Spark**: Procesamiento de datos distribuido  
- **Delta Lake**: Almacenamiento de datos con versionado  
- **Selenium**: Automatización de navegadores web  
- **CatBoost**: Algoritmo de gradient boosting  
- **MLflow**: Gestión del ciclo de vida de modelos  
- **Databricks**: Plataforma de datos unificada  
- **PostgreSQL**: Base de datos para exportación  
- **Azure**: Infraestructura cloud  
  
## Despliegue y Endpoints  
  
Los modelos entrenados se despliegan automáticamente en endpoints de Databricks para inferencia en tiempo real, con integración completa con MLflow para versionado y gestión de modelos. ModelTask.py:42-48 
  
## Notas  
  
Este sistema está diseñado para funcionar en un entorno Databricks con acceso a múltiples fuentes de datos web y capacidades de procesamiento distribuido. La arquitectura modular permite la ejecución independiente de componentes individuales así como la orquestación completa del pipeline.  
