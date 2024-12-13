# Proyecto NYC Taxis

## 📜 Alcance del Proyecto

### Contexto

- Se busca diversificar el negocio de transporte mediante la incursión en transporte de pasajeros con automóviles en Nueva York.

- Se desea analizar la posibilidad de incorporar vehículos eléctricos en la flota contribuyendo a un futuro menos contaminado y ajustándose a las demandas de |un mercado más consciente ambientalmente.

### Objetivo General

Realizar un análisis de viabilidad económico y ambiental para la implementación de una nueva flotilla de taxis híbridos y eléctricos en Nueva York.

### Objetivos Específicos

- Encontrar que distritos cuentan con mayor número de usuarios que realizan viajes.
- Realizar un análisis de viabilidad ambiental al utilizar vehículos híbridos y eléctricos.
- Realizar un análisis costo/beneficio entre el consumo eléctrico y consumo gasolina.

## 🎯 KPIs

### 1. Aumentar el los taxis electricos e hibridos activos en el trimestre.

![KPI1](/assets/img/KPI1.jpg)

### 2. Aumentar la cantidad de viajes por borough por trimestre.

![KPI2](/assets/img/KPI2.jpg)

### 3. Mantener el tiempo promedio de espera del pasajero en menos de 5 minutos, medido mensualmente.

![KPI3](/assets/img/KPI3.jpg)

## 🔍 Análisis Preliminar

Para viabilizar el cumplimiento de los objetivos se revisó las fuentes de datos disponibles junto con el lanzamiento del proyecto, así como nuevas fuentes que se hacen necesarias para cumplir estos objetivos.

Nuestra conclusión es obtener la infomación desde los dataset estáticos de viajes y emisiones e incorporar nuevas fuentes de datos al análisis, las que se especifican en el [informe de análisis preliminar](/EDA/Análisis%20Preliminar.md).

## 🖥️ Tech Stack

![Tech Stack](/assets/img/nyc_taxi_tech_stack.jpg)

Puedes consultar la fundamentación del stack tecnológico en el siguiente [link](/TECH-STACK/README.md)

## 📄 Flujo de trabajo

![Flujo de trabajo](/assets/img/nyc_taxi_data_flow.jpg)

El flujo de trabajo en la figura anterior, presenta el flujo que seguirán los datos desde su origen hasta quedar disponibles en las plataformas de visulización.

## 🧑‍💻 Metodología de trabajo

Adoptamos la metodología ágil SCRUM para gestionar el proyecto, centrada en colaboración, adaptabilidad y entregas continuas mediante Sprints. El proyecto se divide en 3 Sprints para un seguimiento detallado:

- Sprint 1: Establecimiento de base, configuración de herramientas de gestión, definición de roles, análisis preliminar de datos y acoplamiento a la metodología. (Semanas 1 y 2)
- Sprint 2: Implementación de procesos ETL, diseño DataWarehouse y MVP visualización de datos. (Semanas 3 y 4)
- Sprint 3: Desarrollo de Producto ML y Dashboard interactivo y finalización de la documentación técnica. (Semanas 5 y 6)

### El cronograma de entregables es el siguiente:

![Entregables](/assets/img/cronograma_entregables.jpg)

Puedes consultar el [diagrama detallado](https://github.com/users/lmunozm1702/projects/12/views/4) y la [asignación de tareas](https://github.com/users/lmunozm1702/projects/12/views/1)

## Análisis Exploratorio de Datos (EDA1)

Basados en los objetivos planteados y en el producto a desarrollar, se requieren fuentes de datos con información relacionada con lo siguiente:

- Información sobre los taxis activos en circulación, que incluya todo lo relacionado al vehiculo, principalmente el modelo.
- Información sobre los modelos de carros, donde se pueda obtener información relacionada a sus costos de compra, costos de uso y características, principalmente del motor.
- Información sobre la concentracion de $\text{CO}_2$ en la ciudad de Nueva York a través del tiempo.
- Información sobre los niveles de sonido en la ciudad de Nueva York a través del tiempo, debido a vehículos.
- Información sobre los viajes en taxi en la ciudad de Nueva York a través del tiempo.

### Del análisis realizado, se obtuvieron las siguientes métricas de calidad de los datos en cada Dataset

- [Taxis Verdes](/EDA/EDA%20green_tripdata_09_2024.ipynb): 95%
- [Taxis Amarillos](/EDA/EDA%20yellow_tripdata_09_2024.ipynb): 93%
- [For Hire](/EDA/EDA%20For-Hire_tripdata_09_2024.ipynb):
- [High Values]:
- [Calidad del Aire](/EDA/EDA-Calidad_de_Air_Quality.ipynb):
- [Calidad de Emisiones de Gas](/EDA/EDA-Calidad_de_Gas_Emissions.ipynb):

### Se agregarán a las fuentes de datos originales, 2 adicionales que se requieren para asegurar la calidad del prducto ML:

- [API con info del VIN de los vehículos](https://vpic.nhtsa.dot.gov/api/), nos ayudará, a partir del VIN del vehículo poder determinar el modelo y tipo de motor del vehículo.
- [Datasets emisiones de GAS de vehículos](https://data.cityofnewyork.us/Environment/NYC-Greenhouse-Gas-Emissions-Inventory/wq7q-htne/about_data), nos disponibilizará la información de las emisiones de gas de los vehículos en la ciudad de Nueva York.

**_ En la estapa de ETL automatizaremos el acceso a estas fuentes de datos, vía webcrawling y ejecución de API's. _**

## 👨‍🔬 Roles

### Data Analyst

Responsable de explorar, analizar e interpretar los datos para generar insights que respalden la toma de decisiones.

**_Responsabilidades_**

- Consultar y extraer datos desde diferentes fuentes.
- Realizar análisis exploratorio de datos (EDA).
- Generar reportes, dashboards y visualizaciones.
- Definir métricas clave de rendimiento (KPIs).
- Colaborar con stakeholders para traducir requerimientos de negocio en consultas analíticas.
- Crear dashboard interactivo para análisis de los datos recolectados.

| ![Imagen usuario](assets/img/user-image.png) | ![Imagen usuario](assets/img/user-image.png) |
| :------------------------------------------: | :------------------------------------------: |
|              **Samuel Rangel**               |           **Francisco Hugo Lezik**           |

### Data Engineer

Encargado de diseñar, construir y mantener la infraestructura de datos necesaria para la ingestión, procesamiento y almacenamiento de datos a gran escala.

**_Responsabilidades_**

- Diseñar y desarrollar pipelines de datos escalables y eficientes.
- Integrar múltiples fuentes de datos (ETL/ELT).
- Optimizar el rendimiento y la calidad de los datos.
- Asegurar la disponibilidad, integridad y seguridad de los datos.
- Monitorear y mantener la infraestructura en entornos on-premise o en la nube.

| ![Imagen usuario](assets/img/user-image.png) | ![Imagen usuario](assets/img/user-image.png) |
| :------------------------------------------: | :------------------------------------------: |
|                **Luis Muñoz**                |           **Juan C. Ruiz Navarro**           |

### ML Engineer

Responsable de implementar, desplegar y mantener modelos de machine learning en producción.

**_Responsabilidades_**

- Colaborar con Data Scientists para traducir modelos en prototipos productivos.
- Diseñar pipelines de machine learning automatizados.
- Desplegar modelos en entornos de producción (CI/CD).
- Monitorear el rendimiento y la precisión de los modelos en producción.
- Optimizar modelos para escalabilidad y eficiencia.
- Crear modelo de machine learning para predecir distritos con más solicitud de viajes.

| ![Imagen usuario](assets/img/user-image.png) | ![Imagen usuario](assets/img/user-image.png) |
| :------------------------------------------: | :------------------------------------------: |
|               **Jose Quispe**                |              **Sebastian Diaz**              |
