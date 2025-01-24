## Producto ML

### **1\. Introducción**

*   **Contexto:** El producto de Machine Learning se desplegará en una aplicación web donde el usuario podrá ingresar una fecha, hora y barrio de la ciudad de Nueva York y el modelo de machine Learning hará una predicción, con base en el entrenamiento del modelo usando la data histórica, de la demanda de viajes en cada una de las zonas de la ciudad. Esto se representará como un mapa de calor utilizando la librería Geo Pandas.
*   Por otra parte, utilizando la misma estrategia para predecir la demanda, de la misma data histórica, se puede hacer un estimado de la oferta, por lo cual, al tener tanto demanda como oferta en cada zona en una fecha y hora determinadas, podremos calcular lo que denominamos "porcentaje de cobertura".
*   **Relevancia**: Permite a los conductores ver la relacion oferta demanda que hay a su alrededor y zonas cercanas; y que pueda desplazarse a una zona que le permita conseguir un pasajero en el menor tiempo de espera. Desde el punto de vista de la empresa permite hacer una distribución mas eficiente de los conductores en la ciudad reduciendo los tiempos de espera de los pasajeros tambien.

---

### **2\. ETL del Dataset**

*   **Origen de los datos**: La aplicación recoge información de dos fuentes, la primera trae información de los viajes hechos en taxis amarillos y verdes de la ciudad de nueva york, la segunda trae información del clima en la ciudad por cada hora.
*   **Limpieza de datos**: Se eliminaron valores de tiempo y distancia que fueron cero y negativos. Tambien se eliminaron otros valores no relevantes como nulos, posibles errores de registro y outliers.
*   **Exploración inicial**: A partir de la visualización de la cantidad de viajes por locación con respecto al tiempo se determino que los valores mas significativos son: La hora, el día de la semana y la zona de inicio de los viajes.

---

### **3\. Análisis Exploratorio (EDA)**

*   **Análisis de correlaciones:** Se analizaron las correlaciones de los datos con la variable a predecir por medio de una matriz de correlaciones. Se descartaron las variables que tenían fuerte correlación entre sí.
*   **Variables predictivas:** Por medio del análisis anterior se conservaron como variables predictivas las siguientes: \[locationID, day\_of\_month, hour\_of\_day, day\_of\_week,   relative\_humidity, apparent\_temperature, temperature, weather\_code, cloud\_cover, wind\_speed, wind\_gusts\] \]
*   **Elección de Modelo**: Dado que la variable a predecir es una variable numérica la predicción es un problema de regresión. Se propuso probar dos modelos, Random Forest regressor y XGboost, elegimos este ultimo porque dio mejores resultados.

---

### **4\. Modelo de Machine Learning XGboost**

*   **Entrenamiento del modelo:** Se separaron los datos para el entrenamiento tomando el 80% de los datos originales.
*   **Elección de hiperparametros:** Se eligieron los siguientes hiperparametros tomando en cuenta disminuir la complejidad del modelo sin comprometer la eficacia de las predicciones. \[ model\_2 = xgb.XGBRegressor(    n\_estimators=80,    max\_depth=15,    learning\_rate=0.2,    subsample=0.8,    colsample\_bytree=0.8,    random\_state=42 ) \]
*   **Resultados de validación**:  Mean Squared Error (MSE): 1203.56  
                                                   Mean Absolute Error (MAE): 17.35  
                                                   R² Score: 0.95

---

### **5\. Despliegue del modelo**

*   **Infraestructura**:
    *   El entrenamiento del modelo se hizo en la nube usando Google functions. El despliegue se realizo en render usando dash.
    *   Se almaceno el modelo entrenado en un Bucket de Cloud Storage, el cual se descarga en el entorno de Render en el momento de la ejecución
*   **Predicciones:**
    *   el usuario ingresa un valor de fecha y hora para dar comienzo a la prediccion
    *   Con el valor de fecha y hora ingresados se obtienen las condiciones climáticas por medio de una API.
    *   Una vez obtenidos estas variables predictoras se generan las predicciones de la cantidad de viajes de todas las locaciones.

---

### **6\. Visualización de predicciones**

*   **Descripción**:
    *   Se grafican en un mapa de calor las predicciones realizadas con los datos puestos por el usuario.

---

### **7\. Mejoras**

*   **Futuras mejoras**:
    *   Utilización de las versiones pagas de las herramientas usadas
    *   Mejora el Frontend de la aplicación 
    

---

### **8\. Conclusiones**

*   **Eficiencia en la planificación de recursos**: El modelo de Machine Learning permite a los conductores identificar zonas con mayor probabilidad de demanda de taxis en tiempo real, ayudando a reducir los tiempos de espera tanto para los pasajeros como para los conductores. Esto no solo optimiza el servicio, sino que también mejora la experiencia del cliente.
*   **Innovación mediante datos climáticos**: La incorporación de variables climáticas como temperatura, humedad y condiciones del tiempo aporta una dimensión adicional a las predicciones, permitiendo que el modelo capture patrones más complejos y genere resultados más precisos.
*   **Impacto en la toma de decisiones**: La visualización en un mapa de calor, junto con la predicción de la oferta y demanda, proporciona una herramienta poderosa para la toma de decisiones tanto para conductores como para la empresa. Este enfoque permite optimizar la cobertura en la ciudad y minimizar la relación oferta-demanda desbalanceada.
