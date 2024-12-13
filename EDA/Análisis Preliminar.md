🏠[Inicio](/README.md)

# Análisis preliminar

Basados en los objetivos planteados y en el producto a desarrollar, se requieren fuentes de datos con información relacionada con lo siguiente:

- Información sobre los taxis activos en circulación, que incluya todo lo relacionado al vehiculo, principalmente el modelo.
- Información sobre los modelos de carros, donde se pueda obtener información relacionada a sus costos de compra, costos de uso y características, principalmente del motor.
- Información sobre la concentracion de $\text{CO}_2$ en la ciudad de Nueva York a través del tiempo.
- Información sobre los viajes en taxi en la ciudad de Nueva York a través del tiempo.

Para ello se buscó en línea y la principal fuente de datos para conseguir la mayoría de esta información se encuentra en la pagina del [NYC OpenData](https://opendata.cityofnewyork.us), a excepcion de la informacion de los modelos de carros y sus costos, la cual podemos obtener la página del [gobierno de Canadá](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64#wb-auto-6)

Para el análisis de viabilidad económico se cruzará la data del [consumo de energia de los vehiculos](https://open.canada.ca/data/en/dataset/98f1a129-f628-4ce4-b24d-6f16bf24dd64#wb-auto-6) con la data de los vehiculos activos funcionando en la ciudad de Nueva York como taxi. [URL-1](https://data.cityofnewyork.us/Transportation/For-Hire-Vehicles-FHV-Active/8wbx-tsch/about_data)|[URL-2](https://data.cityofnewyork.us/Transportation/Medallion-Vehicles-Authorized/rhe8-mgbb/about_data)

Sobre la data de los viajes de los diferentes grupos de taxis se usará principalmente la información de los pick-up, particularmente la zona, fecha y hora, como base para el desarrollo de un producto de Machine Learning que tendrá como función la generación de un mapa de calor prediciendo las zonas de acuerdo al dia y la hora donde se espera mayor demanda de servicios de taxi.

Sobre la data de taxis en cirulacion, contaminación ($\text{CO}_2$ y calidad de aire), consumo de energía y modelos de carro, se utilizara principalmente para medir el impacto en la migración de la flota de vehículos a base de combustibles fósiles a vehículos eléctricos o híbridos, al comparar el cambio a través del tiempo en las flotas de transporte con las emisiones de gas en determinado período de tiempo.

🏠[Inicio](/README.md)
