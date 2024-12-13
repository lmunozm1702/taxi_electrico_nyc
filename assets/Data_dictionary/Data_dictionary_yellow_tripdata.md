🏠[Inicio](/README.md)
# Diccionario de datos:
## Registro de Viajes de Taxis Amarillos

|Nombre de la columna| Descripción |
|--------------------|-------------|
|VendorID |Código del proveedor tecnológico que generó el viaje y suministró la data.<br> 1= Creative Mobile Technologies, LLC <br>2= VeriFone Inc.|
|tpep_pickup_datetime| Fecha y hora donde se activó el taximetro indicando el comienzo del viaje.|
|tpep_dropoff_datetime| Fecha y hora donde se activó el taximetro indicando el fin del viaje.|
|Passenger_count| Numero de pasajeros en el Taxi (este valor es suministrado por el conductor).|
|Trip_distance| Distancia en millas reportada por el taximetro.|
|PULocationID| TLC Taxi Zone donde el taximetro fue activado y comenzó el viaje|
|DOLocationID| TLC Taxi Zone donde el taximetro indica el fin del viaje|
|RateCodeID| Tipo de tarifa aplicada al terminar el viaje.<br>1=Standard rate<br>2=JFK<br>3=Newark<br>4=Nassau or Westchester<br>5=Negotiated fare<br>6=Group ride|
|Store_and_fwd_flag| Si la informacion del viaje fue suministrada al momento, o si fue almacenada en memoria del vehiculo por no tener conexion con el servidor.<br>Y= store and forward trip<br>N= NOT a store and forward trip|
|Payment_type| Código numérico que indica el medio de pago del pasajero.<br>1= Credit card<br>2= Cash<br>3= No charge<br>4= Dispute<br>5= Unknown<br>6= Voided trip|
|Fare_amount| Tarifa base del viaje de acuerdo al tiempo y distancia.|
|Extra| Recargos y extras. Actualmente, esto incluye solo el $0.50 de hora pico y el $1 de recargo nocturno.|
|MTA_tax| $0.50 impuesto MTA (Metropolitan Transportation Authority) que se activa automaticamente basado en el tipo de tarifa aplicada.|
|Improvement_surcharge| $0.30 de recargo por mejora. Este recargo comenzó a cobrarse en 2015.|
|Tip_amount| Propina (este campo es llenado automaticamente con propinas mediante tarjetas de crédito. Propinas en efectivo no están incluidas).|
|Tolls_amount| Monto total de todos los peajes pagados durante el viaje.|
|Total_amount| Monto total del viaje pagado por el pasajero. No incluye propinas en efectivo.|
|Congestion_Surcharge| Monto cobrado en el viaje como recargo por congestion de trafico.|
|Airport_fee| $1.25 de recargo aplicado por buscar en los aeropuertos de LaGuardia y John F. Kennedy.|

🏠[Inicio](/README.md)