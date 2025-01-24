DROP TABLE IF EXISTS `project_data.coordinates`;

CREATE TABLE `project_data.coordinates` (
  `location_id` INT64 NOT NULL,
  `zone` STRING NOT NULL,
  `borough` STRING NOT NULL,
  PRIMARY KEY (`location_id`) NOT ENFORCED
);

DROP TABLE IF EXISTS `project_data.weather`;

CREATE TABLE `project_data.weather` (
  `location_id` INT64 NOT NULL,
  `year` INT64 NOT NULL,
  `month` INT64 NOT NULL,
  `day_of_month` INT64 NOT NULL,
  `hour_of_day` INT64 NOT NULL,
  'day_of_week' INT64 NOT NULL,
  `temperature` FLOAT64 NOT NULL,
  `relative_humidity` FLOAT64 NOT NULL,
  `dew_point` FLOAT64 NOT NULL,
  `apparent_temperature` FLOAT64 NOT NULL,
  `weather_code` FLOAT64 NOT NULL,
  `pressure_msl` FLOAT64 NOT NULL,
  `cloud_cover` FLOAT64 NOT NULL,
  `wind_speed` FLOAT64 NOT NULL,
  `wind_direction` FLOAT64 NOT NULL,
  `wind_gusts` FLOAT64 NOT NULL,
  PRIMARY KEY (`location_id`, `year`, `month`, `day_of_month`, `hour_of_day`) NOT ENFORCED,  
);

DROP TABLE IF EXISTS `project_data.trips`;

CREATE TABLE `project_data.trips` (
  `trip_id` STRING NOT NULL,
  `taxi_type` STRING NOT NULL,
  `motor_type` STRING NOT NULL,
  `pickup_location_id` INT64 NOT NULL,  
  `pickup_quarter` INT64 NOT NULL,
  `pickup_year` INT64 NOT NULL,
  `pickup_month` INT64 NOT NULL,
  `pickup_day_of_month` INT64 NOT NULL,  
  `pickup_day_of_week` INT64 NOT NULL,
  `pickup_hour_of_day` INT64 NOT NULL,
  `fare_amount` FLOAT64 NOT NULL,  
  PRIMARY KEY (`trip_id`) NOT ENFORCED,
  FOREIGN KEY (`pickup_location_id`) REFERENCES `project_data.coordinates`(`location_id`) NOT ENFORCED,
);

DROP TABLE IF EXISTS `project_data.emissions`;

CREATE TABLE `project_data.emissions` (
  `sector` STRING NOT NULL,
  `inventory_type` STRING NOT NULL,
  `category_full` STRING NOT NULL,
  `category_label` STRING NOT NULL,
  `source_full` STRING NOT NULL,
  `source_label` STRING NOT NULL,
  `source_units` STRING NOT NULL,
  `concept` STRING NOT NULL,
  `year` INT64 NOT NULL,
  `value` FLOAT64 NOT NULL,
  PRIMARY KEY (`sector`, `inventory_type`, `category_full`, `source_full`, `concept`, `year`) NOT ENFORCED
);

DROP TABLE IF EXISTS `project_data.active_vehicles_count`;

CREATE TABLE `project_data.active_vehicles_count` (
  `vehicle_type` STRING NOT NULL,
  `quarter` INT64 NOT NULL,
  `month` INT64 NOT NULL,
  `year` INT64 NOT NULL,
  `count` INT64 NOT NULL,
  PRIMARY KEY (`vehicle_type`, `quarter`, `month`, `year`) NOT ENFORCED 
);

-- Calcula poligonos
SELECT coordinates.location_id, coordinates.geom, coordinates.zone, coordinates.borough, ST_GeogFromText(coordinates.geom) as polygon
FROM `driven-atrium-445021-m2.project_data.coordinates` as coordinates;

-- Vista que agrupa los viajes por year, quarter, location_id y borough
CREATE OR REPLACE MATERIALIZED VIEW `project_data.trips_year_qtr_map` AS
SELECT trips.pickup_year, trips.pickup_quarter, polygons.location_id, polygons.borough, ANY_VALUE(polygons.polygon) as map_location, ANY_VALUE(polygons.zone) as zone, count(*) as cantidad
FROM `driven-atrium-445021-m2.project_data.trips` as trips
JOIN `driven-atrium-445021-m2.project_data.polygons` as polygons
ON trips.pickup_location_id = polygons.location_id
WHERE polygons.borough <> 'EWR'
GROUP BY trips.pickup_year, trips.pickup_quarter, polygons.location_id, polygons.borough;

-- Vista KPI1
CREATE OR REPLACE MATERIALIZED VIEW `project_data.kpi1` AS
SELECT year, quarter, month, count FROM `driven-atrium-445021-m2.project_data.active_vehicles_count` WHERE vehicle_type = 'HYB' or vehicle_type = 'BEV';


-- kpi3_no_borough
CREATE OR REPLACE MATERIALIZED VIEW `project_data.kpi3_nb` AS
SELECT trips.pickup_year, trips.pickup_quarter, avg(trips.fare_amount) as avg_fare_amount FROM `driven-atrium-445021-m2.project_data.trips` as trips
GROUP BY pickup_year, pickup_quarter;

-- kpi3_borough
CREATE OR REPLACE MATERIALIZED VIEW `project_data.kpi3_sb` AS
SELECT trips.pickup_year, trips.pickup_quarter, coordinates.borough as borough, avg(trips.fare_amount) as avg_fare_amount FROM `driven-atrium-445021-m2.project_data.trips` as trips
INNER JOIN `driven-atrium-445021-m2.project_data.coordinates` as coordinates
ON coordinates.location_id = trips.pickup_location_id
WHERE coordinates.borough <> 'EWR'
GROUP BY pickup_year, pickup_quarter, coordinates.borough;

-- kpi4_no_borough
CREATE OR REPLACE MATERIALIZED VIEW `project_data.kpi4_nb` AS
SELECT trips.taxi_type, trips.pickup_year, ANY_VALUE(trips.pickup_quarter) as pickup_quarter, trips.pickup_month, count(*) as cantidad FROM `driven-atrium-445021-m2.project_data.trips` as trips
GROUP BY trips.taxi_type, pickup_year, pickup_month;

-- kpi4_borough
CREATE OR REPLACE MATERIALIZED VIEW `project_data.kpi4_sb` AS
SELECT trips.taxi_type, trips.pickup_year, ANY_VALUE(trips.pickup_quarter) as pickup_quarter, trips.pickup_month, coordinates.borough as borough, count(*) as cantidad FROM `driven-atrium-445021-m2.project_data.trips` as trips
INNER JOIN `driven-atrium-445021-m2.project_data.coordinates` as coordinates
ON coordinates.location_id = trips.pickup_location_id
WHERE coordinates.borough <> 'EWR'
GROUP BY trips.taxi_type, trips.pickup_year, trips.pickup_month, coordinates.borough;

-- card total viajes
CREATE OR REPLACE MATERIALIZED VIEW `project_data.card_total_viajes` AS
SELECT trips.pickup_year, coordinates.borough, count(*) AS cantidad
FROM `driven-atrium-445021-m2.project_data.trips` as trips INNER JOIN `driven-atrium-445021-m2.project_data.coordinates` as coordinates
ON coordinates.location_id = trips.pickup_location_id
WHERE coordinates.borough <> 'EWR'
GROUP BY trips.pickup_year, coordinates.borough;

-- card viaje promnedio dia
CREATE OR REPLACE MATERIALIZED VIEW `project_data.card_viaje_promedio_dia` AS
SELECT count(*) as cantidad, pickup_year, max(pickup_month) as pickup_month, coordinates.borough 
FROM `driven-atrium-445021-m2.project_data.trips` as trips INNER JOIN `driven-atrium-445021-m2.project_data.coordinates` as coordinates
ON coordinates.location_id = trips.pickup_location_id
WHERE coordinates.borough <> 'EWR'
GROUP BY trips.pickup_year, coordinates.borough;

-- trips by day of week and borough
CREATE OR REPLACE MATERIALIZED VIEW `project_data.trips_week_day` AS
SELECT coordinates.borough, trips.pickup_year, trips.pickup_day_of_week, count(*) AS cantidad
FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates
ON trips.pickup_location_id = coordinates.location_id 
WHERE coordinates.borough <> 'EWR'
GROUP BY coordinates.borough, trips.pickup_year, trips.pickup_day_of_week;

-- trips horly pickup by borough
CREATE OR REPLACE MATERIALIZED VIEW `project_data.trips_hourly_pickup` AS
SELECT coordinates.borough, trips.pickup_year, trips.pickup_hour_of_day, count(*) AS cantidad
FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates
ON trips.pickup_location_id = coordinates.location_id 
WHERE coordinates.borough <> 'EWR'
GROUP BY coordinates.borough, trips.pickup_year, trips.pickup_hour_of_day;

-- card_trip_duration_average;
CREATE OR REPLACE MATERIALIZED VIEW `project_data.card_trip_duration_average` AS
SELECT trips.pickup_year, coordinates.borough, AVG(trips.trip_duration) AS trip_duration
FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates
ON trips.pickup_location_id = coordinates.location_id 
WHERE coordinates.borough <> 'EWR'
GROUP BY trips.pickup_year, coordinates.borough;

-- looker dashboard trips by month with fare and trip duration and geolocation
CREATE OR REPLACE MATERIALIZED VIEW `project_data.trips_by_month` AS
SELECT coordinates.borough, trips.pickup_year, trips.pickup_month, count(*) AS cantidad, sum(trips.fare_amount) as fare_amount, avg(trips.trip_duration) as trip_duration
FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates
ON trips.pickup_location_id = coordinates.location_id 
WHERE coordinates.borough <> 'EWR'
GROUP BY coordinates.borough, trips.pickup_year, trips.pickup_month;

CREATE OR REPLACE MATERIALIZED VIEW `project_data.fare_year_qtr_map_from_trips` AS
SELECT trips.pickup_year, trips.pickup_quarter, polygons.location_id, polygons.borough, ANY_VALUE(polygons.polygon) as map_location, ANY_VALUE(polygons.zone) as zone, count(*) as cantidad, sum(trips.fare_amount) as fare_amount, avg(trips.trip_duration) as trip_duration
FROM `driven-atrium-445021-m2.project_data.trips` as trips
JOIN `driven-atrium-445021-m2.project_data.polygons` as polygons
ON trips.pickup_location_id = polygons.location_id
WHERE polygons.borough <> 'EWR'
GROUP BY trips.pickup_year, trips.pickup_quarter, polygons.location_id, polygons.borough;

CREATE OR REPLACE MATERIALIZED VIEW `project_data.fare_year_qtr_map_to_trips` AS
SELECT trips.pickup_year, trips.pickup_quarter, polygons.location_id, polygons.borough, ANY_VALUE(polygons.polygon) as map_location, ANY_VALUE(polygons.zone) as zone, count(*) as cantidad, sum(trips.fare_amount) as fare_amount, avg(trips.trip_duration) as trip_duration
FROM `driven-atrium-445021-m2.project_data.trips` as trips
JOIN `driven-atrium-445021-m2.project_data.polygons` as polygons
ON trips.dropoff_location_id = polygons.location_id
WHERE polygons.borough <> 'EWR'
GROUP BY trips.pickup_year, trips.pickup_quarter, polygons.location_id, polygons.borough;

-- Fares for weekday
CREATE OR REPLACE MATERIALIZED VIEW `project_data.fares_weekday` AS
SELECT trips.pickup_location_id,coordinates.borough, trips.pickup_year, trips.pickup_day_of_week, count(*) AS cantidad, sum(trips.fare_amount) as fare_amount, avg(trips.trip_duration) as trip_duration
FROM project_data.trips AS trips INNER JOIN project_data.coordinates AS coordinates
ON trips.pickup_location_id = coordinates.location_id 
WHERE coordinates.borough <> 'EWR'
GROUP BY coordinates.borough, trips.pickup_location_id,trips.pickup_year, trips.pickup_day_of_week;