DROP TABLE IF EXISTS `project_data.coordinates`;

CREATE TABLE `project_data.coordinates` (
  `location_id` INT64 NOT NULL,
  `geom` STRING NOT NULL,
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
SELECT trips.pickup_year, trips.pickup_quarter, polygons.location_id, polygons.borough, ANY_VALUE(polygons.polygon) as map_location, count(*) as cantidad
FROM `driven-atrium-445021-m2.project_data.trips` as trips
JOIN `driven-atrium-445021-m2.project_data.polygons` as polygons
ON trips.pickup_location_id = polygons.location_id
GROUP BY trips.pickup_year, trips.pickup_quarter, polygons.location_id, polygons.borough;