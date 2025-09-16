-- trip_sql.sql
-- Returns trips that exist in Vehicle table
--We have inputs Drive, Vehicles data
--In output we need trip_id, average_eng_load_perc, average_velocity, fuel_used
--First lets get the average_eng_load_perc, average_velocity
-- Step 1: Average engine load and velocity per trip
WITH trip_avg AS (
  SELECT
    trip_id,
    AVG(100.0 * eng_load / 255.0) AS average_eng_load_perc,
    AVG(velocity) AS average_velocity
  FROM Drive
  GROUP BY trip_id
),
--To Calculate the Fuel used we need to get the start and end fuel levels of the trip
-- Step 2: Get start_fuel and end_fuel
fuel_bounds AS (
  SELECT DISTINCT
    trip_id,
    FIRST_VALUE(fuel_level) OVER (PARTITION BY trip_id ORDER BY datetime ASC) AS start_fuel,
    FIRST_VALUE(fuel_level) OVER (PARTITION BY trip_id ORDER BY datetime DESC) AS end_fuel,
    FIRST_VALUE(vehicle_spec_id) OVER (PARTITION BY trip_id ORDER BY datetime ASC) AS vehicle_spec_id
  FROM Drive
)
--Now getting all the required columns in the result
--We have an edge case here that is if end fuel is greater then start fuel which shows refuel we get negative value
-- As fuel_used shouldn't be negative, Making the value as 0.0
SELECT
  ta.trip_id,
  --Rounding the decimal values to 6 digits.
  ta.average_eng_load_perc AS average_eng_load_perc,
  ta.average_velocity AS average_velocity,
  CASE
    WHEN b.start_fuel IS NULL OR b.end_fuel IS NULL OR v.fuel_tank_capacity IS NULL THEN NULL
    ELSE GREATEST((b.start_fuel - b.end_fuel) * v.fuel_tank_capacity / 255.0, 0.0)
  END AS fuel_used
FROM trip_avg ta
JOIN fuel_bounds b ON ta.trip_id = b.trip_id
--Considering trips where we have vehicle specification
JOIN Vehicle v ON b.vehicle_spec_id = v.vehicle_spec_id
ORDER BY ta.trip_id;
