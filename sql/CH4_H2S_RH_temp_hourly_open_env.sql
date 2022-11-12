with arduino as (SELECT timestamp_trunc(`roof_open_environment.roof_open_environment`.`timestamp`, minute) AS `timestamp`, avg(`roof_open_environment.roof_open_environment`.`CH4_concentration`) AS `CH4`, 
avg(`roof_open_environment.roof_open_environment`.`H2S_concentration`) AS `H2S`
FROM `roof_open_environment.roof_open_environment`
WHERE (`roof_open_environment.roof_open_environment`.`timestamp` >= timestamp "2022-10-11 00:00:00 GMT"
   AND `roof_open_environment.roof_open_environment`.`timestamp` < timestamp "2022-10-18 00:00:00 GMT")
GROUP BY `timestamp`
ORDER BY `timestamp` ASC

),
temp_humid_sensors as (SELECT timestamp_trunc(`temperature_humidity_sensor.temperature_humidity_sensor`.`timestamp`, minute) AS `timestamp`,
avg(`temperature_humidity_sensor.temperature_humidity_sensor`.`Temperature`) AS `Temperature`,
avg(`temperature_humidity_sensor.temperature_humidity_sensor`.`Humidity_RH`) AS `Humidity_RH`
FROM `temperature_humidity_sensor.temperature_humidity_sensor`
GROUP BY `timestamp`
ORDER BY `timestamp` ASC
) 

SELECT arduino.timestamp, arduino.CH4, arduino.H2S,temp_humid_sensors.Temperature, temp_humid_sensors.Humidity_RH
from arduino JOIN temp_humid_sensors
on arduino.timestamp = temp_humid_sensors.timestamp
order by arduino.timestamp ASC