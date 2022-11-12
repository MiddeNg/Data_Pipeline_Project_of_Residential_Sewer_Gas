with arduino as (SELECT timestamp_trunc(`roof_vent.roof_vent`.`timestamp`, minute) AS `timestamp`, avg(`roof_vent.roof_vent`.`CH4_concentration`) AS `CH4`, 
avg(`roof_vent.roof_vent`.`H2S_concentration`) AS `H2S`
FROM `roof_vent.roof_vent`
WHERE (`roof_vent.roof_vent`.`timestamp` >= timestamp "2022-10-11 00:00:00 GMT"
   AND `roof_vent.roof_vent`.`timestamp` < timestamp "2022-10-18 00:00:00 GMT")
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

SELECT extract(hour from arduino.timestamp) as `timestamp`, avg(arduino.CH4) as `CH4`, avg(arduino.H2S) as `H2S`,avg(temp_humid_sensors.Temperature) as `Temperature`, avg(temp_humid_sensors.Humidity_RH) as `Humidity_RH`
from arduino JOIN temp_humid_sensors
on arduino.timestamp = temp_humid_sensors.timestamp
group by `timestamp`
order by `timestamp` ASC