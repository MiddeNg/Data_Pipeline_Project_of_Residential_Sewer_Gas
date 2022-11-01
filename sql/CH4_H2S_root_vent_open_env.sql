SELECT timestamp_trunc(`roof_open_environment.roof_open_environment`.`timestamp`, minute) AS `timestamp`, avg(`roof_open_environment.roof_open_environment`.`CH4_concentration`) AS `CH4_concentration`, avg(`roof_open_environment.roof_open_environment`.`H2S_concentration`) AS `H2S_concentration`
FROM `roof_open_environment.roof_open_environment`
GROUP BY `timestamp`
ORDER BY `timestamp` ASC
