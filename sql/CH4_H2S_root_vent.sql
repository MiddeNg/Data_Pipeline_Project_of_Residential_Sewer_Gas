SELECT timestamp_trunc(`roof_vent.roof_vent`.`timestamp`, minute) AS `timestamp`, avg(`roof_vent.roof_vent`.`CH4_concentration`) AS `CH4`, avg(`roof_vent.roof_vent`.`H2S_concentration`) AS `H2S`
FROM `roof_vent.roof_vent`
WHERE (`roof_vent.roof_vent`.`timestamp` >= timestamp "2022-10-11 00:00:00 GMT"
   AND `roof_vent.roof_vent`.`timestamp` < timestamp "2022-10-18 00:00:00 GMT")
GROUP BY `timestamp`
ORDER BY `timestamp` ASC
