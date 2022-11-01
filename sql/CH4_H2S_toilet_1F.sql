SELECT timestamp_trunc(`1st_floor_toilet.1st_floor_toilet`.`timestamp`, minute) AS `timestamp`, avg(`1st_floor_toilet.1st_floor_toilet`.`CH4_concentration`) AS `CH4_concentration`, avg(`1st_floor_toilet.1st_floor_toilet`.`H2S_concentration`) AS `H2S_concentration`
FROM `1st_floor_toilet.1st_floor_toilet`
GROUP BY `timestamp`
ORDER BY `timestamp` ASC