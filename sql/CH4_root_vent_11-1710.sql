with rv1110 as (
  select extract (hour from `timestamp`) AS `timestamp_hour`, avg(`CH4_concentration`) AS `CH4_1110`
  from sensor_data_raw.roof_vent11_10
  group by timestamp_hour
),
rv1210 as (
  select extract (hour from `timestamp`) AS `timestamp_hour`, avg(`CH4_concentration`) AS `CH4_1210`
  from sensor_data_raw.roof_vent12_10
  group by timestamp_hour
),
rv1310 as (
  select extract (hour from `timestamp`) AS `timestamp_hour`, avg(`CH4_concentration`) AS `CH4_1310`
  from sensor_data_raw.roof_vent13_10
  group by timestamp_hour
),
rv1410 as (
  select extract (hour from `timestamp`) AS `timestamp_hour`, avg(`CH4_concentration`) AS `CH4_1410`
  from sensor_data_raw.roof_vent14_10
  group by timestamp_hour
),
rv1510 as (
  select extract (hour from `timestamp`) AS `timestamp_hour`, avg(`CH4_concentration`) AS `CH4_1510`
  from sensor_data_raw.roof_vent15_10
  group by timestamp_hour
),
rv1610 as (
  select extract (hour from `timestamp`) AS `timestamp_hour`, avg(`CH4_concentration`) AS `CH4_1610`
  from sensor_data_raw.roof_vent16_10
  group by timestamp_hour
),
rv1710 as (
  select extract (hour from `timestamp`) AS `timestamp_hour`, avg(`CH4_concentration`) AS `CH4_1710`
  from sensor_data_raw.roof_vent17_10
  group by timestamp_hour
)


SELECT rv1510.timestamp_hour as hour,rv1110.CH4_1110,rv1210.CH4_1210, rv1310.CH4_1310, 
rv1410.CH4_1410 , rv1510.CH4_1510 , rv1610.CH4_1610, rv1710.CH4_1710
FROM rv1510 
    FULL JOIN rv1110 ON rv1510.timestamp_hour = rv1110.timestamp_hour 
    FULL JOIN rv1210 ON rv1510.timestamp_hour = rv1210.timestamp_hour 
    FULL JOIN rv1310 ON rv1510.timestamp_hour = rv1310.timestamp_hour 
    FULL JOIN rv1410 ON rv1510.timestamp_hour = rv1410.timestamp_hour 
    FULL JOIN rv1610 ON rv1510.timestamp_hour = rv1610.timestamp_hour
    FULL JOIN rv1710 ON rv1510.timestamp_hour = rv1710.timestamp_hour 
ORDER BY rv1510.timestamp_hour ASC