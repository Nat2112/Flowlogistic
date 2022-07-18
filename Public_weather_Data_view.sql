--create subset view of weather data
create or replace view `speedy-insight-355300.FlowLogistic.gsod_port`
as
select
    cast (stn as int) as stn,
    date(cast(year as int64),cast(mo as int64),cast(da as int64)) as date,
    case when temp = 9999.9 then null else round((temp - 32) * (5/9),1) end as temperature_celcius_mean,
    case when max = 9999.9 then null else round((max - 32) * (5/9),1) end as temperature_celcius_max,
    case when min = 9999.9 then null else round((min - 32) * (5/9),1) end as temperature_celcius_min,
    case when visib = 999.9 then null else round(visib * 1.609,1) end as visibility_km,
    case when wdsp = '999.9' then null else round(cast(wdsp as float64) * 1.852,1) end as wind_speed_kmh,
    case when prcp = 99.99 then null else round(prcp * 25.4,1) end as precipitation_mm,
    case when sndp = 999.9 then 0.0 else round(sndp * 25.4,1) end as snow_depth_mm,
    fog,
    rain_drizzle,
    hail,
    thunder
from
    `bigquery-public-data.noaa_gsod.gsod*`   
where
    _table_suffix between '2016' and '2017'
    and date(cast(year as int64),cast(mo as int64),cast(da as int64)) >= '2016-08-01'
    and date(cast(year as int64),cast(mo as int64),cast(da as int64)) <= '2017-03-01'
    and stn in (
        '720718',
'720722',
'720914',
'722041',
'722253',
'722334',
'722403',
'720953',
'722406',
'997848',
'720352',
'720467',
'720468',
'720739',
'720913'
    )
order by
    date desc