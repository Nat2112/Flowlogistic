--Create enhance ML to predict duration in relation with weather data

CREATE OR REPLACE MODEL
  FlowLogistic.LReg_Model_Enhanced
OPTIONS
  (input_label_cols=['ship_duration'],
    model_type='linear_reg') AS
SELECT
  Departure_Date,	
  Departure_Port,	
  Arrival_Date	,
  Arrival_Port	,
  Vehicle_ID	,
  Ship_Duration,
  fog,
  rain_drizzle,
  thunder,
  snow_depth_mm
FROM
  `speedy-insight-355300.FlowLogistic.Shipping_Vehicle` SV
left join `speedy-insight-355300.FlowLogistic.Port` DP on DP.PortID=SV.Departure_Port
left join `speedy-insight-355300.FlowLogistic.Port` AP on AP.PortID=SV.Arrival_Port
left join `speedy-insight-355300.FlowLogistic.gsod_port` GSOD on DP.gsod_station=GSOD.stn

--Evaluate the model
SELECT * FROM ML.EVALUATE(MODEL `FlowLogistic.LReg_Model_Enhanced`)

--Run prediction against the model
SELECT
  *
FROM
  ML.PREDICT(MODEL FlowLogistic.LReg_Model_Enhanced,
    (
    SELECT
  Departure_Date,	
  Departure_Port,	
  Arrival_Date	,
  Arrival_Port	,
  Vehicle_ID	,
  Ship_Duration,
  fog,
  rain_drizzle,
  thunder,
  snow_depth_mm
FROM
  `speedy-insight-355300.FlowLogistic.Shipping_Vehicle` SV
left join `speedy-insight-355300.FlowLogistic.Port` DP on DP.PortID=SV.Departure_Port
left join `speedy-insight-355300.FlowLogistic.Port` AP on AP.PortID=SV.Arrival_Port
left join `speedy-insight-355300.FlowLogistic.gsod_port` GSOD on DP.gsod_station=GSOD.stn
    LIMIT
      100) )