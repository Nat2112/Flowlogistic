--Create basic ML to predict duration
CREATE OR REPLACE MODEL
  FlowLogistic.LReg_Model
OPTIONS
  (input_label_cols=['ship_duration'],
    model_type='linear_reg') AS
SELECT
  Departure_Date,	
  Departure_Port,	
  Arrival_Date	,
  Arrival_Port	,
  Vehicle_ID	,
  Ship_Duration
FROM
  `speedy-insight-355300.FlowLogistic.Shipping_Vehicle` SV
left join `speedy-insight-355300.FlowLogistic.Port` DP on DP.PortID=SV.Departure_Port
left join `speedy-insight-355300.FlowLogistic.Port` AP on AP.PortID=SV.Arrival_Port

--Evaluate the model
SELECT * FROM ML.EVALUATE(MODEL `FlowLogistic.LReg_Model`)

--Run prediction against the model
SELECT
  *
FROM
  ML.PREDICT(MODEL FlowLogistic.LReg_Model,
    (
    SELECT
  Departure_Date,	
  Departure_Port,	
  Arrival_Date	,
  Arrival_Port	,
  Vehicle_ID	,
  Ship_Duration
FROM
  `speedy-insight-355300.FlowLogistic.Shipping_Vehicle`
    LIMIT
      100) )
