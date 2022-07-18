--clean up  the data where departure port=arrival port
Delete from `speedy-insight-355300.FlowLogistic.Shipping_Vehicle`
where departure_port=Arrival_Port

--Clean up the data : set arrival_date = departure_date+ship_duration
Update `FlowLogistic.Shipping_Vehicle`
  set Arrival_date=Departure_Date+ship_Duration
where Arrival_date<>Departure_Date+ship_Duration