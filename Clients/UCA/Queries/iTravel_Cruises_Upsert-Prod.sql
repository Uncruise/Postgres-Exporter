select 
	cat.cruise_code as Cruise_Id,
	cat.departure_date as Cruise_Date, 
	cat.region_code as Destination_Code, 
	cat.itin_code as Itinerary_Code, 
	itin.itinerary_name as Itinerary_Description, 
	cat.lifecycle_status as Closed_Level, 
	cat.departure_port as Itinerary_Embark, 
	cat.arrival_port as Itinerary_Disembark, 
	cat.lifecycle_status as Cruise_LifeCycle,
	cat.status as Cruise_Status

from itrvl_rpt.itrvl_cru_catalogue_vw cat 
	inner join itrvl_rpt.itrvl_cru_itin_defn_vw itin on cat.itin_code = itin.itinerary_code 
	
where cat.departure_date > now() and (
    (cat.departure_date >= '9/1/2023' and cat.region_code <> 'AKA') or cat.departure_date >= '1/1/2024')
order by cat.departure_date asc