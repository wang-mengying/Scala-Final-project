package bean

/**
 * case class of the accident.csv
 * @param accident_index
 * @param location_Easting_OSGR
 * @param location_Northing_OSGR
 * @param longitude
 * @param latitude
 * @param police_Force
 * @param accident_Severity
 * @param number_of_Vehicles
 * @param number_of_Casualties
 * @param date
 * @param day_of_Week
 * @param Time
 * @param local_Authority_District
 * @param local_Authority_Highway
 * @param first_Road_Class
 * @param first_Road_Number
 * @param road_Type
 * @param speed_Limit
 * @param junction_detail
 * @param junction_Control
 * @param second_road_class
 * @param second_road_number
 * @param pedestrian_Crossing_Human_Control
 * @param pedestrian_Crossing_physical_facilities
 * @param light_Conditions
 * @param weather_Conditions
 * @param special_Conditions_at_Site
 * @param carriageway_Hazards
 * @param urban_or_Rural_Area
 * @param did_Police_Officer_Attend_Scene_of_Accident
 * @param LSOA_of_Accident_Location
 */

case class Accident(accident_index:String, location_Easting_OSGR:String,location_Northing_OSGR:String,longitude:String,latitude:String,police_Force:String,
                    accident_Severity:String,number_of_Vehicles:String,number_of_Casualties:String,date:String,day_of_Week:String,Time:String,local_Authority_District:String,
                    local_Authority_Highway:String,first_Road_Class:String,first_Road_Number:String,road_Type:String,speed_Limit:String,junction_detail:String, junction_Control:String,
                    second_road_class:String,second_road_number:String,pedestrian_Crossing_Human_Control:String,pedestrian_Crossing_physical_facilities:String,
                    light_Conditions:String,weather_Conditions:String,special_Conditions_at_Site:String,carriageway_Hazards:String,urban_or_Rural_Area:String,did_Police_Officer_Attend_Scene_of_Accident:String,
                    LSOA_of_Accident_Location:String)