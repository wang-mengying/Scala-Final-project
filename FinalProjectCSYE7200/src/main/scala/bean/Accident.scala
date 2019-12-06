package bean

/**
 *
 * @param accident_index
 * @param location_easting_osgr
 * @param location_northing_osgr
 * @param longitude
 * @param latitude
 * @param police_force
 * @param accident_severity
 * @param number_of_vehicles
 * @param number_of_casualties
 * @param date
 * @param day_of_week
 * @param time
 * @param local_authority_district
 * @param local_authority_highway
 * @param first_road_class
 * @param first_road_number
 * @param road_type
 * @param speed_limit
 * @param junction_detail
 * @param junction_control
 * @param second_road_class
 * @param second_road_number
 * @param pedestrian_crossing_human_control
 * @param pedestrian_crossing_physical_facilities
 * @param light_conditions
 * @param weather_conditions
 * @param special_conditions_at_site
 * @param carriageway_hazards
 * @param urban_or_rural_area
 * @param did_police_officer_attend_scene_of_accident
 * @param lsoa_of_accident_location
 */
case class Accident(accident_index:String, location_easting_osgr:String,location_northing_osgr:String,longitude:String,latitude:String,police_force:String,
                    accident_severity:Int,number_of_vehicles:Int,number_of_casualties:Int,date:String,day_of_week:String,time:String,local_authority_district:String,
                    local_authority_highway:String,first_road_class:String,first_road_number:String,road_type:String,speed_limit:Int,junction_detail:String, junction_control:String,
                    second_road_class:String,second_road_number:String,pedestrian_crossing_human_control:String,pedestrian_crossing_physical_facilities:String,
                    light_conditions:String,weather_conditions:String,special_conditions_at_site:String,carriageway_hazards:String,urban_or_rural_area:String,did_police_officer_attend_scene_of_accident:String,
                    lsoa_of_accident_location:String)