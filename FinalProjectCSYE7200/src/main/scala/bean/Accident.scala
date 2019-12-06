package bean

case class Accident(accident_index:String, location_easting_osgr:String,location_northing_osgr:String,longitude:String,latitude:String,police_force:String,
                    accident_severity:String,number_of_vehicles:String,number_of_casualties:String,date:String,day_of_week:String,time:String,local_authority_district:String,
                    local_authority_highway:String,first_road_class:String,first_road_number:String,road_type:String,speed_limit:String,junction_detail:String, junction_control:String,
                    second_road_class:String,second_road_number:String,pedestrian_crossing_human_control:String,pedestrian_crossing_physical_facilities:String,
                    light_conditions:String,weather_conditions:String,special_conditions_at_site:String,carriageway_hazards:String,urban_or_rural_area:String,did_police_officer_attend_scene_of_accident:String,
                    lsoa_of_accident_location:String)