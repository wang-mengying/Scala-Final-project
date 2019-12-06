package bean

/**
 * param for casualty bean class
 * @param accident_index
 * @param vehicle_reference
 * @param casualty_reference
 * @param casualty_class
 * @param sex_of_casualty
 * @param age_of_casualty
 * @param age_band_of_casualty
 * @param casualty_severity
 * @param pedestrian_location
 * @param pedestrian_movement
 * @param car_passenger
 * @param bus_or_coach_passenger
 * @param pedestrian_road_maintenance_worker
 * @param casualty_type
 * @param casualty_home_area_Type
 * @param casualty_imd_decile
 */
case class Casualty(accident_index:String,vehicle_reference:String,casualty_reference:String,casualty_class:String,sex_of_casualty:String,age_of_casualty:Int,
                    age_band_of_casualty:String,casualty_severity:Int,pedestrian_location:String,pedestrian_movement:String,car_passenger:String,bus_or_coach_passenger:String,
                    pedestrian_road_maintenance_worker:String,casualty_type:String,casualty_home_area_Type:String,casualty_imd_decile:Int)
