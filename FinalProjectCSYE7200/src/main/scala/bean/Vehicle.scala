package bean

/**
 * param for bean vehicle
 * @param accident_index
 * @param vehicle_reference
 * @param vehicle_type
 * @param towing_and_articulation
 * @param vehicle_manoeuvre
 * @param vehicle_location_restricted_lane
 * @param junction_location
 * @param skidding_and_overturning
 * @param hit_Object_in_carriageway
 * @param vehicle_leaving_carriageway
 * @param hit_object_off_carriageway
 * @param first_point_of_impact
 * @param was_vehicle_left_hand_drive
 * @param journey_purpose_of_driver
 * @param sex_of_driver
 * @param age_of_driver
 * @param age_band_of_driver
 * @param engine_capacity
 * @param propulsion_code
 * @param age_of_vehicle
 * @param driver_imd_decile
 * @param driver_home_area_type
 * @param vehicle_imd_decile
 */
case class Vehicle(accident_index:String,vehicle_reference:String,vehicle_type:String,towing_and_articulation:String,vehicle_manoeuvre:String,vehicle_location_restricted_lane:String,
                   junction_location:String,skidding_and_overturning:String,hit_Object_in_carriageway:String,vehicle_leaving_carriageway:String,hit_object_off_carriageway:String,
                   first_point_of_impact:String,was_vehicle_left_hand_drive:String,journey_purpose_of_driver:String,sex_of_driver:String,age_of_driver:Int,age_band_of_driver:String,
                   engine_capacity:Int,propulsion_code:String,age_of_vehicle:Int,driver_imd_decile:Int,driver_home_area_type:String,vehicle_imd_decile:String)