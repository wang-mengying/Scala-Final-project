package bean

/**
 * Bean class for vehicle.csv
 * @param accident_Index
 * @param vehicle_Reference
 * @param vehicle_Type
 * @param towing_and_Articulation
 * @param vehicle_Manoeuvre
 * @param vehicle_Location_Restricted_Lane
 * @param junction_Location
 * @param skidding_and_Overturning
 * @param hit_Object_in_Carriageway
 * @param vehicle_Leaving_Carriageway
 * @param hit_Object_off_Carriageway
 * @param first_Point_of_Impact
 * @param was_Vehicle_Left_Hand_Drive
 * @param journey_Purpose_of_Driver
 * @param sex_of_Driver
 * @param age_of_Driver
 * @param age_Band_of_Driver
 * @param engine_Capacity
 * @param propulsion_Code
 * @param age_of_Vehicle
 * @param driver_IMD_Decile
 * @param driver_Home_Area_Type
 * @param vehicle_IMD_Decile
 */
case class Vehicle(accident_Index:String,vehicle_Reference:String,vehicle_Type:String,towing_and_Articulation:String,vehicle_Manoeuvre:String,vehicle_Location_Restricted_Lane:String,
                   junction_Location:String,skidding_and_Overturning:String,hit_Object_in_Carriageway:String,vehicle_Leaving_Carriageway:String,hit_Object_off_Carriageway:String,
                   first_Point_of_Impact:String,was_Vehicle_Left_Hand_Drive:String,journey_Purpose_of_Driver:String,sex_of_Driver:String,age_of_Driver:String,age_Band_of_Driver:String,
                   engine_Capacity:String,propulsion_Code:String,age_of_Vehicle:String,driver_IMD_Decile:String,driver_Home_Area_Type:String,vehicle_IMD_Decile:String)
