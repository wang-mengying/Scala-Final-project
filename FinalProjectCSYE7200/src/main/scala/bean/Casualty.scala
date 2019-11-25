package bean

/**
 * Bean class for Casualty.csv
 * @param accident_Index
 * @param vehicle_Reference
 * @param casualty_Reference
 * @param casualty_Class
 * @param sex_of_Casualty
 * @param age_of_Casualty
 * @param age_Band_of_Casualty
 * @param casualty_Severity
 * @param pedestrian_Location
 * @param pedestrian_Movement
 * @param car_Passenger
 * @param bus_or_Coach_Passenger
 * @param pedestrian_Road_Maintenance_Worker
 * @param casualty_Type
 * @param casualty_Home_Area_Type
 * @param casualty_IMD_Decile
 */

case class Casualty(accident_Index:String,vehicle_Reference:String,casualty_Reference:String,casualty_Class:String,sex_of_Casualty:String,age_of_Casualty:String,
                    age_Band_of_Casualty:String,casualty_Severity:String,pedestrian_Location:String,pedestrian_Movement:String,car_Passenger:String,bus_or_Coach_Passenger:String,
                    pedestrian_Road_Maintenance_Worker:String,casualty_Type:String,casualty_Home_Area_Type:String,casualty_IMD_Decile:String)
