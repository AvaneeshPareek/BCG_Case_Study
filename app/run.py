from pyspark.sql import SparkSession
from src.crash_accidents_analysis import USVehicleAccidentAnalysis
import configparser

if __name__ == "__main__":
    # Initialize spark session
    spark = SparkSession.builder.appName("USVehicleAccidentAnalysis").getOrCreate()

    config_file_name = "../config.ini"
    config = configparser.ConfigParser()
    config.read(config_file_name)
    spark.sparkContext.setLogLevel("ERROR")
    output_file_paths = config["OUTPUT_PATH"]
    file_format = config["FILE_FORMAT"]

    usvaa = USVehicleAccidentAnalysis(spark, config)

    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2?
    print(
        "1. Result:",
        usvaa.count_male_accidents(output_file_paths["1"], file_format["Output"]),
    )

    # 2. How many two-wheelers are booked for crashes?
    print(
        "2. Result:",
        usvaa.count_2_wheeler_accidents(
            output_file_paths["2"], file_format["Output"]
        ),
    )

    # 3. Determine the Top 5 Vehicles made of the cars present in the crashes in which a driver died and Airbags did
    # not deploy.
    print(
        "3. Result:",
        usvaa.top_5_vehicle_makes_for_fatal_crashes_without_airbags(
            output_file_paths["3"], file_format["Output"]
        ),
    )

    # 4. Determine the number of Vehicles with a driver having valid licences involved in hit-and-run?
    print(
        "4. Result:",
        usvaa.count_hit_and_run_with_valid_licenses(
            output_file_paths["4"], file_format["Output"]
        ),
    )

    # 5. Which state has the highest number of accidents in which females are not involved?
    print(
        "5. Result:",
        usvaa.get_state_with_no_female_accident(
            output_file_paths["5"], file_format["Output"]
        ),
    )

    # 6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print(
        "6. Result:",
        usvaa.get_top_vehicle_contributing_to_injuries(
            output_file_paths["6"], file_format["Output"]
        ),
    )

    # 7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("7. Result:")
    usvaa.get_top_ethnic_ug_crash_for_each_body_style(
        output_file_paths["7"], file_format["Output"]
    ).show(truncate=False)

    # 8. Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the
    # contributing factor to a crash (Use Driver Zip Code)
    print(
        "8. Result:",
        usvaa.get_top_5_zip_codes_with_alcohols_as_cf_for_crash(
            output_file_paths["8"], file_format["Output"]
        ),
    )

    # 9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above
    # 4 and car avails Insurance
    print(
        "9. Result:",
        usvaa.get_crash_ids_with_no_damage(
            output_file_paths["9"], file_format["Output"]
        ),
    )

    # 10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed
    # Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
    # offenses (to be deduced from the data)
    print(
        "10. Result:",
        usvaa.get_top_5_vehicle_brand(
            output_file_paths["10"], file_format["Output"]
        ),
    )

    spark.stop()