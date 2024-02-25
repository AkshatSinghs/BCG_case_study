from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window
from common_utils import utils


# import os
# import sys

# if os.path.exists('src.zip'):
#    sys.path.insert(0, 'src.zip')
# else:
#    sys.path.insert(0, './Code/src')


class accident_analysis:
    def __init__(self, params_path):
        # Read input locations from parameters file
        input_locations = utils.get_params(params_path).get("input_locations")

        # Create dataframes from csv files
        self.primary_person_df = utils.create_df(spark, input_locations.get("primary_person"))
        self.units_df = utils.create_df(spark, input_locations.get("units"))
        self.damages_df = utils.create_df(spark, input_locations.get("damages"))
        self.charges_df = utils.create_df(spark, input_locations.get("charges"))

    def accidents_male_deaths(self, output_location):
        """
        This function finds the no. of accidents where male deaths are greater than 2
        and writes the result to output location
        """
        # Filter for crashes with male deaths
        crashes_with_male_deaths_df = self.primary_person_df.filter(
            (col("prsn_gndr_id") == "MALE") &
            (col("prsn_injry_sev_id") == "KILLED")
        ).groupBy("crash_id").agg(count("*").alias("male_death_count"))

        # Filter for crashes with more than 2 male fatalities
        result_df = crashes_with_male_deaths_df.filter(col("male_death_count") > 2)

        # Write result to output location
        utils.write_csv(result_df, output_location)

    def two_wheeler_crashes(self, output_location):
        """
        This function finds the no. of 2 wheelers booked in accidents
        and writes the result to output location
        """
        # Get units with charges
        booked_units = self.charges_df.select("crash_id", "unit_nbr").distinct()

        # Get two-wheelers with charges
        two_wheelers_booked_df = self.units_df.join(
            booked_units,
            (self.units_df['CRASH_ID'] == booked_units['CRASH_ID']) & (
                        self.units_df['UNIT_NBR'] == booked_units['UNIT_NBR']),
            "inner"
        ).filter(
            (self.units_df["VEH_BODY_STYL_ID"] == 'MOTORCYCLE') | (
                        self.units_df["VEH_BODY_STYL_ID"] == 'POLICE MOTORCYCLE')
        ).select(self.units_df["CRASH_ID"], self.units_df['UNIT_NBR']).distinct().agg(count("*").alias("count_two_wheeler_booked"))

        # Write result to output location
        utils.write_csv(two_wheelers_booked_df, output_location)

    def top_five_vehicle_makes(self, output_location):
        """
        This function finds the top 5 vehicle makes that were in crashes where the driver died and airbags did not deploy
        """
        # Filter primary_person data for drivers with fatal injuries and no airbag deployment
        filtered_primary_person_df = self.primary_person_df.filter(
            (col("prsn_type_id") == "DRIVER") &
            (col("PRSN_INJRY_SEV_ID") == "KILLED") &
            (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")
        ).select("crash_id")

        # Join with units data to fetch vehicle make
        veh_make_df = self.units_df.join(
            filtered_primary_person_df,
            (self.units_df['crash_id'] == filtered_primary_person_df['crash_id']),
            'inner'
        ).select(
            self.units_df["crash_id"],
            "unit_nbr",
            "veh_make_id"
        ).distinct()

        # Get the top five vehicle makes by count
        top_five_veh_make_df = veh_make_df.groupBy("veh_make_id").agg(count("*").alias("cnt")).orderBy("cnt", ascending=False).limit(5)

        # Write the output to the specified location
        utils.write_csv(top_five_veh_make_df, output_location)

    def num_vehicles_licensed_hnr(self, output_location):
        """
        This function finds the no. of vehicles in hit and runs where the driver had a DL
        and writes the result to output location
        """
        # Get distinct units
        units_distinct_df = self.units_df.select("crash_id", "veh_hnr_fl").distinct()

        # Join with primary person df to find crashes where driver was licensed
        licensed_hnr_df = self.primary_person_df.join(
            units_distinct_df,
            self.primary_person_df['crash_id'] == units_distinct_df['crash_id'],
            'inner'
        ).filter(
            (col("prsn_type_id") == "DRIVER") &
            (col("DRVR_LIC_CLS_ID").like('%CLASS%'))
        )

        # Count the occurrences
        count_licensed_hnr = licensed_hnr_df.agg(count("*").alias("count_licensed"))

        utils.write_csv(count_licensed_hnr, output_location)

    def highest_non_female_state(self, output_location):
        """
        This function finds the state with the highest non - female accidents
        """
        # Get distinct crash IDs with female drivers
        female_crash_ids_df = self.primary_person_df.filter(col("prsn_gndr_id") == 'FEMALE').select(
            'crash_id').distinct()

        # Filter out crash IDs with female drivers from primary_person_df
        non_female_drivers_df = self.primary_person_df.join(
            female_crash_ids_df,
            self.primary_person_df['crash_id'] == female_crash_ids_df['crash_id'],
            "left_anti"
        ).select(
            self.primary_person_df["crash_id"],
            'DRVR_LIC_STATE_ID'
        ).distinct().groupBy(
            col('DRVR_LIC_STATE_ID')
        ).agg(
            count('DRVR_LIC_STATE_ID').alias('state_count')
        ).orderBy(
            col("state_count").desc()
        ).limit(1)

        # Write the output to the specified location
        utils.write_csv(non_female_drivers_df, output_location)

    def veh_make_causing_injuries(self, output_location):
        """
        This function finds the 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        # Get distinct crash IDs with injury and death counts
        distinct_cas = self.units_df.filter(col("veh_make_id") != "NA").select("crash_id", "veh_make_id", "TOT_INJRY_CNT",
                                                                          "DEATH_CNT").distinct()

        # Calculate total casualties and group by vehicle make ID
        total_cas_df = distinct_cas.select('crash_id', 'veh_make_id',
                                           (col('TOT_INJRY_CNT') + col('DEATH_CNT')).alias("tot_cas_cnt")) \
            .groupBy("veh_make_id").agg(sum('tot_cas_cnt').alias("sum_cas")).orderBy(col('sum_cas').desc()).limit(5)

        # Get the DataFrame for third to fifth vehicle makes
        third_to_fifth_veh_df = total_cas_df.limit(5).subtract(total_cas_df.limit(2))

        # Write the output to the specified location
        utils.write_csv(third_to_fifth_veh_df, output_location)

    def ethnic_group_per_body_style(self, output_location):
        """
        This function finds top ethnic user group of each unique body style that was involved in crashes
        """
        # Filter units df for valid vehicle body styles
        filtered_units_df = self.units_df.filter(
            col("VEH_BODY_STYL_ID").isin("NA", "OTHER  (EXPLAIN IN NARRATIVE)", "NOT REPORTED") == False
        ).select("crash_id", "unit_nbr", "VEH_BODY_STYL_ID").distinct()

        # Join tables
        ethnc_veh_df = self.primary_person_df.join(
            filtered_units_df,
            (self.primary_person_df['crash_id'] == filtered_units_df['crash_id']) & (
                        self.primary_person_df['unit_nbr'] == filtered_units_df['unit_nbr'])
        ).filter(
            col("prsn_ethnicity_id").isin("NA", "UNKNOWN") == False
        ).select("PRSN_ETHNICITY_ID", "VEH_BODY_STYL_ID")

        # Group by PRSN_ETHNICITY_ID and VEH_BODY_STYL_ID and count occurrences
        ethnc_count = ethnc_veh_df.groupBy("PRSN_ETHNICITY_ID", "VEH_BODY_STYL_ID").agg(count("*").alias("cnt"))

        # Use window function to assign row numbers based on the count
        windowSpec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("cnt").desc())
        ethnc_rank_df = ethnc_count.withColumn("row_num", row_number().over(windowSpec))

        # Filter rows with row number 1
        top_ethnc_df = ethnc_rank_df.filter(col("row_num") == 1).select("PRSN_ETHNICITY_ID", "VEH_BODY_STYL_ID")

        # Write the output to the specified location
        utils.write_csv(top_ethnc_df, output_location)

    def zip_codes_alcohol_contributing(self, output_location):
        """
        This function finds top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
        """
        alcohol_crash_ids_df = self.units_df.filter(
            (col("CONTRIB_FACTR_1_ID").like("%ALCOHOL%")) |
            (col("CONTRIB_FACTR_2_ID").like("%ALCOHOL%")) |
            (col("CONTRIB_FACTR_P1_ID").like("%ALCOHOL%"))
        ).select("crash_id").distinct()

        alcohol_zip_df = self.primary_person_df.join(
            alcohol_crash_ids_df,
            self.primary_person_df['crash_id'] == alcohol_crash_ids_df['crash_id']
        ).select(self.primary_person_df["crash_id"], "drvr_zip", "unit_nbr")

        # Use window function to assign row numbers based on crash_id and drvr_zip
        windowSpec = Window.partitionBy("crash_id", "drvr_zip").orderBy("unit_nbr")
        zip_ranked_df = alcohol_zip_df.withColumn("row_num", row_number().over(windowSpec))

        # Filter rows with row number 1 and non-null drvr_zip
        top_zip_df = zip_ranked_df.filter((col("row_num") == 1) & (col("drvr_zip").isNotNull()))

        # Group by drvr_zip and count occurrences
        result_df = top_zip_df.groupBy("drvr_zip").agg(count("*").alias("tot_count")).orderBy(
            col("tot_count").desc()).limit(5)

        utils.write_csv(result_df, output_location)

    def crashes_no_damage(self, output_location):
        """
        This function finds the distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance.
        """
        # Filter units to find high damage units where insurance was availed
        filtered_units_df = self.units_df.filter(
            (
                    (col("VEH_DMAG_SCL_1_ID").isin("DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST")) |
                    (col("VEH_DMAG_SCL_2_ID").isin("DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"))
            ) &
            (col("FIN_RESP_TYPE_ID").like("%INSURANCE%"))
        )

        # Join with damages to find units where no damage was done
        no_damage_df = filtered_units_df.join(self.damages_df,
                                              filtered_units_df['crash_id'] == self.damages_df['crash_id'], 'leftanti')

        # Count the distinct crash_ids
        count_no_damage_df = no_damage_df.select("crash_id").distinct().agg(count("*").alias("count_no_damage"))

        utils.write_csv(count_no_damage_df, output_location)

    def veh_makes_speeding(self, output_location):
        """
        This function finds the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences
        """
        # Create df for top 25 states contributing to crashes
        top_state_df = self.units_df.filter(~col("veh_lic_state_id").isin("NA")) \
            .select("crash_id", "veh_lic_state_id").distinct() \
            .groupBy("veh_lic_state_id").agg(count("*").alias("state_count")) \
            .orderBy(col("state_count").desc()).limit(25)

        # Create df for top 10 colours of vehicles in crashes
        top_colour_df = self.units_df.filter(col("veh_color_id") != "NA") \
            .select("crash_id", "unit_nbr", "veh_color_id").distinct() \
            .groupBy("veh_color_id").agg(count("*").alias("color_count")) \
            .orderBy(col("color_count").desc()).limit(10)

        # Join primary with units to get vehicle details, charges to find units with speeding offences, top_colour to filter records with colour in top_colour,
        # top_state to filter records with state in top_state, and filter on DRVR_LIC_CLS_ID to find licensed drivers.
        make_count_df = self.primary_person_df.join(
            self.units_df.select("crash_id", "VEH_MAKE_ID", "unit_nbr", "veh_color_id", "veh_lic_state_id").distinct(),
            "crash_id"
        ).join(
            self.charges_df.filter(col("charge").like("%SPEED%")).select("crash_id"),
            "crash_id"
        ).join(
            top_colour_df.select("veh_color_id"),
            "veh_color_id"
        ).join(
            top_state_df.select("veh_lic_state_id"),
            "veh_lic_state_id"
        ).filter(
            col("DRVR_LIC_CLS_ID").like("%CLASS%")
        ).groupBy("VEH_MAKE_ID").agg(count("*").alias("make_count")) \
            .orderBy(col("make_count").desc()).limit(5)

        # Write result to output location
        utils.write_csv(make_count_df, output_location)


if __name__ == '__main__':
    # Initialize sparks session
    spark = SparkSession \
        .builder \
        .appName("USVehicleAccidentAnalysis") \
        .getOrCreate()

    params = "common_params.yml"
    output_locations = utils.get_params(params).get("output_locations")

    # spark.sparkContext.setLogLevel("ERROR")

    aa = accident_analysis(params)

    # 1. Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
    aa.accidents_male_deaths(output_locations.get("output_1"))
    print("Result for analysis 1 written to ", output_locations.get("output_1"))

    # 2.Analysis 2: How many two wheelers are booked for crashes?
    aa.two_wheeler_crashes(output_locations.get("output_2"))
    print("Result for analysis 2 written to ", output_locations.get("output_2"))

    # 3. Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
    aa.top_five_vehicle_makes(output_locations.get("output_3"))
    print("Result for analysis 3 written to ", output_locations.get("output_3"))

    # 4. Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run?
    aa.num_vehicles_licensed_hnr(output_locations.get("output_4"))
    print("Result for analysis 4 written to ", output_locations.get("output_4"))

    # 5. Analysis 5: Which state has highest number of accidents in which females are not involved?
    aa.highest_non_female_state(output_locations.get("output_5"))
    print("Result for analysis 5 written to ", output_locations.get("output_5"))

    # 6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    aa.veh_make_causing_injuries(output_locations.get("output_6"))
    print("Result for analysis 6 written to ", output_locations.get("output_6"))

    # 7. Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    aa.ethnic_group_per_body_style(output_locations.get("output_7"))
    print("Result for analysis 7 written to ", output_locations.get("output_7"))

    # 8. Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    aa.zip_codes_alcohol_contributing(output_locations.get("output_8"))
    print("Result for analysis 8 written to ", output_locations.get("output_8"))

    # 9. Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    aa.crashes_no_damage(output_locations.get("output_9"))
    print("Result for analysis 9 written to ", output_locations.get("output_9"))

    # 10. Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and
    # has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    aa.veh_makes_speeding(output_locations.get("output_10"))
    print("Result for analysis 10 written to ", output_locations.get("output_10"))

    spark.stop()
