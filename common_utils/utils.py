import yaml
def get_params(input_location):
    """
    This function reads the parameters file
    """
    with open(input_location, "r") as f:
        return yaml.safe_load(f)

def create_df(spark, input_location):
    """
    This function reads a csv file and creates a dataframe on the file
    """
    return spark.read.format("csv").option("header","true").option("inferschema","false").load(input_location)

def write_csv(df, output_location):
    """
    This function writes a parquet from a dataframe to the given output location
    """
    # df = df.coalesce(1)
    df.write.format("csv").mode('overwrite').option("header", "true").save(output_location)

