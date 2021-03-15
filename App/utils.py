def read_csv_file(spark, file_path, schema=None):
    if schema is not None:
        df = spark.read.format('csv') \
            .schema(schema) \
            .option("header", "true") \
            .option("sep", ",") \
            .load(file_path)

    else:
        df = spark.read.format('csv') \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .option("sep", ",") \
            .load(file_path)
    return df
