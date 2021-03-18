import subprocess

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


# def is_hdfs_file_exist(sparkContext,path):
#     fs = sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(sparkContext._jsc.hadoopConfiguration())
#     return fs.exists(sparkContext._jvm.org.apache.hadoop.fs.Path(path))

def read_parquet_file(spark, file_path):
    df = spark.read.parquet(file_path)
    return df


def is_hdfs_file_exist(path):
    proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', path])
    proc.communicate()
    return proc.returncode == 0