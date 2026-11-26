#Imports
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, when, lit, concat_ws, row_number
from pyspark.sql.window import Window

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType


# Create Spark session


builder = SparkSession.builder \
    .appName("TransactionPipeline") \
    .master("local[*]") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set(
    "spark.sql.csv.parser.columnPruning.enabled", "false"
)
spark.sparkContext.setLogLevel("ERROR")



# Load configuration


project_path = Path(__file__).resolve().parent

with open(project_path / "config.json", "r") as file:
    config = json.load(file)






source_folder = project_path / config["source_folder"]
target_folder = project_path / config["target_folder"]
rejected_folder = project_path / config["rejected_folder"]
processed_folder = project_path / config["processed_folder"]

file_name = config["file_name"]
primary_key = config["primary_key"]
schema = config["schema"]
deduplication_order = config["deduplication_order"]
late_arrival_hours = config["late_arrival_hours"]
processed_folder.mkdir(parents=True, exist_ok=True)






def process_file(file):

    # Read CSV
    df = spark.read \
        .option("header", True) \
        .csv(file.as_posix())


    # Schema enforcement
    expected_columns = list(schema.keys())
    actual_columns = df.columns

    missing_columns = set(expected_columns) - set(actual_columns)
    extra_columns = set(actual_columns) - set(expected_columns)

    if missing_columns or extra_columns:
        raise ValueError(
            f"Schema mismatch. Missing: {missing_columns}, "
            f"Unexpected: {extra_columns}"
        )

    print("Schema validation passed")

    raw_schema = StructType([
    StructField(column_name, StringType(), True)
    for column_name in actual_columns
        ])

    raw_schema.add("_corrupt_record", StringType(), True)

    df = spark.read \
    .option("header", True) \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(raw_schema) \
    .csv(file.as_posix())


    # Data-type conversion
    for column_name, data_type in schema.items():

        df = df.withColumn(
            column_name + "_validated",
            expr(f"try_cast(`{column_name}` AS {data_type})")
        )


    # Identify invalid records
    errors = [
    when(
        col("_corrupt_record").isNotNull(),
        lit("Malformed CSV record")
    )
        ]

    for column_name in config["required_columns"]:

        errors.append(
            when(
                col(column_name + "_validated").isNull(),
                lit(f"Missing or invalid {column_name}")
            )
        )

    df = df.withColumn(
        "rejection_reason",
        concat_ws(", ", *errors)
    )


    # Separate valid and invalid records
    invalid_df = df.filter(col("rejection_reason") != "")
    valid_df = df.filter(col("rejection_reason") == "")


    # Reconciliation
    print("Source records:", df.count())
    print("Valid records:", valid_df.count())
    print("Invalid records:", invalid_df.count())


    # Quarantine invalid records
    invalid_df.select(
        *schema.keys(),
        "_corrupt_record",
        "rejection_reason"
    ).write.mode("overwrite").parquet(
        (rejected_folder / file.name).as_posix()
    )


    # Select validated columns
    clean_df = valid_df.select(
        *[
            col(column_name + "_validated").alias(column_name)
            for column_name in schema.keys()
        ]
    )


    # Remove duplicates using primary key
    window = Window.partitionBy(
        primary_key
    ).orderBy(
        col(deduplication_order).desc()
    )

    deduplicated_df = clean_df.withColumn(
        "row_num",
        row_number().over(window)
    ).filter(
        col("row_num") == 1
    ).drop("row_num")

    print("Records after deduplication:", deduplicated_df.count())
    

    late_df = deduplicated_df.filter(
    expr(
        f"ingested_at > transaction_at "
        f"+ INTERVAL {late_arrival_hours} HOURS"
    )
    )
    print("Late-arriving records:", late_df.count())


    # Delta target
    target_path = target_folder.as_posix()

    if not DeltaTable.isDeltaTable(spark, target_path):

        deduplicated_df.write \
            .format("delta") \
            .mode("errorifexists") \
            .save(target_path)

        print("Delta table created")

    else:

        delta_table = DeltaTable.forPath(spark, target_path)

        delta_table.alias("t").merge(
            deduplicated_df.alias("s"),
            f"t.{primary_key} = s.{primary_key}"
        ).whenMatchedUpdateAll(
            condition=(
                f"s.{deduplication_order} > "
                f"t.{deduplication_order}"
            )
        ).whenNotMatchedInsertAll().execute()

        print("Delta merge completed")



#  Incremental file processing

files = list(source_folder.glob(file_name))

if not files:
    print("No new files to process")
    spark.stop()
    exit()

for file in files:

    archive_path = processed_folder / file.name

    if archive_path.exists():
        raise FileExistsError(
            f"File already archived: {file.name}"
        )

    print(f"Processing: {file.name}")

    process_file(file)

    # Archive only after successful processing
    file.rename(archive_path)

    print(f"Successfully processed: {file.name}")


# Display final target

target_df = spark.read \
    .format("delta") \
    .load(target_folder.as_posix())

target_df.show(truncate=False)


print("Total target records:", target_df.count())



delta_table = DeltaTable.forPath(
    spark, target_folder.as_posix()
)

#delta_table.history(1).select(
#    "operation",
#    "operationMetrics"
#).show(truncate=False)


spark.read.parquet(
    (rejected_folder / "transactions_005.csv").as_posix()
).show(truncate=False)

spark.stop()