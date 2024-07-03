
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import *




# ###########################################################
#
# VARIABLES
#
# ###########################################################
CSV_PATH="./csv/"
DATABASE="omop_cdm"
TABLES_AS_CSV=["vocabulary.csv", "care_site.csv","person.csv","death.csv", "concept_class.csv", "concept.csv",\
               "concept_relationship.csv", "concept_ancestor.csv"]
SCHEMA_PERSON =  StructType([
    StructField("person_id", IntegerType(), True),  #  (string, nullable)
    StructField("gender_concept_id", IntegerType(), True),  #  (integer, nullable)
    StructField("year_of_birth", IntegerType(), True),  #   (double, nullable)
    StructField("month_of_birth", IntegerType(), True), #   (integer, nullable)
    StructField("day_of_birth", IntegerType(), True),  #   (integer, nullable)
    StructField("birth_datetime", TimestampType(), True),  # (integer, nullable)
    #StructField("birth_datetime", StringType(), True),  # (integer, nullable)
    StructField("race_concept_id", IntegerType(), True),  # (integer, nullable)
    StructField("ethnicity_concept_id", IntegerType(), True),  # (integer, nullable)
    StructField("location_id", IntegerType(), True),  # (integer, nullable)
    StructField("provider_id", IntegerType(), True),  # (integer, nullable)
    StructField("care_site_id", IntegerType(), True),  # (integer, nullable)
    StructField("person_source_value", StringType(), True),  # (integer, nullable)
    StructField("gender_source_value", StringType(), True),  # (integer, nullable)
    StructField("gender_source_concept_id", IntegerType(), True),  # (integer, nullable)
    StructField("race_source_value", StringType(), True), # (integer, nullable)
    StructField("race_source_concept_id", IntegerType(), True), # (integer, nullable)
    StructField("ethnicity_source_value", StringType(), True), # (integer, nullable)
    StructField("ethnicity_source_concept_id", IntegerType(), True)  # (integer, nullable)
])
SCHEMA_CARE_SITE = StructType([
    StructField("care_site_id", IntegerType(), True),  #  (string, nullable)
    StructField("care_site_name", StringType(), True),  #  (integer, nullable)
    StructField("place_of_service_concept_id", IntegerType(), True),  #   (double, nullable)
    StructField("location_id", IntegerType(), True), #   (integer, nullable)
    StructField("care_site_source_value", StringType(), True),  #   (integer, nullable)
    StructField("place_of_service_source_value", StringType(), True),  # (integer, nullable)
])

SCHEMA_VOCABULARY = StructType([
    StructField("vocabulary_id", StringType(), True),  #  (string, nullable)
    StructField("vocabulary_name", StringType(), True),  #  (integer, nullable)
    StructField("vocabulary_reference", StringType(), True),  #   (double, nullable)
    StructField("vocabulary_version", StringType(), True), #   (integer, nullable)
    StructField("vocabulary_concept_id", IntegerType(), True),  #   (integer, nullable)

])
SCHEMA_DEATH = StructType([
    StructField("person_id", IntegerType(), True),  #  (string, nullable)
    StructField("death_date", TimestampType(), True),  #  (integer, nullable)
    StructField("cause_concept_id", IntegerType(), True),  # (integer, nullable)
    StructField("death_datetime", TimestampType(), True),  #   (double, nullable)
    StructField("death_type_concept_id", IntegerType(), True), #   (integer, nullable)
    StructField("cause_source_value", StringType(), True),  #   (integer, nullable)
    StructField("cause_source_concept_id", IntegerType(), True),  # (integer, nullable)
])
SCHEMA_CONCEPT_CLASS = StructType([
    StructField("concept_class_id", StringType(), True),  #  (string, nullable)
    StructField("concept_class_name", StringType(), True),  #  (integer, nullable)
    StructField("concept_class_concept_id", IntegerType(), True),  # (integer, nullable)

])
SCHEMA_CONCEPT = StructType([
    StructField("concept_id", IntegerType(), True),  #  (string, nullable)
    StructField("concept_name", StringType(), True),  #  (integer, nullable)
    StructField("domain_id", StringType(), True),  # (integer, nullable)
    StructField("vocabulary_id", StringType(), True),  # (integer, nullable)
    StructField("concept_class_id", StringType(), True),  # (integer, nullable)
    StructField("standard_concept", StringType(), True),  # (integer, nullable)
    StructField("concept_code", StringType(), True),  # (integer, nullable)
    StructField("valid_start_date", TimestampType(), True),  # (integer, nullable)
    StructField("valid_end_date", TimestampType(), True),  # (integer, nullable)
    StructField("invalid_reason", StringType(), True),  # (integer, nullable)

])
SCHEMA_CONCEPT_RELATIONSHIP = StructType([
    StructField("concept_id_1", IntegerType(), True),  #  (string, nullable)
    StructField("concept_id_2", IntegerType(), True),  #  (integer, nullable)
    StructField("relationship_id", IntegerType(), True),  # (integer, nullable)
    StructField("valid_start_date", TimestampType(), True),  # (integer, nullable)
    StructField("valid_end_date", TimestampType(), True),  # (integer, nullable)
    StructField("invalid_reason", StringType(), True),  # (integer, nullable)

])
SCHEMA_CONCEPT_ANCESTOR = StructType([
    StructField("ancestor_concept_id", IntegerType(), True),  #  (string, nullable)
    StructField("descendant_concept_id", IntegerType(), True),  #  (integer, nullable)
    StructField("min_levels_of_separation", IntegerType(), True),  # (integer, nullable)
    StructField("max_levels_of_separation", IntegerType(), True),  # (integer, nullable)

])

# ###########################################################
#
# FUNCTIONS
#
# ###########################################################


# ###########################################################
#
# MAIN SECTION
#
# ###########################################################

# ###########################################################
# Generate the Spark Session
#############################################################

print ("\n***** Creando la sesion Spark ...\n")
spark = (SparkSession\
    .builder.appName("Spark-SQL-Test") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .enableHiveSupport() \
    .getOrCreate())



spark.sparkContext.setLogLevel("ERROR")
print ("\n****** Sesion creada.\n")

# ###########################################################
# Load the CSV into a Spark DF
#############################################################


for csv in TABLES_AS_CSV:
    print("****** Reading CSV: ", csv)

    if csv == "person.csv":
        df_csv = spark.read.csv(CSV_PATH + csv, header=True, schema=SCHEMA_PERSON)
    elif csv== "care_site.csv":
        df_csv = spark.read.csv(CSV_PATH + csv, header=True, schema=SCHEMA_CARE_SITE)
    elif csv=="death.csv":
        df_csv = spark.read.csv(CSV_PATH + csv, header=True, schema=SCHEMA_DEATH)
    elif csv=="vocabulary.csv":
        df_csv = spark.read.csv(CSV_PATH+csv, header=True, schema=SCHEMA_VOCABULARY)
    elif csv == "concept_class.csv":
        df_csv = spark.read.csv(CSV_PATH + csv, header=True, schema=SCHEMA_CONCEPT_CLASS)
    elif csv == "concept.csv":
        df_csv = spark.read.csv(CSV_PATH + csv, header=True, schema=SCHEMA_CONCEPT)
    elif csv == "concept_relationship.csv":
        df_csv = spark.read.csv(CSV_PATH + csv, header=True, schema=SCHEMA_CONCEPT_RELATIONSHIP)
    elif csv == "concept_ancestor.csv":
        df_csv = spark.read.csv(CSV_PATH + csv, header=True, schema=SCHEMA_CONCEPT_ANCESTOR)
    else:
        print ("****** THERE IS A TABLE THAT DONT BELONG TO OMOP: ", csv)
        print ("****** EXITING TO PREVENT MORE ERRORS")
        exit(-1)
    print("\n****** CSV Loaded: ", csv)
    print("****** Showing first 10 registers:\n")

    print("****** The dftypes is this:")
    print(df_csv.dtypes)

    print("")
    df_csv.show (10)

    # #######################################################
    # Load the df into the iceberg table
    # Table name is the csv name except .csv
    # #######################################################
    print("****** Truncating the destination table to clear data:\n")
    SQL_sentence = "truncate table "+ DATABASE+"."+csv[:-4]
    resultados = spark.sql(SQL_sentence)
    resultados.show(1)

    table_name=DATABASE+"."+csv[:-4]
    print ("\n****** We are going to write into table: ", table_name)
    print ()
    df_csv.write.format("iceberg").mode("append").saveAsTable(table_name)
    print("\n****** Registers loaded into: ", table_name)
    print()


print ("\n****** Cerrando sesion spark...\n")

spark.stop
