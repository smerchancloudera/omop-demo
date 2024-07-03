
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import *
from variables_create_tables import *



# ###########################################################
#
# VARIABLES
#
# ###########################################################

LIST_TABLES=[]
LIST_TABLES.append(["PERSON",SQL_DDL_PERSON])
LIST_TABLES.append (["VISIT_OCCURRENCE",SQL_DDL_VISIT_OCCURRENCE])
LIST_TABLES.append (["VISIT_DETAIL",SQL_DDL_VISIT_DETAIL])
LIST_TABLES.append (["CONDITION_OCCURRENCE", SQL_DDL_CONDITION_OCCURRENCE])
LIST_TABLES.append (["DRUG_EXPOSURE",SQL_DDL_DRUG_EXPOSURE])
LIST_TABLES.append (["PROCEDURE_OCCURRENCE",SQL_DDL_PROCEDURE_OCCURRENCE])
LIST_TABLES.append (["DEVICE_EXPOSURE",SQL_DDL_DEVICE_EXPOSURE])
LIST_TABLES.append (["MEASUREMENT",SQL_DDL_MEASUREMENT])
LIST_TABLES.append (["OBSERVATION",SQL_DDL_OBSERVATION])
LIST_TABLES.append (["DEATH",SQL_DDL_DEATH])
LIST_TABLES.append (["NOTE",SQL_DDL_NOTE])
LIST_TABLES.append (["NOTE_NLP",SQL_DDL_NOTE_NLP])
LIST_TABLES.append (["SPECIMEN",SQL_DDL_SPECIMEN])
LIST_TABLES.append (["FACT_RELATIONSHIP",SQL_DDL_FACT_RELATIONSHIP])
LIST_TABLES.append (["LOCATION",SQL_DDL_FACT_LOCATION])
LIST_TABLES.append (["CARE_SITE",SQL_DDL_CARE_SITE])
LIST_TABLES.append (["PROVIDER",SQL_DDL_PROVIDER])
LIST_TABLES.append (["PAYER_PLAN_PERIOD",SQL_DDL_PAYER_PLAN_PERIOD])
LIST_TABLES.append (["COST",SQL_DDL_COST])
LIST_TABLES.append (["DRUG_ERA",SQL_DDL_DRUG_ERA])
LIST_TABLES.append (["DOSE_ERA",SQL_DDL_DOSE_ERA])
LIST_TABLES.append (["CONDITION_ERA",SQL_DDL_CONDITION_ERA])
LIST_TABLES.append (["EPISODE",SQL_DDL_EPISODE])
LIST_TABLES.append (["EPISODE_EVENT",SQL_DDL_EPISODE_EVENT])
LIST_TABLES.append (["METADATA",SQL_DDL_METADATA])
LIST_TABLES.append (["CDM_SOURCE",SQL_DDL_CDM_SOURCE])
LIST_TABLES.append (["CONCEPT",SQL_DDL_CONCEPT])
LIST_TABLES.append (["VOCABULARY",SQL_DDL_VOCABULARY])
LIST_TABLES.append (["DOMAIN",SQL_DDL_DOMAIN])
LIST_TABLES.append (["CONCEPT_CLASS",SQL_DDL_CONCEPT_CLASS])
LIST_TABLES.append (["CONCEPT_RELATIONSHIP",SQL_DDL_CONCEPT_RELATIONSHIP])
LIST_TABLES.append (["RELATIONSHIP",SQL_DDL_RELATIONSHIP])
LIST_TABLES.append (["CONCEPT_SYNONYM",SQL_DDL_CONCEPT_SYNONYM])
LIST_TABLES.append (["CONCEPT_ANCESTOR",SQL_DDL_CONCEPT_ANCESTOR])
LIST_TABLES.append (["SOURCE_TO_CONCEPT_MAP",SQL_DDL_SOURCE_TO_CONCEPT_MAP])
LIST_TABLES.append (["DRUG_STRENGHT",SQL_DDL_DRUG_STRENGTH])
LIST_TABLES.append (["COHORT",SQL_DDL_COHORT])
LIST_TABLES.append (["COHORT_DEFINITION",SQL_DDL_COHORT_DEFINITION])

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
# Generate the tables with DDLs the CSV into a Spark DF
#############################################################

# Listing the tables adquired in the list variable.

print("***** TABLES LOADED WITH DDL:")
for duple in LIST_TABLES:
    print ("***** Table name: ", duple[0])

print("****** SELECTING THE CORRECT DATABASE:", DATABASE)
resultados = spark.sql(SQL_DDL_USE_DATABASE)
resultados.show(1)

# Generating the tables
for duple in LIST_TABLES:
   print("****** CREATING TABLE:", duple[0])
   resultados = spark.sql(duple[1])
   resultados.show(1)


# ###########################################################
# CLOSING THE SESSION
#############################################################

print ("\n****** Cerrando sesion spark...\n")

spark.stop


#######


