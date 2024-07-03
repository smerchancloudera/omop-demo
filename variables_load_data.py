# ###########################################################
#
# VARIABLES
#
# ###########################################################
CSV_PATH="./csv/"  #path where the CSV are (normally under HDFS /user/youruser/csv
DATABASE="omop_cdm"  #The destination database name

#Now the tables to load with their Struct type

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



