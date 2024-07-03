
# ###########################################################
#
# VARIABLES
#
# ###########################################################
DATABASE="omop_cdm_test"  #the database where the tables will be generated
SQL_DDL_USE_DATABASE="USE "+DATABASE+";" #first SQL sentence to select that database
SQL_DDL_PERSON="CREATE TABLE person (\
			person_id INTEGER,\
			gender_concept_id INTEGER,\
			year_of_birth INT,\
			month_of_birth integer ,\
			day_of_birth integer ,\
			birth_datetime TIMESTAMP,\
			race_concept_id INT,\
			ethnicity_concept_id INT,\
			location_id integer ,\
			provider_id integer ,\
			care_site_id integer ,\
			person_source_value string,\
			gender_source_value string,\
			gender_source_concept_id integer ,\
			race_source_value string,\
			race_source_concept_id integer ,\
			ethnicity_source_value string,\
			ethnicity_source_concept_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_VISIT_OCCURRENCE="CREATE TABLE visit_occurrence (\
			visit_occurrence_id INT,\
			person_id INT,\
			visit_concept_id INT,\
			visit_start_date TIMESTAMP,\
			visit_start_datetime TIMESTAMP,\
			visit_end_date TIMESTAMP,\
			visit_end_datetime TIMESTAMP,\
			visit_type_concept_id INT,\
			provider_id integer ,\
			care_site_id integer ,\
			visit_source_value string,\
			visit_source_concept_id integer ,\
			admitted_from_concept_id integer ,\
			admitted_from_source_value string,\
			discharged_to_concept_id integer ,\
			discharged_to_source_value string,\
			preceding_visit_occurrence_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_VISIT_DETAIL="\
CREATE TABLE visit_detail (\
			visit_detail_id INT,\
			person_id INT,\
			visit_detail_concept_id INT,\
			visit_detail_start_date TIMESTAMP,\
			visit_detail_start_datetime TIMESTAMP,\
			visit_detail_end_date TIMESTAMP,\
			visit_detail_end_datetime TIMESTAMP,\
			visit_detail_type_concept_id INT,\
			provider_id integer ,\
			care_site_id integer ,\
			visit_detail_source_value string,\
			visit_detail_source_concept_id Integer ,\
			admitted_from_concept_id Integer ,\
			admitted_from_source_value string,\
			discharged_to_source_value string,\
			discharged_to_concept_id integer ,\
			preceding_visit_detail_id integer ,\
			parent_visit_detail_id integer ,\
			visit_occurrence_id INT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_CONDITION_OCCURRENCE="\
CREATE TABLE condition_occurrence (\
			condition_occurrence_id INT,\
			person_id INT,\
			condition_concept_id INT,\
			condition_start_date TIMESTAMP,\
			condition_start_datetime TIMESTAMP,\
			condition_end_date TIMESTAMP,\
			condition_end_datetime TIMESTAMP,\
			condition_type_concept_id INT,\
			condition_status_concept_id integer ,\
			stop_reason string,\
			provider_id integer ,\
			visit_occurrence_id integer ,\
			visit_detail_id integer ,\
			condition_source_value string,\
			condition_source_concept_id integer ,\
			condition_status_source_value string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_DRUG_EXPOSURE="\
CREATE TABLE drug_exposure (\
			drug_exposure_id INT,\
			person_id INT,\
			drug_concept_id INT,\
			drug_exposure_start_date TIMESTAMP,\
			drug_exposure_start_datetime TIMESTAMP,\
			drug_exposure_end_date TIMESTAMP,\
			drug_exposure_end_datetime TIMESTAMP,\
			verbatim_end_date TIMESTAMP,\
			drug_type_concept_id INT,\
			stop_reason string,\
			refills integer ,\
			quantity FLOAT,\
			days_supply integer ,\
			sig string,\
			route_concept_id integer ,\
			lot_number string,\
			provider_id integer ,\
			visit_occurrence_id integer ,\
			visit_detail_id integer ,\
			drug_source_value string,\
			drug_source_concept_id integer ,\
			route_source_value string,\
			dose_unit_source_value string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_PROCEDURE_OCCURRENCE="\
CREATE TABLE procedure_occurrence (\
			procedure_occurrence_id INT,\
			person_id INT,\
			procedure_concept_id INT,\
			procedure_date TIMESTAMP,\
			procedure_datetime TIMESTAMP,\
			procedure_end_date TIMESTAMP,\
			procedure_end_datetime TIMESTAMP,\
			procedure_type_concept_id INT,\
			modifier_concept_id integer ,\
			quantity integer ,\
			provider_id integer ,\
			visit_occurrence_id integer ,\
			visit_detail_id integer ,\
			procedure_source_value string,\
			procedure_source_concept_id integer ,\
			modifier_source_value string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_DEVICE_EXPOSURE="\
CREATE TABLE device_exposure (\
			device_exposure_id INT,\
			person_id INT,\
			device_concept_id INT,\
			device_exposure_start_date TIMESTAMP,\
			device_exposure_start_datetime TIMESTAMP,\
			device_exposure_end_date TIMESTAMP,\
			device_exposure_end_datetime TIMESTAMP,\
			device_type_concept_id INT,\
			unique_device_id string,\
			production_id string,\
			quantity integer ,\
			provider_id integer ,\
			visit_occurrence_id integer ,\
			visit_detail_id integer ,\
			device_source_value string,\
			device_source_concept_id integer ,\
			unit_concept_id integer ,\
			unit_source_value string,\
			unit_source_concept_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_MEASUREMENT="\
CREATE TABLE measurement (\
			measurement_id INT,\
			person_id INT,\
			measurement_concept_id INT,\
			measurement_date TIMESTAMP,\
			measurement_datetime TIMESTAMP,\
			measurement_time string,\
			measurement_type_concept_id INT,\
			operator_concept_id integer ,\
			value_as_number FLOAT,\
			value_as_concept_id integer ,\
			unit_concept_id integer ,\
			range_low FLOAT,\
			range_high FLOAT,\
			provider_id integer ,\
			visit_occurrence_id integer ,\
			visit_detail_id integer ,\
			measurement_source_value string,\
			measurement_source_concept_id integer ,\
			unit_source_value string,\
			unit_source_concept_id integer ,\
			value_source_value string,\
			measurement_event_id integer ,\
			meas_event_field_concept_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_OBSERVATION="\
CREATE TABLE observation (\
			observation_id INT,\
			person_id INT,\
			observation_concept_id INT,\
			observation_date TIMESTAMP,\
			observation_datetime TIMESTAMP,\
			observation_type_concept_id INT,\
			value_as_number FLOAT,\
			value_as_string string,\
			value_as_concept_id Integer ,\
			qualifier_concept_id integer ,\
			unit_concept_id integer ,\
			provider_id integer ,\
			visit_occurrence_id integer ,\
			visit_detail_id integer ,\
			observation_source_value string,\
			observation_source_concept_id integer ,\
			unit_source_value string,\
			qualifier_source_value string,\
			value_source_value string,\
			observation_event_id integer ,\
			obs_event_field_concept_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_DEATH="\
CREATE TABLE death (\
			person_id INT,\
			death_date TIMESTAMP,\
			death_datetime TIMESTAMP,\
			death_type_concept_id integer ,\
			cause_concept_id integer ,\
			cause_source_value string,\
			cause_source_concept_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_NOTE="\
CREATE TABLE note (\
			note_id INT,\
			person_id INT,\
			note_date TIMESTAMP,\
			note_datetime TIMESTAMP,\
			note_type_concept_id INT,\
			note_class_concept_id INT,\
			note_title string,\
			note_text string,\
			encoding_concept_id INT,\
			language_concept_id INT,\
			provider_id integer ,\
			visit_occurrence_id integer ,\
			visit_detail_id integer ,\
			note_source_value string,\
			note_event_id integer ,\
			note_event_field_concept_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_NOTE_NLP="\
CREATE TABLE note_nlp (\
			note_nlp_id INT,\
			note_id INT,\
			section_concept_id integer ,\
			snippet string,\
			`offset` string,\
			lexical_variant string,\
			note_nlp_concept_id integer ,\
			note_nlp_source_concept_id integer ,\
			nlp_system string,\
			nlp_date TIMESTAMP,\
			nlp_datetime TIMESTAMP,\
			term_exists string,\
			term_temporal string,\
			term_modifiers string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_SPECIMEN="\
CREATE TABLE specimen (\
			specimen_id INT,\
			person_id INT,\
			specimen_concept_id INT,\
			specimen_type_concept_id INT,\
			specimen_date TIMESTAMP,\
			specimen_datetime TIMESTAMP,\
			quantity FLOAT,\
			unit_concept_id integer ,\
			anatomic_site_concept_id integer ,\
			disease_status_concept_id integer ,\
			specimen_source_id string,\
			specimen_source_value string,\
			unit_source_value string,\
			anatomic_site_source_value string,\
			disease_status_source_value string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_FACT_RELATIONSHIP="\
CREATE TABLE fact_relationship (\
			domain_concept_id_1 INT,\
			fact_id_1 INT,\
			domain_concept_id_2 INT,\
			fact_id_2 INT,\
			relationship_concept_id INT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_FACT_LOCATION="\
CREATE TABLE `location` (\
			location_id INT,\
			address_1 string,\
			address_2 string,\
			city string,\
			state string,\
			zip string,\
			county string,\
			location_source_value string,\
			country_concept_id integer ,\
			country_source_value string,\
			latitude FLOAT,\
			longitude FLOAT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_CARE_SITE="\
CREATE TABLE care_site (\
			care_site_id INT,\
			care_site_name string,\
			place_of_service_concept_id integer ,\
			location_id integer ,\
			care_site_source_value string,\
			place_of_service_source_value string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_PROVIDER="\
CREATE TABLE provider (\
			provider_id INT,\
			provider_name string,\
			npi string,\
			dea string,\
			specialty_concept_id integer ,\
			care_site_id integer ,\
			year_of_birth integer ,\
			gender_concept_id integer ,\
			provider_source_value string,\
			specialty_source_value string,\
			specialty_source_concept_id integer ,\
			gender_source_value string,\
			gender_source_concept_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_PAYER_PLAN_PERIOD="\
CREATE TABLE payer_plan_period (\
			payer_plan_period_id INT,\
			person_id INT,\
			payer_plan_period_start_date TIMESTAMP,\
			payer_plan_period_end_date TIMESTAMP,\
			payer_concept_id integer ,\
			payer_source_value string,\
			payer_source_concept_id integer ,\
			plan_concept_id integer ,\
			plan_source_value string,\
			plan_source_concept_id integer ,\
			sponsor_concept_id integer ,\
			sponsor_source_value string,\
			sponsor_source_concept_id integer ,\
			family_source_value string,\
			stop_reason_concept_id integer ,\
			stop_reason_source_value string,\
			stop_reason_source_concept_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"
SQL_DDL_COST="\
CREATE TABLE cost (\
			cost_id INT,\
			cost_event_id INT,\
			cost_domain_id string,\
			cost_type_concept_id INT,\
			currency_concept_id integer ,\
			total_charge FLOAT,\
			total_cost FLOAT,\
			total_paid FLOAT,\
			paid_by_payer FLOAT,\
			paid_by_patient FLOAT,\
			paid_patient_copay FLOAT,\
			paid_patient_coinsurance FLOAT,\
			paid_patient_deductible FLOAT,\
			paid_by_primary FLOAT,\
			paid_ingredient_cost FLOAT,\
			paid_dispensing_fee FLOAT,\
			payer_plan_period_id integer ,\
			amount_allowed FLOAT,\
			revenue_code_concept_id integer ,\
			revenue_code_source_value string,\
			drg_concept_id integer ,\
			drg_source_value string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_DRUG_ERA="\
CREATE TABLE drug_era (\
			drug_era_id INT,\
			person_id INT,\
			drug_concept_id INT,\
			drug_era_start_date TIMESTAMP,\
			drug_era_end_date TIMESTAMP,\
			drug_exposure_count integer ,\
			gap_days integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_DOSE_ERA="\
CREATE TABLE dose_era (\
			dose_era_id INT,\
			person_id INT,\
			drug_concept_id INT,\
			unit_concept_id INT,\
			dose_value FLOAT,\
			dose_era_start_date TIMESTAMP,\
			dose_era_end_date TIMESTAMP )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_CONDITION_ERA="\
CREATE TABLE condition_era (\
			condition_era_id INT,\
			person_id INT,\
			condition_concept_id INT,\
			condition_era_start_date TIMESTAMP,\
			condition_era_end_date TIMESTAMP,\
			condition_occurrence_count integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_EPISODE="\
CREATE TABLE episode (\
			episode_id INT,\
			person_id INT,\
			episode_concept_id INT,\
			episode_start_date TIMESTAMP,\
			episode_start_datetime TIMESTAMP,\
			episode_end_date TIMESTAMP,\
			episode_end_datetime TIMESTAMP,\
			episode_parent_id integer ,\
			episode_number integer ,\
			episode_object_concept_id INT,\
			episode_type_concept_id INT,\
			episode_source_value string,\
			episode_source_concept_id integer  )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_EPISODE_EVENT="\
CREATE TABLE episode_event (\
			episode_id INT,\
			event_id INT,\
			episode_event_field_concept_id INT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_METADATA="\
CREATE TABLE `metadata` (\
			metadata_id INT,\
			metadata_concept_id INT,\
			metadata_type_concept_id INT,\
			name string,\
			value_as_string string,\
			value_as_concept_id integer ,\
			value_as_number FLOAT,\
			metadata_date TIMESTAMP,\
			metadata_datetime TIMESTAMP )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_CDM_SOURCE="\
CREATE TABLE cdm_source (\
			cdm_source_name string,\
			cdm_source_abbreviation string,\
			cdm_holder string,\
			source_description string,\
			source_documentation_reference string,\
			cdm_etl_reference string,\
			source_release_date TIMESTAMP,\
			cdm_release_date TIMESTAMP,\
			cdm_version string,\
			cdm_version_concept_id INT,\
			vocabulary_version string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_CONCEPT="\
CREATE TABLE concept (\
			concept_id INT,\
			concept_name string,\
			domain_id string,\
			vocabulary_id string,\
			concept_class_id string,\
			standard_concept string,\
			concept_code string,\
			valid_start_date TIMESTAMP,\
			valid_end_date TIMESTAMP,\
			invalid_reason string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_VOCABULARY="\
CREATE TABLE vocabulary (\
			vocabulary_id string,\
			vocabulary_name string,\
			vocabulary_reference string,\
			vocabulary_version string,\
			vocabulary_concept_id INT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_DOMAIN="\
CREATE TABLE domain (\
			domain_id string,\
			domain_name string,\
			domain_concept_id INT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_CONCEPT_CLASS="\
CREATE TABLE concept_class (\
			concept_class_id string,\
			concept_class_name string,\
			concept_class_concept_id INT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_CONCEPT_RELATIONSHIP="\
CREATE TABLE concept_relationship (\
			concept_id_1 INT,\
			concept_id_2 INT,\
			relationship_id string,\
			valid_start_date TIMESTAMP,\
			valid_end_date TIMESTAMP,\
			invalid_reason string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_RELATIONSHIP="\
CREATE TABLE relationship (\
			relationship_id string,\
			relationship_name string,\
			is_hierarchical string,\
			defines_ancestry string,\
			reverse_relationship_id string,\
			relationship_concept_id INT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_CONCEPT_SYNONYM="\
CREATE TABLE concept_synonym (\
			concept_id INT,\
			concept_synonym_name string,\
			language_concept_id INT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_CONCEPT_ANCESTOR="\
CREATE TABLE concept_ancestor (\
			ancestor_concept_id INT,\
			descendant_concept_id INT,\
			min_levels_of_separation INT,\
			max_levels_of_separation INT )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_SOURCE_TO_CONCEPT_MAP="\
CREATE TABLE source_to_concept_map (\
			source_code string,\
			source_concept_id INT,\
			source_vocabulary_id string,\
			source_code_description string,\
			target_concept_id INT,\
			target_vocabulary_id string,\
			valid_start_date TIMESTAMP,\
			valid_end_date TIMESTAMP,\
			invalid_reason string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_DRUG_STRENGTH="\
CREATE TABLE drug_strength (\
			drug_concept_id INT,\
			ingredient_concept_id INT,\
			amount_value FLOAT,\
			amount_unit_concept_id integer ,\
			numerator_value FLOAT,\
			numerator_unit_concept_id integer ,\
			denominator_value FLOAT,\
			denominator_unit_concept_id integer ,\
			box_size integer ,\
			valid_start_date TIMESTAMP,\
			valid_end_date TIMESTAMP,\
			invalid_reason string )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"


SQL_DDL_COHORT="\
CREATE TABLE cohort (\
			cohort_definition_id INT,\
			subject_id INT,\
			cohort_start_date TIMESTAMP,\
			cohort_end_date TIMESTAMP )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ;"

SQL_DDL_COHORT_DEFINITION="\
CREATE TABLE cohort_definition (\
			cohort_definition_id INT,\
			cohort_definition_name string,\
			cohort_definition_description string,\
			definition_type_concept_id INT,\
			cohort_definition_syntax string,\
			subject_concept_id INT,\
			cohort_initiation_date TIMESTAMP )\
			USING ICEBERG  TBLPROPERTIES ('format-version'='2') ; "

