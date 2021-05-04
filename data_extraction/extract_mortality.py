import sqlite3
import pandas as pd 




##############################################################################
####################### CONNECT TO MIMIC #####################################
connection = sqlite3.connect("C:\\Users\\Maria\\Desktop\\Projects Data Scripts\\MIMIC\\data\\mimic3.db")

#We can verify we successfully created our connection object by running:
print(connection.total_changes)
# Be sure to close the connection
#con.close()

# Create our test query
test_query = """
SELECT subject_id, hadm_id, admittime, dischtime, admission_type, diagnosis
FROM admissions
"""

# Run the query and assign the results to a variable
test = pd.read_sql_query(test_query,connection)

print("TEST:", test.head())
##############################################################################
##################### QUERYING, MORTALITY COHORT #############################

query = """

WITH icu_patients AS
(
SELECT icu.subject_id, icu.hadm_id, icu.icustay_id, pat.DOB, pat.gender, icu.intime 
, (JulianDay(OUTTIME) - JulianDay(INTIME)) as icu_length_of_stay
, (JulianDay(icu.INTIME) - JulianDay(pat.DOB))/ 364.242 as age
, adm.hospital_expire_flag , icu.outtime 
, RANK() OVER (PARTITION BY icu.subject_id ORDER BY icu.intime) AS icustay_id_order 


FROM icustays icu
INNER JOIN patients pat
  ON icu.subject_id = pat.subject_id
INNER JOIN admissions adm
    ON adm.subject_id = icu.subject_id
    AND adm.hadm_id = icu.hadm_id
)


SELECT
    fa.subject_id, fa.hadm_id, fa.icustay_id, fa.icustay_id_order, fa.intime as icu_intime, fa.outtime as icu_outtime, fa.DOB, fa.GENDER, fa.age as patient_age, fa.hospital_expire_flag as mortality, fa.icu_length_of_stay
  , CASE 
        WHEN fa.icu_length_of_stay < 2 then 1
    ELSE 0 END
        as exclusion_los
  , CASE
        WHEN fa.age < 18 then 1
    ELSE 0 END
        as exclusion_age
FROM icu_patients fa

"""

icu = pd.read_sql_query(query, connection)



###############################################################################


query = """

WITH filter_prescriptions AS
(
SELECT d.subject_id, d.icustay_id, d.drug as drug_name, CAST(d.NDC AS varchar) as NDC, d.startdate as drug_startdate, d.enddate as drug_enddate, d.prod_strength as drug_strength, d.dose_val_rx as drug_dosage, d.dose_unit_rx as drug_unit
FROM prescriptions d
INNER JOIN icustays icu
	ON d.ICUSTAY_ID = icu.ICUSTAY_ID
ORDER  BY d.SUBJECT_ID ASC
)

SELECT *
FROM filter_prescriptions 

"""

drugs = pd.read_sql_query(query, connection)



#####################################################################################


#query = """

#WITH filter_labs AS
#(
#SELECT l.subject_id, l.hadm_id, l.charttime as lab_charttime, l.value as lab_value, l.valuenum as lab_value_num, l.flag as lab_flag, dl.label as lab_label, dl.loinc_code as loinc
#FROM labevents l
#INNER JOIN icustays icu, admissions
#    ON l.subject_id = icu.subject_id
#    AND l.hadm_id = admissions.hadm_id
#INNER JOIN d_labitems dl
#    ON dl.itemid = l.itemid
#ORDER  BY l.SUBJECT_ID ASC
#)

#SELECT *
#FROM filter_labs 


#print("in labs")
#labs_chunks = pd.read_sql_query(query, connection, chunksize=100)

#labs_d = []
#for df in labs_chunks:
#    labs_d.append(df)
#labs = pd.concat(labs_d)

############################################################################################
print("in dia")


query = """

WITH filter_diagnoses AS 
(
    SELECT icd.subject_id, icd.hadm_id, icd.icd9_code, d_icd.short_title as icd9_title
    FROM diagnoses_icd icd
    INNER JOIN icustays, admissions
        ON icd.subject_id = icustays.subject_id
        AND icustays.hadm_id = admissions.hadm_id
    INNER JOIN d_icd_diagnoses d_icd
        ON icd.icd9_code = d_icd.icd9_code
)


SELECT *
FROM filter_diagnoses
ORDER BY subject_id

"""

diagnoses = pd.read_sql_query(query, connection)

##################################################################################################


##################################################################################################
################################ PREPROCESSING ###################################################


icu.columns = list(icu.iloc[:0])
drugs.columns = list(drugs.iloc[:0])
diagnoses.columns = list(diagnoses.iloc[:0])
#labs.columns = list(labs.iloc[:0])


drugs.set_index(["subject_id"], inplace=True)
drugs.to_csv("C:\\Users\\Maria\\Desktop\\Projects Data Scripts\\MIMIC\\data\\Mortality\\mortality_drugs.csv")

diagnoses.set_index(["subject_id"], inplace=True)
diagnoses.to_csv("C:\\Users\\Maria\\Desktop\\Projects Data Scripts\\MIMIC\\data\\Mortality\\mortality_diagnoses.csv")
"""
labs.set_index(["subject_id"], inplace=True)
labs.to_csv("C:\\Users\\Maria\\Desktop\\Projects Data Scripts\\MIMIC\\data\\Mortality\\mortality_labs.csv")
"""
icu.set_index(["subject_id"], inplace=True)
icu.to_csv("C:\\Users\\Maria\\Desktop\\Projects Data Scripts\\MIMIC\\data\\Mortality\\mortality_icu.csv")
