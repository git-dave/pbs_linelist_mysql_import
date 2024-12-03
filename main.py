import pandas as pd
from mysql import connector
from sqlmodel import create_engine


# file_path = 'E:/#CIHP files/NMRS matters/PBS matters/pbs_test.xlsx'
# file_path = 'E:/#CIHP files/NMRS matters/PBS matters/PBS Line List 01122024.xlsx'
file_path = input('Enter full file path: ')
df = pd.read_excel(file_path)

# Since the dataframe take the headers in the excel file, there is need to rename the
# columns to exactly how they are named in the databse table.
df = df.rename({'Funders' : 'funders',
                'Ip' : 'ip',
                'StateName' : 'state',
                'LgaName' : 'lga',
                'FacilityName' : 'facility',
                'DatimCode' : 'datimcode',
                'unique_id' : 'unique_id',
                'Patient_Id' : 'patient_Id',
                'PatientARTNumber' : 'patient_art_number',
                'HospitalNumber' : 'hospital_number',
                'Sex' : 'sex',
                'DateOfBirth' : 'date_of_birth',
                'AgeAtArtInitiation' : 'age_at_art_initiation',
                'CurrentAge' : 'current_age',
                'ArtStartDate' : 'art_start_date',
                'DaysofARVRefill' : 'days_of_arv_refill',
                'LastDrugPickupDate' : 'last_drug_pickup_date',
                'LastClinicVisitDate' : 'last_clinic_visit_date',
                'Clinical Encounter' : 'clinical_encounter',
                'CurrentVL' : 'currentvl',
                'DateofCurrentSampleCollection' : 'date_of_current_sample_collection',
                'CurrentVLDate' : 'currentvl_date',
                'CurrentStatus28' : 'currentstatus28',
                'CurrentStatus28Q1' : 'currentstatus28Q1',
                'CurrentStatus28Q2' : 'currentstatus28Q2',
                'CurrentStatus28Q3' : 'currentstatus28Q3',
                'CurrentStatus28Q4' : 'currentstatus28Q4',
                'PatientTransferredOut' : 'patient_transferred_out',
                'TransferredOutDate' : 'transferred_out_date',
                'PatientTransferredIn' : 'patient_transferred_in',
                'TransferredInDate' : 'transferred_in_date',
                'PatientHaSDied' : 'patient_has_died',
                'PatientDeceAsedDate' : 'patient_deceased_date',
                'Fingerprint Enrolled' : 'fingerprint_enrolled',
                'Fingerprint Valid' : 'fingerprint_valid',
                'FingerPrintStatus' : 'fingerprint_status',
                'Duplicate Status' : 'duplicate_status',
                'Date_Captured' : 'date_captured',
                'ABIS Line' : 'abis_line',
                'Print Line' : 'print_line',
                'Number of Fingerprints Status' : 'number_of_fingerprints_status',
                'Number of Fingerprints' : 'number_of_fingerprints',
                'duplicate_type' : 'duplicate_type',
                'Recapture Fingerprint Enrolled' : 'recapture_fingerprint_enrolled',
                'Recapture Print Line' : 'recapture_print_line',
                'match_outcome' : 'match_outcome',
                'pri_sec_match_count' : 'pri_sec_match_count',
                'pri_match_count' : 'pri_match_count',
                'sec_match_count' : 'sec_match_count',
                'replaced_print' : 'replaced_print',
                'replaced_count' : 'replaced_count',
                'ndr_downloaded_date' : 'ndr_downloaded_date'}, axis='columns')

# print(df.shape[0])
print('\n')
print(df.head(5))

try:
    # The database URL must be in a specific format
    database_url = "mysql+mysqlconnector://{USER}:{PWD}@{HOST}/{DBNAME}"
    # Replace the values below with target DB profile
    # DB username, password, host and database name
    database_url = database_url.format(
        USER = "root",
        PWD = "pa$$w0rd",
        HOST = "localhost:3306",
        DBNAME = "pbs"
    )

    datatable = 'line_list'
    engine = create_engine(database_url, echo=False)
    # To avoid connection time for large data processing, set the chunksize parameter
    df.to_sql(datatable, engine, if_exists='append', index=False, chunksize=200)

    print(f'PBS data import for {df.shape[0]} records completed...')
except connector.Error as e:
    print(e)
