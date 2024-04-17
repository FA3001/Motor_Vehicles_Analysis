

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path
from pandas import DataFrame
from os import path
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Data Loader for Crashes
@data_loader
def load_data_from_api(*args, **kwargs):
    url = 'https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD'
    
    sel_crashes = [
        "COLLISION_ID",
        "CRASH DATE",
        "CRASH TIME",
        "BOROUGH",
        "NUMBER OF PERSONS INJURED",
        "NUMBER OF PERSONS KILLED",
        "VEHICLE TYPE CODE 1",
        "CONTRIBUTING FACTOR VEHICLE 1",
        "VEHICLE TYPE CODE 2",
        "CONTRIBUTING FACTOR VEHICLE 2",
        "VEHICLE TYPE CODE 3",
        "CONTRIBUTING FACTOR VEHICLE 3",
        "VEHICLE TYPE CODE 4",
        "CONTRIBUTING FACTOR VEHICLE 4" 
    ]
    sel_crashes_rename = {
        "COLLISION_ID" : "collision_id",
        "CRASH DATE" : "crash_date",
        "CRASH TIME" : "crash_time",
        "BOROUGH" : "borough",
        "NUMBER OF PERSONS INJURED" : "injured" ,
        "NUMBER OF PERSONS KILLED" : "killed" ,
        "CONTRIBUTING FACTOR VEHICLE 1" : "contr_f_vhc_1",
        "CONTRIBUTING FACTOR VEHICLE 2" : "contr_f_vhc_2",
        "CONTRIBUTING FACTOR VEHICLE 3" : "contr_f_vhc_3",
        "CONTRIBUTING FACTOR VEHICLE 4" : "contr_f_vhc_4",
        "VEHICLE TYPE CODE 1" : "vhc_1_code",
        "VEHICLE TYPE CODE 2" : "vhc_2_code",
        "VEHICLE TYPE CODE 3" : "vhc_3_code",
        "VEHICLE TYPE CODE 4" : "vhc_4_code"
    }
    sel_crashes_types = {
        "collision_id" : pd.Int64Dtype(),
        "borough" : str,
        "injured" : pd.Int64Dtype(),
        "killed" : pd.Int64Dtype(),
        "vhc_1_code" : str,
        "contr_f_vhc_1" : str,
        "vhc_2_code" : str,
        "contr_f_vhc_2" : str,
        "vhc_3_code" : str,
        "contr_f_vhc_3" : str,
        "vhc_4_code" : str,
        "contr_f_vhc_4" : str
    }

    df = pd.read_csv(url, sep=',', dtype=sel_crashes_types)
    df = df[sel_crashes]
    df.rename(columns=sel_crashes_rename, inplace=True)

    df['crash_date'] = pd.to_datetime(df['crash_date']).dt.date
    df['crash_time'] = pd.to_datetime(df['crash_time'], format='%H:%M').dt.time
    return df

# # Data Loader for Vehicles
# @data_loader
#     url = 'https://data.cityofnewyork.us/api/views/bm4k-52h4/rows.csv?accessType=DOWNLOAD'
# def load_data_from_api(*args, **kwargs):

#     sel_vh = [
#         "UNIQUE_ID" ,
#         "COLLISION_ID" ,
#         "CRASH_DATE" ,
#         "CRASH_TIME" ,
#         "VEHICLE_TYPE", 
#         "VEHICLE_DAMAGE" ,
#         "DRIVER_SEX" ,
#         "DRIVER_LICENSE_STATUS" ,
#         "VEHICLE_YEAR" ,
#         "VEHICLE_OCCUPANTS" ,  
#         "STATE_REGISTRATION" , 
#         "CONTRIBUTING_FACTOR_1"
#     ]
#     sel_vh_rename = {
#         "UNIQUE_ID" : "unique_id",
#         "COLLISION_ID" : "collision_id",
#         "CRASH_DATE" : "crash_date",
#         "CRASH_TIME" : "crash_time",
#         "STATE_REGISTRATION" : "state_reg",
#         "VEHICLE_TYPE" : "vhc_type",
#         "VEHICLE_YEAR" : "vhc_year",
#         "VEHICLE_OCCUPANTS" : "vhc_occupants",
#         "DRIVER_SEX" : "dr_sex",
#         "DRIVER_LICENSE_STATUS" : "dr_lic_status",
#         "VEHICLE_DAMAGE" : "vhc_dmg",
#         "CONTRIBUTING_FACTOR_1" : "contr_f"
#     }
#     sel_vh_types = {
#         "unique_id" : pd.Int64Dtype(),
#         "collision_id" : pd.Int64Dtype(),
#         "vhc_type" : str,
#         "vhc_dmg" : str,
#         "dr_sex" : str,
#         "dr_lic_status" : str,
#         "vhc_year"  : pd.Int64Dtype(),
#         "vhc_occupants"  : pd.Int64Dtype(),
#         "state_reg" : str,
#         "contr_f" : str
#     }

#     df_vehicles = pd.read_csv(url, sep=',', dtype=sel_vh_types)
#     df_vehicles = df_vehicles[sel_vh]
#     df_vehicles.rename(columns=sel_vh_rename, inplace=True)

#     df_vehicles['crash_date'] = pd.to_datetime(df_vehicles['crash_date']).dt.date
#     df_vehicles['crash_time'] = pd.to_datetime(df_vehicles['crash_time'], format='%H:%M').dt.time
#     return df_vehicles

# Data Loader for Persons
# @data_loader
# def load_data_from_api(*args, **kwargs):
#     url = 'https://data.cityofnewyork.us/api/views/f55k-p6yu/rows.csv?accessType=DOWNLOAD'


#     sel_p_rename = {
#         "UNIQUE_ID" : "unique_id",
#         "COLLISION_ID" : "collision_id",
#         "CRASH_DATE" : "crash_date",
#         "CRASH_TIME" : "crash_time",
#         "EJECTION" : "ejection",
#         "BODILY_INJURY" : "body_inj",
#         "PERSON_INJURY" : "person_inj",
#         "POSITION_IN_VEHICLE" : "pos_in_vhc",
#         "SAFETY_EQUIPMENT" : "safety_equip",
#         "PERSON_TYPE" : "person_type",
#         "PERSON_AGE" : "age",
#         "PERSON_SEX" : "sex",
#         "EMOTIONAL_STATUS" : "emot_status",
#         "CONTRIBUTING_FACTOR_1" : "contr_f"
#     }
#     sel_P = [
#         "UNIQUE_ID" ,
#         "COLLISION_ID",
#         "CRASH_DATE" ,
#         "CRASH_TIME" ,
#         "EJECTION" ,
#         "BODILY_INJURY" ,
#         "PERSON_INJURY" ,
#         "POSITION_IN_VEHICLE" ,
#         "SAFETY_EQUIPMENT" ,
#         "PERSON_TYPE" ,
#         "PERSON_AGE" ,
#         "PERSON_SEX",
#         "EMOTIONAL_STATUS" ,
#         "CONTRIBUTING_FACTOR_1" 
#     ]

#     sel_P_types = {
#         "unique_id" : pd.Int64Dtype(),
#         "collision_id" : pd.Int64Dtype(),
#         "ejection" : str,
#         "body_inj" : str,
#         "person_inj" : str,
#         "pos_in_vhc" : str,
#         "safety_equip" : str,
#         "person_type" : str,
#         "age" : pd.Int64Dtype(),
#         "sex" : str,
#         "emot_status" : str,
#         "contr_f" : str
#     }


#     df = pd.read_csv(url, sep=',', dtype=sel_P_types)
#     df = df[sel_P]
#     df.rename(columns=sel_p_rename, inplace=True)
#     df['crash_date'] = pd.to_datetime(df['crash_date']).dt.date
#     df['crash_time'] = pd.to_datetime(df['crash_time'], format='%H:%M').dt.time
#     return df
#Test function
@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'