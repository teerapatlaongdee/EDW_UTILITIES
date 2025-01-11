import functions as f
import json, os, re, shutil
import pandas as pd
from datetime import datetime, timedelta, timezone
from tkinter.filedialog import askopenfilename

# current_path = os.getcwd()
# input_file = askopenfilename(initialdir=current_path, title="Choose PREPARE_ITEM")
# print(input_file)

input_file = "C:/scb100690/Playground/test_repo/Generate_Deployment/Input_folder/PREPARE_ITEM_LIST_SI-0000_SR-00000_SR-00000_SYSTEM_Test.xlsx"

#Read excel file to Pandas DataFrame
df = pd.read_excel(input_file, sheet_name='Config_List')
df_adf_config = pd.read_excel(input_file, sheet_name='U21_Import_ADF_Config')
df_regist_config = pd.read_excel(input_file, sheet_name='U22_Import_File_Config')
df_table_def = pd.read_excel(input_file, sheet_name='U23_Import_Table_Definition')
df_int_mapping = pd.read_excel(input_file, sheet_name='U24_Import_Interface_Mapping')
df_table_view = pd.read_excel(input_file, sheet_name='99_Run_replace_DDL')
df_dlp = df_table_def[df_table_def['SCHEMA_NAME_LIST'] == 'DLPRST'].copy() ## for recreate persist

#Get month_period and UR DEPLOY_DATE
ur_no = df["Information"].values[0]
user_email = df["Information"].values[1]
month_period = df["Information"].values[2]
deploy_date = df["Information"].values[3][-2:]

# print(f'UR_NO : {ur_no}')
# print(f'User Email : {user_email}')
# print(f'Month_Period : {month_period}')
# print(f'Deploy_Date : {df["Information"].values[3]}')

tz = timezone(timedelta(hours=7)) #set timezone UTC+7
bak_time = datetime.now(tz=tz).strftime("%y%m%d")+"_"+datetime.now(tz=tz).strftime("%H%M")

# path_destfolder = "C:/scb100690/Playground/test_repo/Generate_Deployment/output_folder/UR/"+month_period+"/"+ur_no
folder_name = 'deploy_test_'+ur_no+'_'+bak_time
# folder_name = 'deploy_test_'+ur_no+'_250112_0012'
path_destfolder = "C:/scb100690/Playground/test_repo/Generate_Deployment/output_folder/"+ur_no+'_'+bak_time

path_osfolder = '/tmp/'+folder_name
path_adls_tmp = path_osfolder+'/edwcloud_adls_tmp'
path_adb_tmp = path_osfolder+'/edwcloud_adb_tmp'

# print(f"Storage path: {path_destfolder}\n")
# print(f"Temp: {path_osfolder}")
# print(f"GIT_ADLS: {path_adls_tmp}")
# print(f"GIT_ADB: {path_adb_tmp}")

# Create folder
f.create_folder(path_osfolder)
f.create_folder(path_adls_tmp)
f.create_folder(path_adb_tmp)

dic = {}
for data in [df_adf_config]:
    for col in data.columns:
        param_list = data[col].dropna().tolist()
        dic[col] = [param_list]        
tmp_df = pd.DataFrame(dic)
for col in tmp_df.columns:
    tmp_df[col] = tmp_df[col].apply(','.join)
df_adf_param = df.loc[df['pipeline_name'].isin(['U21_Import_ADF_Config'])]    
df_adf_param = df_adf_param.reset_index()
grouped_adf_config = pd.concat([df_adf_param, tmp_df], axis=1)
grouped_adf_config = grouped_adf_config.fillna("")

dic = {}
for data in [df_regist_config]:
    for col in data.columns:
        param_list = data[col].dropna().tolist()
        dic[col] = [param_list]            
tmp_df = pd.DataFrame(dic)
for col in tmp_df.columns:
    tmp_df[col] = tmp_df[col].apply(','.join)
df_regist_param = df.loc[df['pipeline_name'].isin(['U22_Import_File_Config'])]  
df_regist_param = df_regist_param.reset_index()   
grouped_file_config = pd.concat([df_regist_param, tmp_df], axis=1)
grouped_file_config = grouped_file_config.fillna("")

dic = {}
for data in [df_table_def]:
    for col in data.columns:
        param_list = data[col].dropna().tolist()
        dic[col] = [param_list]          
tmp_df = pd.DataFrame(dic)
for col in tmp_df.columns:
    tmp_df[col] = tmp_df[col].apply(','.join)
df_table_d_param = df.loc[df['pipeline_name'].isin(['U23_Import_Table_Definition'])]   
df_table_d_param = df_table_d_param.reset_index()      
grouped_table_def = pd.concat([df_table_d_param, tmp_df], axis=1)
grouped_table_def = grouped_table_def.fillna("")

dic = {}
for data in [df_int_mapping]:
    for col in data.columns:
        param_list = data[col].dropna().tolist()
        dic[col] = [param_list]           
tmp_df = pd.DataFrame(dic)
for col in tmp_df.columns:
    tmp_df[col] = tmp_df[col].apply(','.join)
df_mapping_param = df.loc[df['pipeline_name'].isin(['U24_Import_Interface_Mapping_Config'])] 
df_mapping_param = df_mapping_param.reset_index()          
grouped_int_mapping = pd.concat([df_mapping_param, tmp_df], axis=1)
grouped_int_mapping = grouped_int_mapping.fillna("")

have_config = False

if (grouped_adf_config['pipeline_name'].values[0] == 'U21_Import_ADF_Config' and grouped_adf_config['GENERATE_FILE_FLAG'].values[0] == 1):
    have_config = True
    config_name = "ADF_CONFIG"
    pl_name = 'U21_Import_ADF_Config'
    if (grouped_adf_config['FILE_NAME_LIST'].str.len().values[0] <= 2048):
    # Convert dataframe to json format
        for i in range(len(grouped_adf_config)):
            data = grouped_adf_config.iloc[i]
            dictionary = f.create_nested_dict(data,pl_name,deploy_date) 
  
        # Serializing json 
        json_object = json.dumps(dictionary, indent = 4) 
        f.write_file_json(json_object, ur_no, config_name, path_adb_tmp)
        # print(json_object)
    else:
        print("fail, the number of character is more than 2048 characters.")

if (grouped_file_config['pipeline_name'].values[0] == 'U22_Import_File_Config' and grouped_file_config['GENERATE_FILE_FLAG'].values[0] == 1):
    have_config = True
    config_name = "REGISTER_CONFIG"
    pl_name = "U22_Import_File_Config"
    if (grouped_file_config['FILE_NAME_LIST'].str.len().values[0] <= 2048):
    # Convert dataframe to json format
        for i in range(len(grouped_file_config)):
            data = grouped_file_config.iloc[i]
            dictionary = f.create_nested_dict(data,pl_name,deploy_date) 
  
        # Serializing json 
        json_object = json.dumps(dictionary, indent = 4) 
        f.write_file_json(json_object, ur_no, config_name, path_adb_tmp)
        # print(json_object)
    else:
        print("fail, the number of character is more than 2048 characters.")

if (grouped_table_def['pipeline_name'].values[0] == 'U23_Import_Table_Definition' and grouped_table_def['GENERATE_FILE_FLAG'].values[0] == 1):
    df_dlp = df_table_def[df_table_def['SCHEMA_NAME_LIST'] == 'DLPRST'].copy() ## for recreate persist
    
    have_config = True
    config_name = "TABLE_DEF"   
    pl_name = "U23_Import_Table_Definition"         
    if (grouped_table_def['SCHEMA_NAME_LIST'].str.len().values[0] <= 2048) & (grouped_table_def['TABLE_NAME_LIST'].str.len().values[0] <= 2048):
    # Convert dataframe to json format
        for i in range(len(grouped_table_def)):
            data = grouped_table_def.iloc[i]
            dictionary = f.create_nested_dict(data,pl_name,deploy_date) 

        dictionary['notebook_task']['base_parameters']['DELETE_FLAG'] = str(dictionary['notebook_task']['base_parameters']['DELETE_FLAG'])
        # Serializing json 
        json_object = json.dumps(dictionary, indent = 4) 
        f.write_file_json(json_object, ur_no, config_name, path_adb_tmp)
        # print(json_object)
    else:
        print("fail, the number of character is more than 2048 characters.")

if (grouped_int_mapping['pipeline_name'].values[0] == 'U24_Import_Interface_Mapping_Config' and grouped_int_mapping['GENERATE_FILE_FLAG'].values[0] == 1):
    have_config = True
    config_name = "INT_MAPPING"
    pl_name = "U24_Import_Interface_Mapping"
    if (grouped_int_mapping['INTERFACE_NAME_LIST'].str.len().values[0] <= 2048):
    # Convert dataframe to json format
        for i in range(len(grouped_int_mapping)):
            data = grouped_int_mapping.iloc[i]
            dictionary = f.create_nested_dict(data,pl_name,deploy_date) 
 
        dictionary['notebook_task']['base_parameters']['WORKSPACE_ID'] = str(dictionary['notebook_task']['base_parameters']['WORKSPACE_ID'])  
        # Serializing json 
        json_object = json.dumps(dictionary, indent = 4) 
        f.write_file_json(json_object, ur_no, config_name, path_adb_tmp)
        # print(json_object)
    else:        
        print("fail, the number of character is more than 2048 characters.")

#Convert excel list to deployment list
if (grouped_adf_config['pipeline_name'].values[0] == 'U21_Import_ADF_Config' and grouped_adf_config['GENERATE_FILE_FLAG'].values[0] == 1):
    myList = []
    file_name = df_adf_config['FILE_NAME_LIST'].tolist()
    for i in range(len(file_name)):
        myStr = f"""scbedwseasta001adls,edw-ctn-landing,UTILITIES/IMPORT/ADF_CONFIG/{file_name[i]}.xlsx,adf_config/{file_name[i]}.xlsx"""
        myList.append(myStr)
    dataframe = pd.DataFrame(myList).apply(f.ljust)
    f.write_file_txt(dataframe, ur_no, path_adls_tmp)

if (grouped_file_config['pipeline_name'].values[0] == 'U22_Import_File_Config' and grouped_file_config['GENERATE_FILE_FLAG'].values[0] == 1):
    myList = []
    file_name = df_regist_config['FILE_NAME_LIST'].tolist()
    for i in range(len(file_name)):
        myStr = f"""scbedwseasta001adls,edw-ctn-landing,UTILITIES/IMPORT/U99_PL_REGISTER_CONFIG/{file_name[i]}.xlsx,utilities/import/U99_PL_REGISTER_CONFIG/{file_name[i]}.xlsx"""
        myList.append(myStr)
    dataframe = pd.DataFrame(myList).apply(f.ljust)
    f.write_file_txt(dataframe, ur_no, path_adls_tmp)

if (grouped_table_def['pipeline_name'].values[0] == 'U23_Import_Table_Definition' and grouped_table_def['GENERATE_FILE_FLAG'].values[0] == 1):
    myList = []
    schema_name = df_table_def['SCHEMA_NAME_LIST'].tolist()
    table_name = df_table_def['TABLE_NAME_LIST'].tolist()
    for i in range(len(table_name)):
        myStr = f"""scbedwseasta001adls,edw-ctn-landing,UTILITIES/IMPORT/U02_TABLE_DEFINITION/{schema_name[i]}_{table_name[i]}.csv,utilities/import/U02_TABLE_DEFINITION/{schema_name[i]}_{table_name[i]}.csv"""
        myList.append(myStr)
    dataframe = pd.DataFrame(myList).apply(f.ljust)
    f.write_file_txt(dataframe, ur_no, path_adls_tmp)

if (grouped_int_mapping['pipeline_name'].values[0] == 'U24_Import_Interface_Mapping_Config' and grouped_int_mapping['GENERATE_FILE_FLAG'].values[0] == 1):
    myList = []
    interface_name = df_int_mapping['INTERFACE_NAME_LIST'].tolist()
    for i in range(len(interface_name)):
        myStr = f"""scbedwseasta001adls,edw-ctn-landing,UTILITIES/IMPORT/U03_INT_MAPPING/{interface_name[i]}.csv,utilities/import/U03_INT_MAPPING/{interface_name[i]}.csv"""
        myList.append(myStr)
    dataframe = pd.DataFrame(myList).apply(f.ljust)
    f.write_file_txt(dataframe, ur_no, path_adls_tmp)

#Convert excel list to deployment list of json file
if (grouped_adf_config['pipeline_name'].values[0] == 'U21_Import_ADF_Config' and grouped_adf_config['GENERATE_FILE_FLAG'].values[0] == 1):
    myList = []
    myStr = f"""ADB_01/{month_period}/{ur_no}/Utilities/JSON_CONVERTED_{ur_no}_ADF_CONFIG.json"""
    myList.append(myStr)
    dataframe = pd.DataFrame(myList).apply(f.ljust)
    f.write_file_txt_of_json(dataframe, ur_no, path_adb_tmp)

if (grouped_file_config['pipeline_name'].values[0] == 'U22_Import_File_Config' and grouped_file_config['GENERATE_FILE_FLAG'].values[0] == 1):
    myList = []
    myStr = f"""ADB_01/{month_period}/{ur_no}/Utilities/JSON_CONVERTED_{ur_no}_REGISTER_CONFIG.json"""
    myList.append(myStr)
    dataframe = pd.DataFrame(myList).apply(f.ljust)
    f.write_file_txt_of_json(dataframe, ur_no, path_adb_tmp)

if (grouped_table_def['pipeline_name'].values[0] == 'U23_Import_Table_Definition' and grouped_table_def['GENERATE_FILE_FLAG'].values[0] == 1):
    myList = []
    myStr = f"""ADB_01/{month_period}/{ur_no}/Utilities/JSON_CONVERTED_{ur_no}_TABLE_DEF.json"""
    myList.append(myStr)
    dataframe = pd.DataFrame(myList).apply(f.ljust)
    f.write_file_txt_of_json(dataframe, ur_no, path_adb_tmp)

if (grouped_int_mapping['pipeline_name'].values[0] == 'U24_Import_Interface_Mapping_Config' and grouped_int_mapping['GENERATE_FILE_FLAG'].values[0] == 1):
    myList = []
    myStr = f"""ADB_01/{month_period}/{ur_no}/Utilities/JSON_CONVERTED_{ur_no}_INT_MAPPING.json"""
    myList.append(myStr)
    dataframe = pd.DataFrame(myList).apply(f.ljust)
    f.write_file_txt_of_json(dataframe, ur_no, path_adb_tmp)

# # DDL deploy list
# # edwcloud_adb
# # change existing table
json_ChangeTable = '{"run_name": "01_Apply_table_change","existing_cluster_id": "","notebook_task":{"notebook_path":"/Shared/Apply_table_change_module/01_Apply_table_change", "base_parameters":{"PATH_SRC": "/dbfs/mnt/edw-ctn-landing/ddl_script_apply_change/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/scripts"}}}'
parsed = json.loads(json_ChangeTable)
json_txt = (json.dumps(parsed, indent=4)).replace("/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/", f"/{month_period}/{ur_no}/").replace("\n","\n\n")
print(json_txt)

changeTB_json = open(f"{path_adb_tmp}/01_Apply_table_change.json", "w")
changeTB_json.write(json_txt)
changeTB_json.close()

deploy_job_changeTB = f"ADB_01/{month_period}/{ur_no}/Apply_table_change_module/01_Apply_table_change.json"
print(deploy_job_changeTB)

deployList_changeTB = open(f"{path_adb_tmp}/01_deployList_{ur_no}_applyTableChange.txt", "w")
deployList_changeTB.write(deploy_job_changeTB)
deployList_changeTB.close()

# Create new object / Change existing view
json_CreateDDL = '{"run_name": "99_Run_replace_DDL_Databrick_loop","existing_cluster_id": "","notebook_task":{"notebook_path":"/Shared/Setup/99_Run_replace_DDL_Databrick_loop", "base_parameters":{"PATH_SRC": "/dbfs/mnt/edw-ctn-landing/ddl_script/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/ddl","PATH_ERROR": "/dbfs/mnt/edw-ctn-landing/ddl_script/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/errorlog"}}}'
parsed = json.loads(json_CreateDDL)
json_txt = (json.dumps(parsed, indent=4)).replace("/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/", f"/{month_period}/{ur_no}/").replace("\n","\n\n")
print(json_txt)

createDDL_json = open(f"{path_adb_tmp}/99_Run_replace_DDL_Databrick_loop.json", "w")
createDDL_json.write(json_txt)
createDDL_json.close()

deploy_job_createDDL = f"ADB_01/{month_period}/{ur_no}/Setup/99_Run_replace_DDL_Databrick_loop.json\n"
print(deploy_job_createDDL)

deployList_createDDL = open(f"{path_adb_tmp}/02_deployList_{ur_no}_createDDL.txt", "w")
deployList_createDDL.write(deploy_job_createDDL)
deployList_createDDL.close()


### Recreate Persist
dlp_list = ",".join(df_dlp['TABLE_NAME_LIST'].tolist())

if not df_dlp.empty:
    dictionary = f.create_nested_dict(None, "25_Recreate_Persisted", deploy_date, dlp_list)
    json_object = json.dumps(dictionary, indent = 4)
    
    print(json_object)

    dlp_json = open(f"{path_adb_tmp}/25_Recreate_Persisted.json", "w")
    dlp_json.write(json_object)
    dlp_json.close()

    deploy_job_recPersist = f"ADB_01/{month_period}/{ur_no}/Setup/25_Recreate_Persisted.json"

    print(deploy_job_recPersist)

    deployList_recPersist = open(f"{path_adb_tmp}/02_deployList_{ur_no}_createDDL.txt", "a")
    deployList_recPersist.write(deploy_job_recPersist)
    deployList_recPersist.close()


### edwcloud_adls
have_view = False
have_table = False
objs = []

for i in range(len(df_table_view)):
    objs.append([df_table_view.iloc[i, 0], df_table_view.iloc[i, 1]])

deploylist_tb_change = open(f"{path_adls_tmp}/01_deployList_{ur_no}.txt", "w")
deploylist_new_obj = open(f"{path_adls_tmp}/02_deployList_{ur_no}.txt", "w")

for obj in objs:
    db = obj[0].split('.')[0].upper()
    db_name = db[2:].upper()
    tb_name = obj[0].split('.')[1].upper()
    check_table = re.split(r'(?<=P1)(?=[A-Z])', obj[0])
    if check_table[1][0] == 'D':
        if str(obj[1]).upper() != 'NEW':
            deploylist_tb_change.write(f"scbedwseasta001adls,edw-ctn-landing,TABLES/{db_name}/{tb_name}.sql,ddl_script_apply_change/{month_period}/{ur_no}/scripts/{db_name}/{tb_name}.sql\n")
        elif str(obj[1]).upper() == 'NEW':
            deploylist_new_obj.write(f"scbedwseasta001adls,edw-ctn-landing,TABLES/{db_name}/{tb_name}.sql,ddl_script/{month_period}/{ur_no}/ddl/{db_name}/{tb_name}.sql\n")
        have_table = True
    elif check_table[1][0] == 'V':
        deploylist_new_obj.write(f"scbedwseasta001adls,edw-ctn-landing,VIEWS/{db_name}/{tb_name}.sql,ddl_script/{month_period}/{ur_no}/ddl/{db_name}/{tb_name}.sql\n")
        have_view = True

deploylist_tb_change.close()
deploylist_new_obj.close()


#############################################################
deploy_config_ = None
deploy_table_ = None
deploy_view_ = None
if os.path.exists(f'{path_adls_tmp}/00_deployList_{ur_no}_utilities.txt'):
    with open(f'{path_adls_tmp}/00_deployList_{ur_no}_utilities.txt', 'r') as f1:
        deploy_config_ = f1.read()
if os.path.exists(f'{path_adls_tmp}/01_deployList_{ur_no}.txt'):
    with open(f'{path_adls_tmp}/01_deployList_{ur_no}.txt', 'r') as f2:
        deploy_table_ = f2.read()
if os.path.exists(f"{path_adls_tmp}/02_deployList_{ur_no}.txt"):
    with open(f"{path_adls_tmp}/02_deployList_{ur_no}.txt", "r") as f3:
        deploy_view_ = f3.read()

write_text = ''
write_text += deploy_config_ if deploy_config_ else ''
write_text += deploy_table_ if deploy_table_ else ''
write_text += deploy_view_ if deploy_view_ else ''
# print(write_text)
        
with open(f'{path_osfolder}/00_deployList_{ur_no}_all.txt', 'w') as output_file:
    output_file.write(write_text)


#########################################################################
#Create git command
# f.create_git_command(ur_no, month_period, deploy_date, have_config, have_view, have_table, path_osfolder, user_email)
f.create_git_command(ur_no, month_period, deploy_date, have_config=True, have_view=True, have_table=True, output_path=path_osfolder, email=user_email)

# #Create GIT folder form
f.create_git_form_folder(ur_no, month_period, path_osfolder)

# remove temp folder
shutil.rmtree(path_osfolder+'/edwcloud_adls_tmp') #remove adls temp folder
shutil.rmtree(path_osfolder+'/edwcloud_adb_tmp') #remove adb temp folder

shutil.move(path_osfolder, path_destfolder)
shutil.rmtree('/tmp/')