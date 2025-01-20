import functions as f
import json, os, re, shutil, time, sys
import pandas as pd
from datetime import datetime, timedelta, timezone
from tkinter.filedialog import askopenfilename, askdirectory

current_path = os.getcwd()
tz = timezone(timedelta(hours=7)) #set timezone UTC+7

# Get input file
input_file = askopenfilename(initialdir=current_path, title="Choose PREPARE_ITEM")
print(input_file)

# Get path output
destination_folder = askdirectory(initialdir=current_path, title="Choose Destination Folder")
print(destination_folder)

## Read excel file to Pandas DataFrame
print("Reading Excel File", end=""); f.print_dots(1, 1)
df = pd.read_excel(input_file, sheet_name='Config_List')
df_adf_config = pd.read_excel(input_file, sheet_name='U21_Import_ADF_Config')
df_regist_config = pd.read_excel(input_file, sheet_name='U22_Import_File_Config')
df_table_def = pd.read_excel(input_file, sheet_name='U23_Import_Table_Definition')
df_int_mapping = pd.read_excel(input_file, sheet_name='U24_Import_Interface_Mapping')
df_table_view = pd.read_excel(input_file, sheet_name='99_Run_replace_DDL')
df_dlp = df_table_def[df_table_def['SCHEMA_NAME_LIST'] == 'DLPRST'].copy() ## for recreate persist
df_adb_notebook = pd.read_excel(input_file, sheet_name='ADB_Notebook_To_Shared_Folder')

## Get UR information
print("Getting UR Information", end=""); f.print_dots(1, 1)
ur_no = df["Information"].values[0]
user_email = df["Information"].values[1]
month_period = df["Information"].values[2]
deploy_date = df["Information"].values[3]

# Basic Information
print(f'UR_NO : {ur_no}')
print(f'User Email : {user_email}')
print(f'Month_Period : {month_period}')
print(f'Deploy_Date : {deploy_date}')
print("")

## SET All Path and Create folder
bak_time = datetime.now(tz=tz).strftime("%y%m%d")+"_"+datetime.now(tz=tz).strftime("%H%M")

path_destfolder = f"{destination_folder}/"+ur_no+'_'+bak_time
folder_name = 'deploy_test_'+ur_no+'_'+bak_time

path_osfolder = '/tmp/'+folder_name
path_adls_tmp = path_osfolder+'/edwcloud_adls_tmp'
path_adb_tmp = path_osfolder+'/edwcloud_adb_tmp'

print("Creating Folder", end=""); f.print_dots(0.5, 1)
print(f"Destination Folder: {path_destfolder}"); time.sleep(0.5)
print(f"Temp Folder: {path_osfolder}"); time.sleep(0.5)
print(f"ADLS temp folder: {path_adls_tmp}"); time.sleep(0.5)
print(f"ADB temp folder: {path_adb_tmp}"); time.sleep(0.5)
print("")

try:
    f.create_folder(path_osfolder)
    f.create_folder(path_adls_tmp)
    f.create_folder(path_adb_tmp)

    ## Create Dictionary of each case from each Dataframe
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

    ### ADF_CONFIG, REGISTER_CONFIG, TABLE_DEFINITION AND INTERFACE_MAPPING
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
        else:        
            print("fail, the number of character is more than 2048 characters.")

    ## Convert excel list to deployment list
    if (grouped_adf_config['pipeline_name'].values[0] == 'U21_Import_ADF_Config' and grouped_adf_config['GENERATE_FILE_FLAG'].values[0] == 1):
        config_name = "ADF_CONFIG"
        myList = []
        file_name = df_adf_config['FILE_NAME_LIST'].tolist()
        for i in range(len(file_name)):
            myStr = f"""scbedwseasta001adls,edw-ctn-landing,UTILITIES/IMPORT/ADF_CONFIG/{file_name[i]}.xlsx,adf_config/{file_name[i]}.xlsx"""
            myList.append(myStr)
        dataframe = pd.DataFrame(myList).apply(f.ljust)
        f.write_file_txt(dataframe, ur_no, path_adls_tmp, config_name)

    if (grouped_file_config['pipeline_name'].values[0] == 'U22_Import_File_Config' and grouped_file_config['GENERATE_FILE_FLAG'].values[0] == 1):
        config_name = "REGISTER_CONFIG"
        myList = []
        file_name = df_regist_config['FILE_NAME_LIST'].tolist()
        for i in range(len(file_name)):
            myStr = f"""scbedwseasta001adls,edw-ctn-landing,UTILITIES/IMPORT/U99_PL_REGISTER_CONFIG/{file_name[i]}.xlsx,utilities/import/U99_PL_REGISTER_CONFIG/{file_name[i]}.xlsx"""
            myList.append(myStr)
        dataframe = pd.DataFrame(myList).apply(f.ljust)
        f.write_file_txt(dataframe, ur_no, path_adls_tmp, config_name)

    if (grouped_table_def['pipeline_name'].values[0] == 'U23_Import_Table_Definition' and grouped_table_def['GENERATE_FILE_FLAG'].values[0] == 1):
        config_name = "TABLE_DEF"
        myList = []
        schema_name = df_table_def['SCHEMA_NAME_LIST'].tolist()
        table_name = df_table_def['TABLE_NAME_LIST'].tolist()
        for i in range(len(table_name)):
            myStr = f"""scbedwseasta001adls,edw-ctn-landing,UTILITIES/IMPORT/U02_TABLE_DEFINITION/{schema_name[i]}_{table_name[i]}.csv,utilities/import/U02_TABLE_DEFINITION/{schema_name[i]}_{table_name[i]}.csv"""
            myList.append(myStr)
        dataframe = pd.DataFrame(myList).apply(f.ljust)
        f.write_file_txt(dataframe, ur_no, path_adls_tmp, config_name)

    if (grouped_int_mapping['pipeline_name'].values[0] == 'U24_Import_Interface_Mapping_Config' and grouped_int_mapping['GENERATE_FILE_FLAG'].values[0] == 1):
        config_name = "INT_MAPPING"
        myList = []
        interface_name = df_int_mapping['INTERFACE_NAME_LIST'].tolist()
        for i in range(len(interface_name)):
            myStr = f"""scbedwseasta001adls,edw-ctn-landing,UTILITIES/IMPORT/U03_INT_MAPPING/{interface_name[i]}.csv,utilities/import/U03_INT_MAPPING/{interface_name[i]}.csv"""
            myList.append(myStr)
        dataframe = pd.DataFrame(myList).apply(f.ljust)
        f.write_file_txt(dataframe, ur_no, path_adls_tmp, config_name)

    ## Convert excel list to deployment list of json file
    if (grouped_adf_config['pipeline_name'].values[0] == 'U21_Import_ADF_Config' and grouped_adf_config['GENERATE_FILE_FLAG'].values[0] == 1):
        config_name = "ADF_CONFIG"
        myList = []
        myStr = f"""ADB_01/{month_period}/{ur_no}/Utilities/JSON_CONVERTED_{ur_no}_ADF_CONFIG.json"""
        myList.append(myStr)
        dataframe = pd.DataFrame(myList).apply(f.ljust)
        f.write_file_txt_of_json(dataframe, ur_no, path_adb_tmp, config_name)

    if (grouped_file_config['pipeline_name'].values[0] == 'U22_Import_File_Config' and grouped_file_config['GENERATE_FILE_FLAG'].values[0] == 1):
        config_name = "REGISTER_CONFIG"
        myList = []
        myStr = f"""ADB_01/{month_period}/{ur_no}/Utilities/JSON_CONVERTED_{ur_no}_REGISTER_CONFIG.json"""
        myList.append(myStr)
        dataframe = pd.DataFrame(myList).apply(f.ljust)
        f.write_file_txt_of_json(dataframe, ur_no, path_adb_tmp, config_name)

    if (grouped_table_def['pipeline_name'].values[0] == 'U23_Import_Table_Definition' and grouped_table_def['GENERATE_FILE_FLAG'].values[0] == 1):
        config_name = "TABLE_DEF"
        myList = []
        myStr = f"""ADB_01/{month_period}/{ur_no}/Utilities/JSON_CONVERTED_{ur_no}_TABLE_DEF.json"""
        myList.append(myStr)
        dataframe = pd.DataFrame(myList).apply(f.ljust)
        f.write_file_txt_of_json(dataframe, ur_no, path_adb_tmp, config_name)

    if (grouped_int_mapping['pipeline_name'].values[0] == 'U24_Import_Interface_Mapping_Config' and grouped_int_mapping['GENERATE_FILE_FLAG'].values[0] == 1):
        config_name = "INT_MAPPING"
        myList = []
        myStr = f"""ADB_01/{month_period}/{ur_no}/Utilities/JSON_CONVERTED_{ur_no}_INT_MAPPING.json"""
        myList.append(myStr)
        dataframe = pd.DataFrame(myList).apply(f.ljust)
        f.write_file_txt_of_json(dataframe, ur_no, path_adb_tmp, config_name)

    ### DDL deploy list
    ## edwcloud_adb
    ## change existing table
    print("Creating (ADB) 01_Apply_table_change.json", end=""); f.print_dots(0.5)
    json_ChangeTable = '{"run_name": "01_Apply_table_change","existing_cluster_id": "","notebook_task":{"notebook_path":"/Shared/Apply_table_change_module/01_Apply_table_change", "base_parameters":{"PATH_SRC": "/dbfs/mnt/edw-ctn-landing/ddl_script_apply_change/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/scripts"}}}'
    parsed = json.loads(json_ChangeTable)
    json_txt = (json.dumps(parsed, indent=4)).replace("/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/", f"/{month_period}/{ur_no}/").replace("\n","\n\n")

    changeTB_json = open(f"{path_adb_tmp}/01_Apply_table_change.json", "w")
    changeTB_json.write(json_txt)
    changeTB_json.close()
    # print("ADB: 01_Apply_table_change.json >> Export File Success")
    print(" Success!")

    print(f"Creating (ADB) 01_deployList_{ur_no}_applyTableChange.txt", end=""); f.print_dots(0.5)
    deploy_job_changeTB = f"ADB_01/{month_period}/{ur_no}/Apply_table_change_module/01_Apply_table_change.json"
    deployList_changeTB = open(f"{path_adb_tmp}/01_deployList_{ur_no}_applyTableChange.txt", "w")
    deployList_changeTB.write(deploy_job_changeTB)
    deployList_changeTB.close()
    # print(f"ADB: 01_deployList_{ur_no}_applyTableChange.txt >> Export File Success")
    print(" Success!")

    ## Create new object / Change existing view
    print(f"Creating (ADB) 99_Run_replace_DDL_Databrick_loop.json", end=""); f.print_dots(0.5)
    json_CreateDDL = '{"run_name": "99_Run_replace_DDL_Databrick_loop","existing_cluster_id": "","notebook_task":{"notebook_path":"/Shared/Setup/99_Run_replace_DDL_Databrick_loop", "base_parameters":{"PATH_SRC": "/dbfs/mnt/edw-ctn-landing/ddl_script/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/ddl","PATH_ERROR": "/dbfs/mnt/edw-ctn-landing/ddl_script/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/errorlog"}}}'
    parsed = json.loads(json_CreateDDL)
    json_txt = (json.dumps(parsed, indent=4)).replace("/MMMyyyy/SI-XXX_SR-XXXXX_SR-XXXXX/", f"/{month_period}/{ur_no}/").replace("\n","\n\n")

    createDDL_json = open(f"{path_adb_tmp}/99_Run_replace_DDL_Databrick_loop.json", "w")
    createDDL_json.write(json_txt)
    createDDL_json.close()
    # print("ADB: 99_Run_replace_DDL_Databrick_loop.json >> Export File Success")
    print(" Success!")
    
    print(f"Creating (ADB) 02_deployList_{ur_no}_createDDL.txt", end=""); f.print_dots(0.5)
    deploy_job_createDDL = f"ADB_01/{month_period}/{ur_no}/Setup/99_Run_replace_DDL_Databrick_loop.json\n"
    deployList_createDDL = open(f"{path_adb_tmp}/02_deployList_{ur_no}_createDDL.txt", "w")
    deployList_createDDL.write(deploy_job_createDDL)
    deployList_createDDL.close()
    # print(f"ADB: 02_deployList_{ur_no}_createDDL.txt >> Export File Success")
    print(" Success!")


    ### Recreate Persist
    dlp_list = ",".join(df_dlp['TABLE_NAME_LIST'].tolist())
    if not df_dlp.empty:
        print("Creating (ADB) 25_Recreate_Persisted.json", end=""); f.print_dots(0.5)
        dictionary = f.create_nested_dict(None, "25_Recreate_Persisted", deploy_date, dlp_list)
        json_object = json.dumps(dictionary, indent = 4)

        dlp_json = open(f"{path_adb_tmp}/25_Recreate_Persisted.json", "w")
        dlp_json.write(json_object)
        dlp_json.close()
        # print("ADB: 25_Recreate_Persisted.json >> Export File Success")
        print(" Success!")

        print(f"Appending (ADB) 02_deployList_{ur_no}_createDDL.txt", end=""); f.print_dots(0.5)
        deploy_job_recPersist = f"ADB_01/{month_period}/{ur_no}/Setup/25_Recreate_Persisted.json"
        deployList_recPersist = open(f"{path_adb_tmp}/02_deployList_{ur_no}_createDDL.txt", "a")
        deployList_recPersist.write(deploy_job_recPersist)
        deployList_recPersist.close()
        # print(f"ADB: 02_deployList_{ur_no}_createDDL.txt >> Append File Success")
        print(" Success!")


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
                have_table = True
            elif str(obj[1]).upper() == 'NEW':
                deploylist_new_obj.write(f"scbedwseasta001adls,edw-ctn-landing,TABLES/{db_name}/{tb_name}.sql,ddl_script/{month_period}/{ur_no}/ddl/{db_name}/{tb_name}.sql\n")
                have_view = True
        elif check_table[1][0] == 'V':
            deploylist_new_obj.write(f"scbedwseasta001adls,edw-ctn-landing,VIEWS/{db_name}/{tb_name}.sql,ddl_script/{month_period}/{ur_no}/ddl/{db_name}/{tb_name}.sql\n")
            have_view = True

    deploylist_tb_change.close()
    deploylist_new_obj.close()
    # print(f"ADB: 01_deployList_{ur_no}_applyTableChange.txt >> Append File Success")
    # print(f"ADB: 02_deployList_{ur_no}_createDDL.txt >> Append File Success\n")
    print(f"Creating (ADB) 01_deployList_{ur_no}_applyTableChange.txt", end=""); f.print_dots(0.5); print(" Success!")
    print(f"Creating (ADB) 02_deployList_{ur_no}_createDDL.txt", end=""); f.print_dots(0.5); print(" Success!")

    ## Remove DDL part where Table or View not exist
    if not have_table:
        print(f"Removing (ADB) 01_deployList_{ur_no}_applyTableChange.txt", end=""); f.print_dots(0.5)
        os.remove(f"{path_adb_tmp}/01_deployList_{ur_no}_applyTableChange.txt")
        print(" Success!")

        print(f"Removing (ADLS) 01_deployList_{ur_no}.txt", end=""); f.print_dots(0.5)
        os.remove(f"{path_adls_tmp}/01_deployList_{ur_no}.txt")
        print(" Success!")
        
    if not have_view:
        print(f"Removing (ADB) 02_deployList_{ur_no}_createDDL.txt", end=""); f.print_dots(0.5)
        os.remove(f"{path_adb_tmp}/02_deployList_{ur_no}_createDDL.txt")
        print(" Success!")

        print(f"Removing (ADLS) 02_deployList_{ur_no}.txt", end=""); f.print_dots(0.5)
        os.remove(f"{path_adls_tmp}/02_deployList_{ur_no}.txt")
        print(" Success!")

    ### Combine every deployment_list
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

    have_adls = False
    if write_text != '':
        have_adls = True

    print(f"Creating (ADLS) 00_deployList_{ur_no}_all.txt", end=""); f.print_dots(0.5)
    with open(f'{path_osfolder}/00_deployList_{ur_no}_all.txt', 'w') as output_file:
        output_file.write(write_text)
    print(" Success!")

    ## Create GIT folder form
    f.create_git_form_folder(ur_no, month_period, path_osfolder, df_adb_notebook)
        
    
    ### Move notebook to Shared folder
    print("Generating Upload Notebook to /Shared/ Folder on Databricks", end=""); f.print_dots(1)
    upload_notebook = False
    exec_notebook = False
    if not df_adb_notebook.empty:
        upload_notebook = True
        adb_git_folder = path_osfolder+'/edwcloud_adb/src/Job/ADB_01'
        adb_deploylist_path = adb_git_folder+f"/deployment_release/{month_period}/{ur_no}/"

        notebook_folder = path_osfolder+'/edwcloud_adb/src/Notebook'
        deploylist_upload_folder = f"{notebook_folder}/deployment_release/{month_period}/{ur_no}"
        adb_execute_json_folder = f"{adb_git_folder}/{month_period}/{ur_no}/Execute"
        dbc_folder = f"{notebook_folder}/{month_period}/{ur_no}/Execute"

        f.create_folder(notebook_folder)
        f.create_folder(deploylist_upload_folder)
        f.create_folder(adb_execute_json_folder)
        f.create_folder(dbc_folder)
        #dec9999/SI-0000_SR-00000_SR-00000_SYSTEM/Execute/Generate_Report_Something.dbc

        deployList_notebook = open(f"{deploylist_upload_folder}/03_deployList_{ur_no}_Upload_Notebook.txt", "w")
        deployList_execute = open(f"{adb_deploylist_path}03_deployList_{ur_no}_execute_notebook.txt", "w")

        for index, row in df_adb_notebook.iterrows():
            if row['SHARED_PATH'][-1] == "/":
                shared_path = row['SHARED_PATH'][:-1]
            else:
                shared_path = row['SHARED_PATH']

            notebook_name = row['NOTEBOOK_NAME']
            execute_flag = row['EXECUTE_FLAG']
            git_folder_flag = row['GIT_PATH']

            if git_folder_flag == 1:
                deployList_notebook.write(f"PYTHON,DBC,{shared_path},{notebook_name},{month_period}/{ur_no}/Execute/{notebook_name}.dbc\n")

                dbc_file = open(f"{dbc_folder}/{notebook_name}.dbc.txt", "w")
                dbc_file.write("dummy")
                dbc_file.close()
            else:
                dbc_folder_0 = f"{notebook_folder}/{shared_path.replace('/Shared/','')}"
                f.create_folder(dbc_folder_0)
                deployList_notebook.write(f"PYTHON,DBC,{shared_path},{notebook_name},{shared_path.replace('/Shared/','')}/{notebook_name}.dbc\n")

                dbc_file = open(f"{dbc_folder_0}/{notebook_name}.dbc.txt", "w")
                dbc_file.write("dummy")
                dbc_file.close()
            
            if execute_flag == 1:
                exec_notebook = True
                json_move_notebook = '{"run_name": "<NOTEBOOK_NAME>","existing_cluster_id": "","notebook_task":{"notebook_path":"<NOTEBOOK_PATH>/<NOTEBOOK_NAME>", "base_parameters":{}}}'
                parsed = json.loads(json_move_notebook)
                json_txt = (json.dumps(parsed, indent=4)).replace("<NOTEBOOK_NAME>", notebook_name).replace("<NOTEBOOK_PATH>", shared_path).replace("\n","\n\n")
        
                move_notebook_json = open(f"{adb_git_folder}/{month_period}/{ur_no}/Execute/01_Execute_{notebook_name}.json", "w")
                move_notebook_json.write(json_txt)
                move_notebook_json.close()

                deploy_job_createDDL = f"ADB_01/{month_period}/{ur_no}/Execute/01_Execute_{notebook_name}.json\n"
                deployList_execute.write(deploy_job_createDDL)
                
        deployList_notebook.close()
        deployList_execute.close()

        # remove deployList_execute if empty
        list_execute_path = f"{adb_deploylist_path}03_deployList_{ur_no}_execute_notebook.txt"

        list_execute_file = open(list_execute_path, 'r')
        content = list_execute_file.read().strip() 
        list_execute_file.close()

        if not content: 
            os.remove(list_execute_path)
            shutil.rmtree(adb_execute_json_folder)
    print(" Success!")

    ## Create git command
    print("Creating Git command and Jenkins Parameters", end=""); f.print_dots(1)
    f.create_git_command(ur_no, month_period, deploy_date[-2:], have_adls, have_config, have_view, have_table, upload_notebook, exec_notebook, path_osfolder, user_email)
    print(" Success!")


    ### remove temp folder
    print("Move everything to Destination folder and Remove Temp Folder", end=""); f.print_dots(1)
    shutil.rmtree(path_osfolder+'/edwcloud_adls_tmp') #remove adls temp folder
    shutil.rmtree(path_osfolder+'/edwcloud_adb_tmp') #remove adb temp folder

    shutil.move(path_osfolder, path_destfolder)
    shutil.rmtree('/tmp/')
    print(" Success!")

    ## Remove Empty files and Empty folder
    f.remove_empty_files_and_folders(path_destfolder)

    ## End words
    f.print_by_letter("Every Document was Generated Successfully :)", 0.05)

    ## Open Destination folder
    os.system(f"explorer {destination_folder.replace("/", "\\")}")

except Exception as error_message:

    shutil.rmtree('/tmp/')
    print(f"\nProgram Error!!! : {str(error_message)}")