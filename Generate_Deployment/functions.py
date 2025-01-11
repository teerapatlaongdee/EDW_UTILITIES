import os, shutil


def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

def ljust(s):
    s = s.astype(str).str.strip()
    return s.str.ljust(s.str.len().max())

def remove_suffix(string, suffix):
    if string.endswith(suffix):
        return string[:-len(suffix)]
    return string

def create_nested_dict(row, pl, deploy_date, dlp_list = None):
    if pl == 'U21_Import_ADF_Config':
        return {
            "run_name": row["run_name"],
            "existing_cluster_id": "",
            "notebook_task": {
                "notebook_path": "/Shared/Utilities/U21_Import_ADF_Config",
                "base_parameters": {
                    "CONTAINER_NAME": "edw-ctn-landing",
                    "DEPLOY_DATE": deploy_date,
                    "FILE_NAME_LIST": row["FILE_NAME_LIST"],
                    "LAST_UPDATE_USER": "Deployment_User",
                    "PATH_NAME": 'adf_config', 
                    "SYSTEM_NAME_LIST": row["SYSTEM_NAME_LIST"]  
                }
            }
        }
    if pl == 'U22_Import_File_Config':
        return {
            "run_name": row["run_name"],
            "existing_cluster_id": "",
            "notebook_task": {
                "notebook_path": "/Shared/Utilities/U22_Import_File_Config",
                "base_parameters": {
                    "File_Name_List": row["FILE_NAME_LIST"],
                    "Pipeline_Name": "U99_PL_REGISTER_CONFIG",
                    "DEPLOY_DATE": deploy_date      
                }
            }
        }
    if pl == 'U23_Import_Table_Definition':
        return {
            "run_name": row["run_name"],
            "existing_cluster_id": "",
            "notebook_task": {
                "notebook_path": "/Shared/Utilities/U23_Import_Table_Definition",
                "base_parameters": {
                    "DB_SCHEMA_PREFIX": "P1",
                    "SCHEMA_NAME_LIST": row["SCHEMA_NAME_LIST"],
                    "TABLE_NAME_LIST": row["TABLE_NAME_LIST"],
                    "UPDATE_USER": "Deployment_User",
                    "DEPLOY_DATE": deploy_date,
                    "OPTION": "import_file",
                    "DELETE_FLAG": "1"        
                }
            }
        }
    if pl == 'U24_Import_Interface_Mapping':
        return {
            "run_name": row["run_name"],
            "existing_cluster_id": "",
            "notebook_task": {
                "notebook_path": "/Shared/Utilities/U24_Import_Interface_Mapping_Config",
                "base_parameters": {
                    "DB_SCHEMA_PREFIX": "P1",
                    "INTERFACE_NAME_LIST": row["INTERFACE_NAME_LIST"],
                    "OPTION": "import_file",
                    "UPDATE_USER": "Deployment_User",
                    "DEPLOY_DATE": deploy_date,
                    "WORKSPACE_ID": "1"        
                }
            }
        }
    if pl == '25_Recreate_Persisted':
        return {
            "run_name": "25_Recreate_Persisted",
            "existing_cluster_id": "",
            "notebook_task": {
                "notebook_path": "/Shared/Setup/25_Recreate_Persisted",
                "base_parameters": {
                    "TABLE_LIST": dlp_list
                }
            }
        }
    
def write_file_json(json_object, ur_no, config_name, path_adb_tmp):
    try:
        Write_Path = f'{path_adb_tmp}/JSON_CONVERTED_{ur_no}_{config_name}.json'

        # Writing JSON to a File
        with open(Write_Path, 'w') as f:
            f.write(json_object)

        print(f"JSON_CONVERTED_{ur_no}_{config_name}.json >>Export File Success")

    except Exception as e:
        print(f"Fail to write JSON >>{str(e)}")

def write_file_txt(dataframe, ur_no, path_adls_tmp):
    try:
        Write_Path = f'{path_adls_tmp}/00_deployList_{ur_no}_utilities.txt'

        # Writing JSON to a File
        with open(Write_Path, 'a') as f:
            StrSet = dataframe.to_string(header=False, index=False, justify='left')
            f.write(f'{StrSet.replace(" ","")}\n')
            f.close()

        print(f"adls: 00_deployList_{ur_no}_utilities.txt >>Export File Success")

    except Exception as e:
        print(f"Fail to write TXT >>{str(e)}")

def write_file_txt_of_json(dataframe, ur_no, path_adb_tmp):
    try:
        Write_Path = f'{path_adb_tmp}/00_deployList_{ur_no}_utilities.txt'
        # Writing JSON to a File
        with open(Write_Path, 'a') as f:
            StrSet = dataframe.to_string(header=False, index=False, justify='left')
            f.write(f'{StrSet.replace(" ","")}\n')
            f.close()

        print(f"adb: 00_deployList_{ur_no}_utilities.txt >>Export File Success")

    except Exception as e:
        print(f"Fail to write TXT >>{str(e)}")

def create_git_command(ur_no, month_period, deploy_date, have_config, have_view, have_table, output_path, email):
    split_ur_no = ur_no.split('_')
    ur_code = ('_').join(split_ur_no[:3])
    sr_epic = ur_code.split('_')[1]
    sr_feature = ur_code.split('_')[2]
    ic_code = ' '+split_ur_no[-1] if len(split_ur_no) == 4 else ""
    branch_name = f'feature/{month_period}_{ur_no}'
    checkout = 'git checkout '
    merge = 'git merge --no-ff origin/'
    commit = 'git commit -m '
    push = 'git push origin'
    environment = ['dev', 'sit', 'release', f'imprelease/{month_period}_{deploy_date}']

    git_command = f"""
### Git feature command ###
{checkout}{branch_name}
git add .
git status
{commit}'{ur_no} [comment]'
{push} {branch_name}

### Git merge dev/sit/release/imprelease command ###"""

    for env in environment:
        git_command += f"""
{checkout}{env}
git fetch -p && git pull origin
{merge}{branch_name}
{commit}'{ur_no} [comment]'
{push}
"""

    git_command += f"""
### ADLS Jenkins Parameters ###
{'CHANGE_NO'}\t\t:{ur_no}/(SREQ-UAT)
{'INPUT_HASH'}\t\t:
{'blob_file'}\t\t\t\t:deployment_release/{month_period}/{ur_no}/00_deployList_{ur_no}_all.txt
{'UR'}\t\t\t\t\t:{ur_code}
{'SENDING_EMAIL'}\t:{email}
{'VERSION'}\t\t\t:

### ADB Jenkins Parameters ###"""
    if have_config:
        git_command += f"""
{'CHANGE_NO'}\t\t\t:{ur_no}/(SREQ-UAT)
{'INPUT_HASH'}\t\t\t:
{'exec_notebook_file1'}\t:ADB_01/deployment_release/{month_period}/{ur_no}/00_deployList_{ur_no}_utilities.txt
{'CLUSTER_ID'}\t\t\t:DEV(0402-042346-cyaeulo4) SIT(0829-162655-y5wtmkoe) UAT(0829-172743-etfsoye4)
{'UR'}\t\t\t\t\t\t:{ur_code}
{'SENDING_EMAIL'}\t\t:{email}
{'VERSION'}\t\t\t\t:
"""
    if have_table:
        git_command += f"""
{'CHANGE_NO'}\t\t\t:{ur_no}/(SREQ-UAT)
{'INPUT_HASH'}\t\t\t:
{'exec_notebook_file1'}\t:ADB_01/deployment_release/{month_period}/{ur_no}/01_deployList_{ur_no}_applyTableChange.txt
{'CLUSTER_ID'}\t\t\t:DEV(0402-042346-cyaeulo4) SIT(0829-162655-y5wtmkoe) UAT(0829-172743-etfsoye4)
{'UR'}\t\t\t\t\t\t:{ur_code}
{'SENDING_EMAIL'}\t\t:{email}
{'VERSION'}\t\t\t\t:
"""
    if have_view:
        git_command += f"""
{'CHANGE_NO'}\t\t\t:{ur_no}/(SREQ-UAT)
{'INPUT_HASH'}\t\t\t:
{'exec_notebook_file1'}\t:ADB_01/deployment_release/{month_period}/{ur_no}/02_deployList_{ur_no}_createDDL.txt
{'CLUSTER_ID'}\t\t\t:DEV(0402-042346-cyaeulo4) SIT(0829-162655-y5wtmkoe) UAT(0829-172743-etfsoye4)
{'UR'}\t\t\t\t\t\t:{ur_code}
{'SENDING_EMAIL'}\t\t:{email}
{'VERSION'}\t\t\t\t:
"""
    git_command += f"""
### Move UAT Parameters ###
{'Short Description'}\t\t :[UAT] {ur_no} : EDW Azure
{'Description'}\t\t\t\t :[UAT] {ur_no} : EDW Azure
{'Environment'}\t\t\t :UAT
{'Application'}\t\t\t\t :EDW
{'Category'}\t\t\t\t :App - PC Application [Open System, Cloud]
{'Implementer Group'}\t :EDW3
{'Requester Group'}\t\t :EDW3
{'Planned start date'}\t :(today) optional
{'Planned end date'}\t\t :(today+3 workday) optional
{'Requester Name'}\t\t :Teerapat L.
{'UR No/SR Epic/IC No'} :{sr_epic}
{'SR Feature'}\t\t\t\t :{sr_feature}
{'Implementation Plan'} :edwcloud_adls, edwcloud_adb
> attach .zip packing folder and Playbook_UAT
"""
    print(git_command)
    #Write git command
    gitcmd_file = open(f"{output_path}/git_command_{ur_no}.txt", "w")
    gitcmd_file.write(git_command)
    gitcmd_file.close()

def create_git_form_folder(ur_no, month_period, path_osfolder):
    adb_git_folder = path_osfolder+'/edwcloud_adb/src/Job/ADB_01'
    adls_git_folder = path_osfolder+'/edwcloud_adls/src'
    create_folder(adb_git_folder)
    create_folder(adls_git_folder)

    all_tmp_items = os.listdir(path_osfolder)
    deployList_all = [path_osfolder+'/'+x for x in all_tmp_items if 'deployList' in x]
    with open(deployList_all[0], 'r') as f:
        contents = f.readlines()

    #Create sub all folder and list files in side of edwcloud_adls
    for content in contents:
        try:
            for_create_sub_folder = content.split(',')[2].split('/')[:-1]
            file_for_move = content.split(',')[2].split('/')[-1]
            sub_sub_folder = adls_git_folder+'/'+'/'.join(for_create_sub_folder)

            create_folder(sub_sub_folder)
            # with open(sub_sub_folder+f"/files_for_{'-'.join(for_create_sub_folder)}.txt", 'a') as f:
            #     f.write(f'{file_for_move}\n')
            f = open(sub_sub_folder+f"/{file_for_move}.txt", 'w')
            f.close()
        except Exception as e:
            print(str(e))
            pass
    
    #Create deployment_release folder and move 00_deployList_[ur].txt
    adls_deploylist_path = adls_git_folder+f'/deployment_release/{month_period}/{ur_no}'
    create_folder(adls_deploylist_path)
    shutil.move(deployList_all[0], adls_deploylist_path+f"/{deployList_all[0].split('/')[-1]}") #edit change .copy to .move

    ### Create adb git folder ###
    #Create deployment_release folder and move deployList_[ur].txt
    adb_path_bf = [path_osfolder+'/'+x for x in all_tmp_items if '.' not in x and 'adb_tmp' in x][0]

    #Get deployList files and json files to list
    adb_deploylist_file_list = [adb_path_bf+'/'+x for x in os.listdir(adb_path_bf) if '.txt' in x]
    adb_json_file_list = [adb_path_bf+'/'+x for x in os.listdir(adb_path_bf) if '.json' in x]

    #Create deployment_release folder in edwcloud_adb and move deployList_[ur].txt
    adb_deploylist_path = adb_git_folder+f"/deployment_release/{month_period}/{ur_no}/"
    create_folder(adb_deploylist_path)

    for deploy_list_file in adb_deploylist_file_list:
        with open(deploy_list_file, 'r') as f:
            contents = f.readlines()
        shutil.copy(deploy_list_file, adb_deploylist_path+deploy_list_file.split('/')[-1])

        for content in contents:
            for_create_sub_folder = content.split('/')[3]
            file_for_move = remove_suffix(content.split('/')[-1], '\n')
            sub_sub_folder = adb_git_folder+f"/{month_period}/{ur_no}/"+for_create_sub_folder
            
            create_folder(sub_sub_folder)
            for x in adb_json_file_list:
                if file_for_move in x:
                    shutil.copy(x, sub_sub_folder+'/'+file_for_move)