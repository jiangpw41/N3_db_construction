import os
import pandas as pd
from wayne_utils import load_data, save_data


# 加载数据库schema构建指令
def load_schema_command( dataset_name ):
    if dataset_name != "harrypotter":
        dir_name = dataset_name  
        DDL_FILE_PATH = f'dataset/{dir_name}/{dir_name}_ddl.ngql'
    else:
        dir_name = "harrypotter"
        DDL_FILE_PATH = f'dataset/{dir_name}/{dir_name}_new_ddl.ngql'
    with open(DDL_FILE_PATH, 'r', encoding='utf-8') as file:
        ddl_commands =  file.read()
    ddl_command_list = ddl_commands.split("\n")
    clear_command_list = []
    for line in ddl_command_list:
        if line.startswith("CREATE") or line.startswith("USE ") or line.startswith(":sleep")  :
            clear_command_list.append( line.strip() )

    return ddl_command_list, clear_command_list

def name_map( name ):
    maps_ = {
        'characters': 'character',
        'rel_kindred': 'kindred',
        'rel_belong_to': 'belong_to',
        'rel_learn_from': 'learn_from'
    }
    if name in maps_:
        return maps_[name]
    else:
        return name

def file_map( name ):
    maps_ = {
        'character': 'characters',
        'kindred': 'rel_kindred',
        'belong_to': 'rel_belong_to',
        'learn_from': 'rel_learn_from'
    }
    if name in maps_:
        return maps_[name]
    else:
        return name

# 加载csv数据
def load_csv_data( dataset_name, file_name = None):
    DDL_FILE_PATH = f'dataset/{dataset_name}/data'
    file_list = os.listdir( DDL_FILE_PATH )
    if file_name == None:
        ret = {}
        for file in file_list:
            if file.endswith( ".csv" ):
                sub_class_name = name_map( file[:-4] )
                node_csv = pd.read_csv( os.path.join( DDL_FILE_PATH, file ),index_col=False, encoding='utf-8')
                ret[ sub_class_name ] = node_csv
        return ret
    else:
        node_csv = None
        file_name = file_map( file_name )
        for file in file_list:
            if file.endswith( ".csv" ) and file[:-4] == file_name:
                node_csv = pd.read_csv( os.path.join( DDL_FILE_PATH, file ),index_col=False)
        return node_csv

if __name__ == "__main__":
    load_csv_data( "harrypotter", "character")