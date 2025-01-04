from wayne_utils import load_data,save_data
from tqdm import tqdm
import os
from construct_utils import load_schema_command, load_csv_data
from common_utils import get_nebula_session

from n3_config import WORK_DIR


def use_space( dataset_name, session ):
    "切换到指定space，如不存在，则先从schema构建"
    space_name = dataset_name if dataset_name != "harrypotter" else "harrypotter_new"
    spaces = session.execute( "SHOW SPACES;" )
    if not spaces.is_succeeded():
        raise Exception( f"Session不可用！")
    if space_name in [ list(spaces)[i].values()[0].as_string() for i in range(len(list(spaces)))]:
        print( f"Space {space_name} 已经存在！")
    else:
        print( f"Space {space_name} 不存在，加载schema并构建！")
        # 加载并修正schema
        ddl_command_list, clear_command_list = load_schema_command( dataset_name )
        if dataset_name == "disease":
            clear_command_list[6] = clear_command_list[6].replace( "describe", "describes")
        for line in clear_command_list:
            session.execute( line )
    result = session.execute( f"USE {space_name};" )
    if not result.is_succeeded():
        raise Exception( f"切换到Space {space_name}失败！") 
    else:
        print( f"切换到Space {space_name}成功，当前space情况如下：")
        print(f"""
            当前全部Space为：{session.execute( f"Show Spaces" )}，其中：
            {space_name}的Vertex有：{session.execute( f"Show TAGs" )}
            {space_name}的Edges有：{session.execute( f"Show Edges" )}""")
    # db_executor( f"DESCRIBE SPACE nba" )
    # db_executor( "SHOW SPACES;" )
    # db_executor( f"DROP SPACE disease" )
    # 获取三个子数据库字段命名
    schema_dict = load_data( "edge_vertex.json", "json" )
    ban_field = [ ":RANK" ]
    return schema_dict, ban_field
    
def get_add_command( dataset_name, schema_dict, ban_field, overwrite = False):
    """
    从CSV文件中构建INSERT语句。
    """
    schema_type = schema_dict[dataset_name]
    # （1）遍历vertex和edge两种类型
    for _type in schema_type.keys():
        type_name_list = schema_type[_type]["name_list"]
        type_vid_list = schema_type[_type]["vid"]
        # （2）遍历该类型中所有对象实例
        for sub_entity in type_name_list: 
            command_path = f"construct_db/{dataset_name}/constrcut_command_{_type}_{sub_entity}.json"
            if os.path.exists( command_path ):
                if overwrite:
                    print( f"已存在：constrcut_command_{_type}_{sub_entity}.json，但进行重写")
                else:
                    print( f"已存在：constrcut_command_{_type}_{sub_entity}.json，不重写")
                    continue
            entity_instance = load_csv_data( dataset_name, sub_entity)                  # 加载CSV
            entity_fields = entity_instance.keys()
            field_list = {}
            vid_field, src_filed, des_field = None, None, None
            
            if _type == "VERTEX":                                                       # 获取vid字段字符串
                vid_field = type_vid_list[0]
            else:
                src_filed, des_field = type_vid_list[0], type_vid_list[1]
            
            for field in entity_fields:                                                 # 获取其他字段字符串
                if field != "embedding" and field not in type_vid_list and field not in ban_field:
                    if dataset_name == "nba":
                        field_type = field.split(":")[1].strip()
                        try:
                            field_name = field.split(":")[0].split(".")[1].strip()
                        except:
                            raise Exception( _type, type_vid_list, field )
                    else:
                        field_name, field_type = field, None
                    field_list[field ] = (field_name, field_type)

            field_list_str = ""
            new_line_list = []
            
            # （3）遍历所有数据行
            for index in tqdm( range( len(entity_instance) ), desc=f"Processing {_type, sub_entity }"):
                data_line = entity_instance.iloc[index, :]
                # （4）遍历每行的所有列，有效字段"""
                other_field_ins = ""
                for i in range( len(list(field_list.keys()))):
                    key = list(field_list.keys())[i]                # 获取列名
                    value = str(data_line[key])
                    if (field_list[key][1] == "string" or dataset_name in ["harrypotter", "disease"]):
                        if dataset_name == "disease":
                            if '"' in value:
                               value = value.replace( '"', '\\"')
                            other_field_ins += f"""\"{value}\""""
                        else:
                            other_field_ins += f"""\"{value}\""""    
                    else:
                        other_field_ins += str(value)

                    if index == 0:                                  # 构建首行属性名表
                        field_list_str += f"{field_list[key][0]}"
                        if i < len(list(field_list.keys()))-1:
                            field_list_str += ", "
                    
                    if i < len(list(field_list.keys()))-1:          # 构建其他行属性值表
                        other_field_ins += ", "
                
                if _type == "VERTEX":                               # 构建行字符串：区分vertex还是edge
                    new_line_str = f"""\"{data_line[vid_field]}\": ({other_field_ins})"""
                else:
                    new_line_str = f"""\"{data_line[src_filed]}\" -> \"{data_line[des_field]}\": ({other_field_ins})"""
                first_line = f"INSERT {_type} IF NOT EXISTS {sub_entity}({field_list_str}) VALUES\n"
                new_line_list.append( first_line + new_line_str + ";" )

            save_data( new_line_list, command_path )

def execute_construct_command( dataset_name, session ):
    schema_type = schema_dict[dataset_name]
    for _type in schema_type.keys():
        type_name_list = schema_type[_type]["name_list"]
        type_vid_list = schema_type[_type]["vid"]
        for sub_entity in type_name_list:
            command_list = load_data( f"construct_db/{dataset_name}/constrcut_command_{_type}_{sub_entity}.json", "json" )
            for index in tqdm( range( len(command_list) ), desc=f"Processing {_type, sub_entity }"):
                result = session.execute( command_list[index] )
                if result.is_succeeded():
                    continue
                else:
                    print( f"{_type}_{sub_entity}；Index: {index}，error code = {result.error_code()},error_msg = {result.error_msg()}")
                    break

if __name__ == "__main__":
    work_dir = WORK_DIR
    os.chdir( work_dir )
    # 获取nebula数据库session
    session = get_nebula_session( )

    # 指定子数据集
    dataset_name_list = ["nba", "harrypotter", "disease"]
    for dataset_name in dataset_name_list:
        # dataset_name = dataset_name_list[1]
        print( f"————————————————————————————————————处理数据集{dataset_name}—————————————————————————————————")
        
        # 选择Space
        schema_dict, ban_field = use_space( dataset_name, session )
        # 构建数Insert语句：可跳过
        get_add_command( dataset_name, schema_dict, ban_field, overwrite = False)
        # 执行Insert语句
        execute_construct_command( dataset_name, session )

