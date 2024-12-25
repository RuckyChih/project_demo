import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Tuple, Union
import sqlparse
import json


# 用一天的 param 抓出 param data type ，如果有更新需要手動記錄在 params.csv
params_type = pd.read_csv('docs/params.csv').drop_duplicates(subset='param', keep=False)
params_type = params_type.set_index('param').value_type.to_dict()


def format_sql_query(sql):
    '''
    格式化 SQL Query
    '''
    # 使用 sqlparse 格式化 SQL
    formatted_sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
    
    return formatted_sql


def list_to_str(list_: Union[str, List[str]]) -> str:
    '''
    Input: [a,b,c]
    Output: 'a','b','c'
    '''

    if not isinstance(list_ , list):
        list_ = [list_]
        
    return "'" +"','".join(list_) + "'"


def get_param_type(param, dict_key=None):
    '''
    用來抓 event param 的 data type，其中 dict list 會多做一層處理，function 直接輸入或是
    
    '''

    if param in params_type.keys():
        param_ = params_type[param]
        # 如果是 dict 還需要多輸一層 dict_key
        if param_ == "dict_list":
            if dict_key == None:
                raise KeyError("Dict_list type param should input dict_key")
            else:
                dict_param = "['"+dict_key+"']"
        else:
            dict_param = ''
    else:
        raise KeyError("params can't be found, please check input again, if the params is new please concat PA to deal with")
    
    return param_, dict_param

def input_data_type_tranform(param_value, param_type):
    if not isinstance(param_value , list):
        if str.lower(param_value) == "null":
            param_value_ = str.lower(param_value)
        else:
            if param_type in ['int_value' , 'int_list']:
                param_value_ = int(param_value)
            else:
                param_value_ = param_value
    else:
        param_value_ = param_value
    return param_value_
        


# 這裡一定要宣告 event_name ，如果有宣告 param 一定要賦予值
# 如果 cal 沒有值，默認用 in
# 默認 event_name 之間用 or 串接，param 如果用 () 包起，其中用 or ，其餘用 and 串接
# input: {event_name1:{param1:{cal:'in',value:[value_1 , value_2]}} , param2:{{cal:'=',value:value_1}} , event_name2:None , event3:(from_section : {value:'item'} , {from_screen:{value:'item'}}) , {info_dict:{cal:'like' ,value: '%u' , dict_key:'search_term'})}
# output: (event_name = 'event_name1' and event_params_map['param1'] in (value_1 , value_2) and event_params_map['param2'] = 'value_1') or (event_name = 'event_name2' and (event_params_map['from_section'] = 'item' or event_params_map['from_screen'] = 'item') and event_params_map['info_dict'][search_term] like '%u')
# 如果同個 event_param 想要用 and ，dict 輸入可以用 {search_term:'11' , search_term_:'asdas'} ，同一 dict key 加上 _
def event_map_to_query(trigger_map):
    trigger_query = ''
    s1 = []
    s2 = []
# {'view_item':({'from_screen':{'value':['item']} , 'from_section':{'value':['bottom_similar_item' , 'side_similar_item']}} , {'screen_name':{'cal':'like' , 'value':'%screen'}})}
    for trigger, params in trigger_map.items():
        # 接受重複的 view_item input
        if trigger.endswith('_') == 1:
            trigger = str.rstrip(trigger , '_')

        if params == None:
            s1 += ['(' + f"event_name = '{trigger}'" + ')']  # 先取出 event_name

        else:
            s_event = [f"event_name = '{trigger}' and "]
            s_or_temp = []
            # 將 tuple 中的 param 默認成 or 連結，其他用 and
            if isinstance(params, tuple):
                pass
            else:
                params = [params]
            for params_ in params:
                s_and_temp = []
                for param, value_dict in params_.items():
                    # 接受重複的 key input 
                    if param.endswith('_') == 1:
                        param = str.rstrip(param , '_')

                    if isinstance(value_dict, dict):
                        value_raw = value_dict
                    else:
                        # 如果是直接用 value 宣告則做轉化
                        value_raw = {}
                        value_raw['value'] = value_dict
                    param_, dict_param = get_param_type(
                        param, value_raw.get('dict_key'))
                    # 如果沒有 value raise error
                    if value_raw.get('value') == None:
                        raise KeyError('params value shouldn be null') 
                    
                    value_raw_  = input_data_type_tranform(value_raw['value'] ,param_)

                    ## 將輸入的 value 做轉化
                    if (value_raw_ == 'null')|(type(value_raw_) == int):
                        value_ = value_raw_
                    elif type(value_raw_) == str:
                        value_ = f"'{value_raw_}'"
                    elif type(value_raw_) == list:
                        value_ = list_to_str(value_raw_)
                    else:
                        raise ValueError('please check params input again')
                    if value_raw.get('cal') == 'contains':
                        value_ = str.replace(str.replace(value_ , '[' , '') , ']','')
                        s_and_temp += [
                        f"contains(event_params_map['{param}'].{param_}{dict_param},{value_})"]

                    # 如果沒有宣告算法，默認 in
                    else:
                        if (value_raw.get('cal') == None) | (value_raw.get('cal') == 'in'):
                            cal_ = 'in'
                            if type(value_) == int:
                                value_ = '(' + str(value_)+')'
                            else:
                                value_ = '(' + value_+')'
                        else:
                            cal_ = value_raw.get('cal')
                        s_and_temp += [
                            f"event_params_map['{param}'].{param_}{dict_param} {cal_} {value_}"]
                s_or_temp += ['('+' and '.join(s_and_temp) + ')']

            # 拼上所有的 param
            s_temp = ["(" + ''.join(s_event) +
                      '('+' or '.join(s_or_temp) + "))"]
            s2 += s_temp
            s2 = [' or '.join(s2)]  # 拼上 event_name 跟對應的 param
        s_total = s1 + s2  # 拼上沒有宣告 param 跟有的 event
    trigger_query += '('+' or '.join(s_total) + ')'
    return trigger_query

# journey 的 metric
# metric -> journey -> {columns_name:None}, {column_name:{cal: , value: }}
# metric -> tracking -> {event_name:{param:xxx , dict_key:xxx} } e.g.{measure_dwell_time:current_timer}{impression_section:position}{view_item:view_id}{impression_item:{param:dict_list , dict_key:position}}}


def metric_map_to_query(metric_map, source_type, journey_connector='or'):
    if source_type == 'journey':
        temp = []
        for m, c in metric_map.items():
            if (c == None) | (c == {}):
                c = {'cal': '', 'value': ''}
            temp += [f'{m} {c["cal"]} {c["value"]}']
        return '('+f' {journey_connector} '.join(temp)+')'
    elif source_type == 'tracking':
        if len(metric_map) > 1:
            raise KeyError('tracking only accept single metric')
        metric = list(metric_map.values())[0]
        if isinstance(metric, dict):
            param = metric['param']
            dict_key = metric.get('dict_key')
            param_, dict_param = get_param_type(param, dict_key)
        else:
            param = metric
            param_, dict_param = get_param_type(param)
        return f"event_params_map['{param}'].{param_}{dict_param}"


def _device_transform(device):
    # check device 的 type , str -> list
    # print(device.dtype)
    if isinstance(device, str):
        device_ = [device]
    elif isinstance(device, list):
        device_ = device
    else:
        raise ValueError('please check device input again')
    # check device 的值
    if len(list(filter(lambda x: x not in ['all', 'iphone', 'android', 'mobile', 'web'], device_))) > 0:
        raise ValueError('please check device input again')
    return device_


# 根據有沒有 device 做不同的 group by
def data_processing_agg(df, device, metric_list=None):
    if metric_list == None:
        return df
    else:
        if device == 'all':
            metric_list = list(map(lambda x: 'all_'+x, metric_list))
            return df.groupby(['var_id', 'beacon_uid'])[metric_list].mean().rename(lambda x: str.replace(x, 'all_', ''), axis=1).reset_index()
        else:
            if 'device' not in df.columns:
                raise ValueError(
                    'your dataframe dont have device columns, you can only check "all"')
            metric_list = list(map(lambda x: 'device_'+x, metric_list))
            return df.groupby(['var_id', 'beacon_uid' , 'device'])[metric_list].mean().rename(lambda x: str.replace(x, 'device_', ''), axis=1).reset_index()
