import pandas as pd
import re
import json

var_id_map = {0: 'AA', 1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J'}
var_id_map_reverse = {v: k for k, v in var_id_map.items()}


def pretty_test_result(test_result, test_variant, user_cnt_result):
    
    tmp = pd.DataFrame(test_result.values())
    tmp['mean_diff%'] = tmp['mean_diff'] / tmp['group_1_mean']
    tmp = tmp.drop(columns=['z_stat', 'se'])
    metric_name = list(test_result.keys())[0].replace('all_', '')

    tmp = tmp[['group_1_mean', 'group_2_mean', 'mean_diff', 'mean_diff%', 'is_significant', 'p_value']]\
        .rename(columns={
            'group_1_mean': 'var_' + test_variant[0],
            'group_2_mean': 'var_' + test_variant[1]
        })\
        .rename(index={
            0: metric_name
        })\
        .T\
        .reset_index()\
        .rename(columns={'index': 'group'})
    
    if re.findall(r'.*(e1|e2|interest|consideration|add_to_cart|purchase|rate|\%)$', metric_name):
        tmp[metric_name] = format_col_result(tmp[metric_name], 'rate')
    else:
        tmp[metric_name] = format_col_result(tmp[metric_name], 'number')
    
    # merge user_cnt
    user_cnt_result = user_cnt_result\
        .assign(
            group=lambda df: df['group'].map({
                'group_1': 'var_' + test_variant[0],
                'group_2': 'var_' + test_variant[1]
            })
        )
    
    tmp = tmp\
        .merge(
            user_cnt_result,
            on='group',
            how='left'
        )[['group', 'user', metric_name]]\
        .fillna('')
    
    tmp.columns = [x.capitalize() for x in tmp.columns]
    
    return tmp


def count_test_result_user(df, check_var_id_list, merge_aa):
    
    if merge_aa:
        df.loc[df['var_id'] == 0, 'var_id'] = 1
    
    var_user_cnt = df\
        .groupby(['var_id'])\
        .agg(
            user=('beacon_uid', 'nunique')
        )\
        .reset_index()
    
    var_user_cnt.loc[var_user_cnt['var_id'] == check_var_id_list[0], 'group'] = 'group_1'
    var_user_cnt.loc[var_user_cnt['var_id'] == check_var_id_list[1], 'group'] = 'group_2'
    
    var_user_cnt = var_user_cnt\
        .query('group in ["group_1", "group_2"]')\
        [['group', 'user']]\
        .assign(
            user=lambda df: df['user'].apply(lambda x: f"{x:.0f}")
        )
    
    return var_user_cnt


def highlight_significant(val):
    if val == 'True':
        return 'color: red'
    if val == 'False':
        return 'color: green'
    return ''


def highlight_pass(val):
    if val == 'Fail':
        return 'color: red'
    if val == 'Pass':
        return 'color: green'
    return ''


def format_col_result(col, mode):

    col = list(col)
    if mode == 'rate':
        output = [
            f"{col[0]:.2%}",
            f"{col[1]:.2%}",
            f"{col[2]:.2%}",
            f"{col[3]:.2%}",
            f"{col[4]}",
            f"{col[5]:.2f}"
        ]
    else:
        output = [
            f"{col[0]:.2f}",
            f"{col[1]:.2f}",
            f"{col[2]:.2f}",
            f"{col[3]:.2%}",
            f"{col[4]}",
            f"{col[5]:.2f}"
        ]
    
    return output


def format_preview_data_result(data):
    
    # 過濾掉 101, 102 等內部人員分組的資料
    data = data\
        .query(f'var_id in {list(var_id_map.keys())}')\
        .assign(
            var_id=lambda df: df['var_id'].map(var_id_map)
        )
    
    # total user cnt
    total_beacon_uid = data['beacon_uid'].nunique()

    # 重複分組 user cnt
    raw_multi_grp_table = data\
        .groupby(['beacon_uid'])\
        .agg(
            n_var_id=('var_id', 'nunique')
        )\
        .query('n_var_id > 1')\
        .reset_index()\
        .groupby(lambda _: '')\
        .agg(
            multi_grp_users=('beacon_uid', 'nunique')
        )\
        .assign(
            multi_grp_user_pct=lambda df: df['multi_grp_users'] / total_beacon_uid
        )\
        .assign(
            multi_grp_user_pct=lambda df: df['multi_grp_user_pct'].apply(lambda x: f"{x:.4%}")
        )\
        .rename(columns={
            'multi_grp_users': '重複分組用戶數',
            'multi_grp_user_pct': '比例'
        })
    
    # total var_id user cnt
    raw_var_id_stat = data\
        .assign(
            device='total'
        )\
        .sort_values('var_id')\
        .pivot_table(
            index='device',
            columns='var_id',
            values='beacon_uid',
            aggfunc='nunique'
        )\

    # device var_id user cnt
    device_var_id_stat = data\
        .sort_values('var_id')\
        .pivot_table(
            index='device',
            columns='var_id',
            values='beacon_uid',
            aggfunc='nunique'
        )

    var_id_stat = pd.concat([
        raw_var_id_stat,
        device_var_id_stat
    ])
    
    var_id_stat_pct = var_id_stat\
        .apply(lambda x: x / x.sum(), axis=1)\
        .apply(lambda x: [f"{y:.2%}" for y in x])\
        .reset_index()
    
    var_id_stat = var_id_stat.reset_index()
    
    return raw_multi_grp_table, var_id_stat, var_id_stat_pct


def format_metric_input_as_json(input):
    '''
    格式化 Input 為 json 檔
    '''
    # 將 JSON 資料格式化，指定縮排層級
    formatted_json = json.dumps(input, indent=3)
    
    return formatted_json

