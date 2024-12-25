import streamlit as st
import re
from sample.ui.format_test_result import format_metric_input_as_json


class TrackingTriggerInput:
    def __init__(self, saver_name, saver) -> None:
        self.saver_name = saver_name
        self.saver = saver
    
    # 定義函數來添加新的 EventName 容器
    def add_input_eventname_expander(self, eventname, params_num):
        self.saver[eventname] = {
            'params_num': params_num
        }
        # 添加 default 設定
        self.saver[eventname]['params_inputs'] = []
        for i in range(params_num):
            self.saver[eventname]['params_inputs'].append(
                [
                    [
                        st.session_state.get(f"{self.saver_name}_{eventname}_{i}_0_name", ""),
                        st.session_state.get(f"{self.saver_name}_{eventname}_{i}_0_dict_key", ""),
                        st.session_state.get(f"{self.saver_name}_{eventname}_{i}_0_ope", ""),
                        st.session_state.get(f"{self.saver_name}_{eventname}_{i}_0_value", "")
                    ]
                ]
            )
    
    def remove_input_eventname_expander(self):
        
        exist_eventname_list = list(self.saver.keys())
        if len(exist_eventname_list) > 0:
            del self.saver[exist_eventname_list[-1]]

    # 定義函數來添加新的 Parameter Unit 容器
    def add_input_parameter_unit(self, eventname, params_id):
        
        i = len(self.saver[eventname]['params_inputs'][params_id])
        
        self.saver[eventname]['params_inputs'][params_id].append(
            [
                st.session_state.get(f"{self.saver_name}_{eventname}_{params_id}_{i+1}_name", ""),
                st.session_state.get(f"{self.saver_name}_{eventname}_{params_id}_{i+1}_dict_key", ""),
                st.session_state.get(f"{self.saver_name}_{eventname}_{params_id}_{i+1}_ope", ""),
                st.session_state.get(f"{self.saver_name}_{eventname}_{params_id}_{i+1}_value", "")
            ]
        )
        
    def remove_input_parameter_unit(self, eventname, params_id):
        
        if len(self.saver[eventname]['params_inputs'][params_id]) > 0:
            if len(self.saver[eventname]['params_inputs'][params_id]) > 1:
                self.saver[eventname]['params_inputs'][params_id].pop()
        
    def update_input_parameter_unit(self, eventname, params_id, idx, col):
        
        if col == 'name':
            tmp = st.session_state.get(f"{self.saver_name}_{eventname}_{params_id}_{idx}_name", "")
            self.saver[eventname]['params_inputs'][params_id][idx][0] = tmp
        if col == 'dict_key':
            tmp = st.session_state.get(f"{self.saver_name}_{eventname}_{params_id}_{idx}_dict_key", "")
            self.saver[eventname]['params_inputs'][params_id][idx][1] = tmp
        if col == 'ope':
            tmp = st.session_state.get(f"{self.saver_name}_{eventname}_{params_id}_{idx}_ope", "")
            self.saver[eventname]['params_inputs'][params_id][idx][2] = tmp
        if col == 'value':
            tmp = st.session_state.get(f"{self.saver_name}_{eventname}_{params_id}_{idx}_value", "")
            self.saver[eventname]['params_inputs'][params_id][idx][3] = tmp


def show_trigger_input(trigger_input_class):
    
    # ===== 定義函數來顯示輸入容器 =====
    def show_trigger_input_params_unit(trigger_input_class, eventname, params_id, units):
        '''
        units 為 [(name, dict_key, ope, value), (name, dict_key, ope, value)] 的 list，彼此間條件為 AND
        '''
        with st.container():
            for idx, unit in enumerate(units):
                col1, col2, col3, col4, col5, col6 = st.columns(6)
                if idx == 0:
                    col1.button(":heavy_plus_sign: Add New Parameter w/ AND condition",
                                on_click=trigger_input_class.add_input_parameter_unit,
                                args=[eventname, params_id],
                                key=f"{trigger_input_class.saver_name}_{eventname}_{params_id}_add_btn"
                                )
                    col2.button(":heavy_minus_sign: Remove Parameter",
                                on_click=trigger_input_class.remove_input_parameter_unit,
                                args=[eventname, params_id],
                                key=f"{trigger_input_class.saver_name}_{eventname}_{params_id}_remove_btn"
                                )
                else:
                    col1.markdown("")
                    col2.markdown(f'{"AND" if idx != 0 else ""}')
                col3.text_input(
                    "Parameter",
                    key=f"{trigger_input_class.saver_name}_{eventname}_{params_id}_{idx}_name",
                    on_change=trigger_input_class.update_input_parameter_unit,
                    args=[eventname, params_id, idx, 'name'],
                    placeholder='screen_name, section, ...'
                )
                col4.text_input(
                    "Dict_list Key (Optional)",
                    key=f"{trigger_input_class.saver_name}_{eventname}_{params_id}_{idx}_dict_key",
                    on_change=trigger_input_class.update_input_parameter_unit,
                    args=[eventname, params_id, idx, 'dict_key'],
                )
                col5.text_input(
                    "Operator",
                    key=f"{trigger_input_class.saver_name}_{eventname}_{params_id}_{idx}_ope",
                    on_change=trigger_input_class.update_input_parameter_unit,
                    args=[eventname, params_id, idx, 'ope'],
                    placeholder='>, <, >=, <=, =, in, contains'
                )
                col6.text_input(
                    "Value",
                    key=f"{trigger_input_class.saver_name}_{eventname}_{params_id}_{idx}_value",
                    on_change=trigger_input_class.update_input_parameter_unit,
                    args=[eventname, params_id, idx, 'value'],
                    placeholder='List Input like {value1},{value2},{value3}'
                )

    def show_trigger_input_params(trigger_input_class, eventname, params_inputs):
        '''
        params_inputs 為 [units, units] 的 list，彼此間條件為 OR
        '''
        
        for param_id, param in enumerate(params_inputs):
            if param_id != 0:
                st.markdown("OR")
            with st.container(border=True):
                show_trigger_input_params_unit(
                    trigger_input_class,
                    eventname,
                    param_id,
                    trigger_input_class.saver[eventname]['params_inputs'][param_id]
                )

    def show_trigger_input_expander(trigger_input_class):
        for event_name, inputs in trigger_input_class.saver.items():
            if len(trigger_input_class.saver) > 1:
                if event_name != list(trigger_input_class.saver.keys())[0]:
                    st.markdown('OR')

            with st.expander(event_name, expanded=True):
                show_trigger_input_params(
                    trigger_input_class,
                    event_name,
                    inputs["params_inputs"]
                )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.text_input('Event Name',
                      key=f'{trigger_input_class.saver_name}_trigger_eventname',
                      placeholder='view_item, impression_section, ...')
    with col2:
        st.number_input('Parameter Nums', min_value=1, step=1, format='%d',
                        key=f'{trigger_input_class.saver_name}_trigger_params_num',
                        help='Add Event Parameter w/ OR condition')
    with col3:
        if st.button(":heavy_plus_sign: Add New Event Name & Parameter w/ OR condition",
                     key=f'{trigger_input_class.saver_name}_trigger_eventname_add_btn',
                     help='Input Event Name First, Then Click Add Button'):
            
            if st.session_state.get(f'{trigger_input_class.saver_name}_trigger_eventname') == '':
                st.error('Please Input Event Name')
            else:
                trigger_input_class.add_input_eventname_expander(
                    st.session_state.get(f'{trigger_input_class.saver_name}_trigger_eventname'),
                    st.session_state.get(f'{trigger_input_class.saver_name}_trigger_params_num')
                )
    with col4:
        if st.button(":heavy_minus_sign: Remove Event Name & Parameter",
                     key=f'{trigger_input_class.saver_name}_trigger_eventname_remove_btn',
                     help='Click Remove Button, Then Remove Last Added Event Name & Parameter'):
            trigger_input_class.remove_input_eventname_expander()
    show_trigger_input_expander(trigger_input_class)


def format_saver_output(saver):
    
    if saver == {}:
        return {}
    
    output = {}
    for event_name, value in saver.items():
        output[event_name] = []
        params_inputs = value["params_inputs"]
        
        if (len(params_inputs) == 1) & (len(params_inputs[0]) == 1) & (params_inputs[0][0][0] == ''):
            output[event_name] = None
        else:
            for params_input_units in params_inputs:
                tmp = {}
                for unit in params_input_units:
                    tmp[unit[0]] = {
                        'dict_key': unit[1],
                        'cal': unit[2],
                        'value': unit[3].replace(' ', '').split(',') if ',' in unit[3] else unit[3]  # 針對 list 形式輸入處理
                    }
                output[event_name].append(tmp)
        
            output[event_name] = tuple(output[event_name])
    
    return output


if __name__ == '__main__':

    # ========== Basic Config ==========
    st.set_page_config(layout="wide")  # 設定頁面為 full width 模式
    
    # 初始化 session state 以保存輸入容器
    if 'spec_metric_trigger_saver' not in st.session_state:
        st.session_state.spec_metric_trigger_saver = {}

    spec_metric_trigger_saver_class = TrackingTriggerInput(
        saver_name='spec_metric_trigger',
        saver=st.session_state.spec_metric_trigger_saver
    )
    with st.container():
        show_trigger_input(
            trigger_input_class=spec_metric_trigger_saver_class
        )
        
        st.code(
            format_metric_input_as_json(
                format_saver_output(st.session_state.spec_metric_trigger_saver)
            )
        )
    
    print(st.session_state.spec_metric_trigger_saver)
    print(format_saver_output(st.session_state.spec_metric_trigger_saver))
