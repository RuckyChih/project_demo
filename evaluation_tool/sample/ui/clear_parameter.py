import streamlit as st
import re
import pandas as pd


def clear_session_state_parameters(regex):

    # print('------')
    for key in list(set(st.session_state.keys())):
        if re.match(regex, key):
            if re.match('.*btn.*', key):
                continue
            
            # print(key)
            st.session_state.stash_value = st.session_state.get(key)
            
            if isinstance(st.session_state.stash_value, bool):
                st.session_state[key] = False
            if isinstance(st.session_state.stash_value, str):
                st.session_state[key] = ''
            if isinstance(st.session_state.stash_value, (int, float)):
                st.session_state[key] = 1
            if isinstance(st.session_state.stash_value, list):
                st.session_state[key] = []
            if isinstance(st.session_state.stash_value, dict):
                st.session_state[key] = {}

    return None


def clear_spec_metric_trigger_parameter():

    for key in list(set(st.session_state.keys())):
        if re.match('^spec_metric_trigger.*', key):
            if re.match('.*btn.*', key):
                continue
            
            st.session_state.stash_value = st.session_state.get(key)
            
            if isinstance(st.session_state.stash_value, bool):
                st.session_state[key] = False
            if isinstance(st.session_state.stash_value, str):
                st.session_state[key] = ''
            if isinstance(st.session_state.stash_value, (int, float)):
                st.session_state[key] = 1
            if isinstance(st.session_state.stash_value, list):
                st.session_state[key] = []
            if isinstance(st.session_state.stash_value, dict):
                st.session_state[key] = {}
                
    st.session_state['spec_metric_trigger'] = {}
    
    return None


def clear_spec_metric_main_condition_saver_parameter():

    for key in list(set(st.session_state.keys())):
        if re.match('^spec_metric_main_condition_saver.*', key) or (key == "spec_metric_main_tracking_params"):
            if re.match('.*btn.*', key):
                continue
            
            st.session_state.stash_value = st.session_state.get(key)
            
            if isinstance(st.session_state.stash_value, bool):
                st.session_state[key] = False
            if isinstance(st.session_state.stash_value, str):
                st.session_state[key] = ''
            if isinstance(st.session_state.stash_value, (int, float)):
                st.session_state[key] = 1
            if isinstance(st.session_state.stash_value, list):
                st.session_state[key] = []
            if isinstance(st.session_state.stash_value, dict):
                st.session_state[key] = {}
                
    st.session_state['spec_metric_main_condition_saver'] = {}
    st.session_state['spec_metric_main_tracking_params'] = {}
    
    return None


def clear_spec_metric_main_parameter():

    for key in list(set(st.session_state.keys())):
        if re.match('^spec_(?!.*saver)(?!metric_main_tracking_params$).*$', key):
            if re.match('.*btn.*', key):
                continue
            
            # print(key)
            st.session_state.stash_value = st.session_state.get(key)
            
            # Selectbox default 選項
            spec_selectbox_default_map = {
                'spec_trigger_source': 'tracking',
                'spec_metric_trigger_journey_metric': 'funnel_e1',
                'spec_main_source': 'tracking',
                'spec_main_metric_journey_key': 'funnel_e1',
                'spec_main_group': 'user',
                'spec_main_method': 'count',
                'spec_need_metric_trigger': False,
                'spec_need_metric_condition': False
            }
            if key in list(spec_selectbox_default_map.keys()):
                st.session_state[key] = spec_selectbox_default_map.get(key)
            else:
                if isinstance(st.session_state.stash_value, bool):
                    st.session_state[key] = False
                if isinstance(st.session_state.stash_value, str):
                    st.session_state[key] = ''
                if isinstance(st.session_state.stash_value, (int, float)):
                    st.session_state[key] = 1
                if isinstance(st.session_state.stash_value, list):
                    st.session_state[key] = []
                if isinstance(st.session_state.stash_value, dict):
                    st.session_state[key] = {}
    
    st.session_state['spec_metric_main'] = {}
    clear_spec_metric_trigger_parameter()
    clear_spec_metric_main_condition_saver_parameter()
    
    # Clear Output
    for key in ['spec_metric_sql_query', 'spec_metric_format_result']:
        st.session_state.stash_value = st.session_state.get(key)
    st.session_state['spec_metric_sql_query'] = ''
    st.session_state['spec_metric_format_result'] = pd.DataFrame()
    
    return None


def clear_exp_config_metric_trigger_parameter():

    for key in list(set(st.session_state.keys())):
        if re.match('^exp_config_trigger.*', key):
            if re.match('.*btn.*', key):
                continue
            
            st.session_state.stash_value = st.session_state.get(key)
            
            if isinstance(st.session_state.stash_value, bool):
                st.session_state[key] = False
            if isinstance(st.session_state.stash_value, str):
                st.session_state[key] = ''
            if isinstance(st.session_state.stash_value, (int, float)):
                st.session_state[key] = 1
            if isinstance(st.session_state.stash_value, list):
                st.session_state[key] = []
            if isinstance(st.session_state.stash_value, dict):
                st.session_state[key] = {}
                
    st.session_state['exp_config_metric_trigger'] = {}
    
    return None
