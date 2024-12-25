import streamlit as st
import pandas as pd
import awswrangler as wr
from sample.data_prep.data_extract import query_exp_id, Dataset
from sample.data_prep.data_pre_tests import srm_check, metric_test
from sample.method.other_function import format_sql_query
from sample.ui.tracking_trigger_input import TrackingTriggerInput, show_trigger_input, format_saver_output
from sample.ui.clear_parameter import clear_session_state_parameters, clear_spec_metric_trigger_parameter, clear_spec_metric_main_condition_saver_parameter, clear_spec_metric_main_parameter, clear_exp_config_metric_trigger_parameter
from sample.ui.format_test_result import var_id_map, var_id_map_reverse, count_test_result_user, pretty_test_result, highlight_significant, format_preview_data_result, format_metric_input_as_json, highlight_pass
from sample.ui.general_metric_input import get_general_metric_input

# ========== Basic Config ==========
st.set_page_config(layout="wide")  # 設定頁面為 full width 模式
mode = 'admin'

# ========== Section 1: Background Info / Introduction ==========

st.title('Experiment Dashboard for Pinkoi')  # 標題
st.caption(
    '''
    The dashboard will display relevant information for the selected experiment.
    These information include but not limited to: SRM test, Normality test ....
    '''
)
st.markdown('''
    這個 Dashboard 是為了讓驗收專案的流程準確並更有效率的一套驗收專案工具。
    
    主要分成三個區塊，詳細操作教學請參考 [Handbook]()
    1. Experiment Setting Data: 填入實驗基本設置資訊，填寫/修正後記得點擊「Save Input & Run」更新設定
    2. SRM / AA: 檢查實驗分組比例、隨機性，確認實驗資料可用性
    3. Metric Test: 實驗指標統計差異檢定
        - General Metrics: 驗收 Goal Metrics (buyer journey metrics)
        - Specific Metrics: 驗收 Leading Metrics (包含從 tracking 計算之指標)
    
''')

# ========== Section 2: Experiment Setting Data ==========

st.markdown('### :closed_book: Experiment Setting Data')

with st.container(border=True):
    
    if 'section1_click_confirm' not in st.session_state:
        st.session_state.section1_click_confirm = False

    if 'exp_config_save' not in st.session_state:
        st.session_state.exp_config_save = False

    # 實驗日期
    default_start_date = (pd.Timestamp.now() - pd.Timedelta(days=7)).date()
    
    st.markdown('**實驗時間**')
    col1, col2 = st.columns(2)
    
    with col1:
        # 資料收集時間區間 exp_date_range=(start, end)
        st.date_input(
            label="資料收集時間區間（資料觀察期）",
            value=(
                default_start_date,
                default_start_date + pd.Timedelta(days=6)
            ),
            max_value=(pd.Timestamp.now() - pd.Timedelta(days=1)).date(),
            format="YYYY/MM/DD",
            key='exp_date_range'
        )
        if len(st.session_state.get('exp_date_range', [])) != 2:
            st.markdown('<p style="color:red; font-size: 12px;"> ⚠️ 要輸入開始與結束日ㄛ ⚠️</p>', unsafe_allow_html=True)
        
    with col2:
        # 分組時間 exp_start_date
        st.date_input(
            label="分組開始時間 (Optional)",
            value=None,
            max_value=(pd.Timestamp.now() - pd.Timedelta(days=1)).date(),
            format="YYYY/MM/DD",
            key='exp_start_date'
        )
    
    if st.button('查詢實驗 id', key='exp_id_query_btn'):

        with st.spinner(text="查詢實驗 id ..."):
            try:
                st.session_state.exp_id_list = query_exp_id(
                    start_date=f"{st.session_state.exp_date_range[0]:%Y-%m-%d}",
                    end_date=f"{st.session_state.exp_date_range[1]:%Y-%m-%d}"
                )

                st.session_state.section1_click_confirm = True
            except Exception:
                st.error('查詢實驗 id 失敗，請檢查時間輸入是否正確')
    
    if st.session_state.section1_click_confirm:
        
        col1, col2, col3 = st.columns(3)
        
        with col1:  # exp_tool_version、exp_platform (目前皆未上線)、exp_id
            st.markdown('**實驗系統 & ID**')
            st.selectbox(
                label="舊/新實驗分組機制",
                options=["舊分組機制"],
                index=0,
                key='exp_tool_version'
            )

            # st.selectbox(
            #     label="前台/後台實驗",
            #     options=["前台", "後台"],
            #     index=0,
            #     key='exp_platform'
            # )
        
            st.selectbox(
                label='Exp ID',
                options=sorted(list(st.session_state.exp_id_list)),
                index=0,
                placeholder="Experiment id",
                key='exp_id',
                disabled=False
            )

        with col2:  # device、ios_app_version、android_app_version
            st.markdown('**實驗裝置**')

            st.multiselect(
                label="Device",
                options=["iphone", "android", "mobile", "web"],
                default=["iphone", "android", "mobile", "web"],
                placeholder="Select Device(s)",
                key='device'
            )
            
            if 'iphone' in st.session_state.device:
                st.text_input(
                    label='Iphone Version (Optional)',
                    placeholder="0.0.00",
                    key='ios_app_version'
                )
            if 'android' in st.session_state.device:
                st.text_input(
                    label='Android Version (Optional)',
                    placeholder="0.0.00",
                    key='android_app_version'
                )

        with col3:  # geo、lang
            st.markdown('**實驗 Geo / Lang**')

            st.radio(
                label="Geo",
                options=['all', 'individual'],
                index=0,
                horizontal=True,
                key='geo_option'
            )
            if st.session_state.get('geo_option', '') != 'all':
                st.multiselect(
                    label="Geo",
                    options=["TW", "HKMO", "CN", "JP", "TH"],
                    default=["TW"],
                    key='geo',
                    label_visibility='hidden'
                )
            
            st.radio(
                label="Lang",
                options=['all', 'individual'],
                index=0,
                horizontal=True,
                key='lang_option'
            )
            if st.session_state.get('lang_option', '') != 'all':
                st.multiselect(
                    label="Lang",
                    options=["zh_TW", "zh_HK", "zh_CN", "jp", "en", "th"],
                    default=["zh_TW"],
                    key='lang',
                    label_visibility='hidden'
                )
        
        # Experiment Trigger
        st.markdown('**實驗 Trigger**')
        # 初始化 exp_config_trigger
        st.toggle('Need Trigger', value=False, key='exp_config_need_metric_trigger', on_change=clear_exp_config_metric_trigger_parameter)

        if 'exp_config_trigger' not in st.session_state:
            st.session_state.exp_config_trigger = {}
        if st.session_state.exp_config_need_metric_trigger:  # 當開啟 Need Trigger 時，才顯示 metric_trigger 區塊
            with st.container(border=True):  # exp_config_trigger

                # 初始化 exp_config_trigger_saver 以保存輸入容器
                if 'exp_config_trigger_saver' not in st.session_state:
                    st.session_state.exp_config_trigger_saver = {}
                
                exp_config_trigger_saver_class = TrackingTriggerInput(
                    'exp_config_trigger',
                    st.session_state.exp_config_trigger_saver
                )
                
                st.selectbox(
                    label='Source Table',
                    options=['tracking'],
                    index=0,
                    key='exp_trigger_source'
                )

                if st.session_state.exp_trigger_source == 'tracking':
                    with st.container(border=True):
                        st.markdown('Tracking Condition')
                        show_trigger_input(exp_config_trigger_saver_class)
                        exp_config_trigger_tracking_params = format_saver_output(st.session_state.exp_config_trigger_saver)
                
                # exp_config_trigger 輸入值
                st.session_state.exp_config_trigger = {
                    'source': st.session_state.exp_trigger_source,
                }
                if st.session_state.exp_trigger_source == 'tracking':
                    st.session_state.exp_config_trigger['param'] = exp_config_trigger_tracking_params
        
        # Preview Input
        exp_input = {
            'exp_id': st.session_state.get('exp_id', ''),
            'start': f"{st.session_state.get('exp_date_range', ['', ''])[0]:%Y-%m-%d}",
            'end': f"{st.session_state.get('exp_date_range', ['', ''])[1]:%Y-%m-%d}",
            'exp_start_date': f"{st.session_state.get('exp_start_date'):%Y-%m-%d}" if st.session_state.get('exp_start_date') else '',
            # 'exp_tool_version': st.session_state.get('exp_tool_version', ''),
            # 'exp_platform': st.session_state.get('exp_platform', ''),
            'device': st.session_state.get('device', []),
            'version': {
                'iphone': st.session_state.get('ios_app_version', ''),
                'android': st.session_state.get('android_app_version', ''),
            } if (st.session_state.get('ios_app_version')) or (st.session_state.get('android_app_version')) else 'all',
            'geo': 'all' if st.session_state.get('geo_option') == 'all' else st.session_state.get('geo', []),
            'lang': 'all' if st.session_state.get('lang_option') == 'all' else st.session_state.get('lang', []),
            'trigger': st.session_state.exp_config_trigger.get('param', {})
        }
        
        if mode == 'admin':
            st.markdown('<p style="font-size: 16px; color: orange; font-weight:bold;"> 🌞 Admin Mode - Debug Input 🌞 </p>', unsafe_allow_html=True)
            with st.expander('Debug - Experiment Input', expanded=True):
                st.code(
                    format_metric_input_as_json(exp_input),
                    language="json"
                )
        
        if 'exp_config_preview_sql_btn' not in st.session_state:
            st.session_state.exp_config_preview_sql_btn = False
        if st.button('Preview SQL'):
            st.session_state.exp_config_preview_sql_btn = True
            if 'data_class' not in st.session_state:
                st.session_state.data_class = None
            st.session_state.data_class = Dataset(**exp_input)
        
        if st.button('Save Input & Run', type="primary"):
            with st.spinner("Experiment Setting ..."):
                if 'data_class' not in st.session_state:
                    st.session_state.data_class = None
                st.session_state.data_class = Dataset(**exp_input)
                st.session_state.exp_config_preview_sql_btn = True  # 顯示 Preview SQL
                
                st.info("Save")  # 顯示完成設定
                st.session_state.exp_config_save = True
    
        # Preview Data
        with st.expander('🔎 Preview SQL', expanded=True):
            if st.session_state.exp_config_preview_sql_btn:
                st.code(
                    format_sql_query(f'''
                    {st.session_state.data_class.sql_query_1}
                    {st.session_state.data_class.all_trigger_query}
                    '''),
                    language="sql"
                )

# ========== Section 3: SRM / AA ==========
# 留給 Jerry 填

if st.session_state.exp_config_save:

    st.markdown('### :pencil: SRM / AA')

    with st.expander('SRM / AA', expanded=True):
        
        col1, col2, col3 = st.columns(3)
        with col1:
        
            st.multiselect(
                label='Device',
                options=['all', 'android', 'iphone', 'mobile', 'web'],
                default='all',
                key='srm_device'
            )
            st.selectbox(
                label='Significance level',
                options=[0.01, 0.05, 0.1],
                key='srm_sig_level'
            )
        
            st.markdown('Var ID Ratio Table (如有多組實驗組，請自行新增組別)')
            var_id_ratio_table = st.data_editor(
                data=pd.DataFrame({
                    'var_id': ['A', 'AA', 'B'],
                    'var_id_ratio(%)': [25, 25, 50]
                }),
                column_order=['var_id', 'var_id_ratio(%)'],
                hide_index=True,
                num_rows='dynamic',
                key='var_id_ratio_table'
            )
        
            # 檢查是否填寫完 var_id_ratio_table
            if var_id_ratio_table.isnull().sum().sum() > 0:
                st.warning('Please fulfill your var_id_ratio_table!')
        
        if 'click_srm_aa_result' not in st.session_state:
            st.session_state.click_srm_aa_result = False

        if (st.session_state.get('srm_device') is not None) & (st.session_state.get('srm_sig_level') is not None) & (var_id_ratio_table.isnull().sum().sum() == 0):
            
            if st.button('Run SRM & AA', key='run_srm_btn', type="primary"):
                with st.spinner('Running ...'):
                    
                    # raw_data 可能因釋出記憶體被刪除，所以需重新載入
                    if 'raw_data' not in st.session_state:
                        try:
                            st.session_state.raw_data = st.session_state.data_class.get_raw_data(mode='data')
                            if len(st.session_state.raw_data) == 0:
                                st.warning("No Data Found!")
                        except Exception as e:
                            st.error(f"Error Msg: {e}")

                    for x in ['raw_multi_grp_table', 'var_id_stat', 'var_id_stat_pct', 'metric_result', 'srm_result']:
                        st.session_state[x] = pd.DataFrame()
                    
                    st.session_state.raw_multi_grp_table, st.session_state.var_id_stat, st.session_state.var_id_stat_pct = format_preview_data_result(st.session_state.raw_data)
                    
                    st.session_state.srm_result = srm_check(
                        df=st.session_state.raw_data,
                        check_var_id_list=[var_id_map_reverse[x] for x in var_id_ratio_table['var_id'].tolist()],
                        ratio_list=var_id_ratio_table['var_id_ratio(%)'].tolist(),
                        device=st.session_state.srm_device,
                        significant_level=st.session_state.srm_sig_level
                    )  # all device SRM Check
                    
                    st.session_state.metric_result = metric_test(
                        df=st.session_state.raw_data,
                        check_var_id_list=['0', '1'],
                        device=st.session_state.srm_device,
                        significant_level=st.session_state.srm_sig_level
                    )  # all device AA Test

                    st.session_state.click_srm_aa_result = True
                    
                    # 計算完 SRM/AA 後，刪除 raw_data 釋出記憶體
                    del st.session_state['raw_data']
        
        with col2:
            
            st.markdown('**各分組用戶人數/比例**')
            
            if st.session_state.click_srm_aa_result:
                st.dataframe(st.session_state.raw_multi_grp_table, hide_index=True)
                st.dataframe(st.session_state.var_id_stat, hide_index=True)
                st.dataframe(st.session_state.var_id_stat_pct, hide_index=True)
        
        with col3:

            st.markdown('**SRM Check Result**')
            
            if st.session_state.click_srm_aa_result:
                st.dataframe(
                    pd.DataFrame(st.session_state.srm_result.values(), index=st.session_state.srm_result.keys())\
                        [['p_value', 'is_significant']]\
                        .assign(
                            result=lambda df: df['is_significant'].apply(lambda x: 'Fail' if x else 'Pass'),
                            p_value=lambda df: df['p_value'].apply(lambda x: f'{x:.4f}')
                        )\
                        .drop(columns=['is_significant'])\
                        .style\
                        .map(highlight_pass)
                )

            st.markdown('---')
            st.markdown('**AA Test Result**')
            
            if st.session_state.click_srm_aa_result:
                st.dataframe(
                    pd.DataFrame(st.session_state.metric_result.values(), index=st.session_state.metric_result.keys())\
                        .reset_index()\
                        .assign(
                            device=lambda df: df['index'].apply(lambda x: x.split('_')[0]),
                            metric=lambda df: df['index'].apply(lambda x: x.replace(f'{x.split("_")[0]}_', '')),
                            result=lambda df: df['is_significant'].apply(lambda x: 'Fail' if x else 'Pass'),
                            p_value=lambda df: df['p_value'].apply(lambda x: f'{x:.4f}'),
                            mean_diff=lambda df: df['mean_diff'].apply(lambda x: f'{x:.4f}'),
                        )\
                        [['device', 'metric', 'mean_diff', 'p_value', 'result']]\
                        .style\
                        .map(highlight_pass),
                    hide_index=True
                )

# ========== Section 4: Metric Test ==========

if st.session_state.exp_config_save:

    st.markdown('### :rocket: Metric Test')

    with st.container():
        
        # Metric Test 共用參數
        with st.popover("更改設定"):
            
            col1, col2 = st.columns(2)
            with col1:
                metric_test_alpha_input = st.selectbox(
                    'Alpha',
                    [0.01, 0.05, 0.1],
                    index=1,
                    key='metric_test_alpha_input'
                )
            with col2:
                metric_test_variant_1 = st.selectbox(
                    'Variant 1',
                    options=['A+AA', 'A', 'AA', 'B', 'C', 'D'],
                    index=0,
                    key='metric_test_variant_1'
                )
                metric_test_variant_2 = st.selectbox(
                    'Variant 2',
                    options=['A+AA', 'A', 'AA', 'B', 'C', 'D'],
                    index=3,
                    key='metric_test_variant_2'
                )
            
        st.markdown(
            f'''
            <style>
            .highlight {{
                font-weight: bold;
                color: orange; /* 你可以根據需要調整顏色 */
            }}
            </style>
            Alpha: <span class="highlight">{metric_test_alpha_input}</span>,
            比較組別: <span class="highlight">{metric_test_variant_1} v.s. {metric_test_variant_2}</span>
            ''', unsafe_allow_html=True
        )

        s3_tab1, s3_tab2, s3_tab3 = st.tabs(["General Metric", "Specific Metric", "Customized Query"])
        
        with s3_tab1:  # General Metric，留給 Jerry 填
            
            st.markdown('### Custom Input')
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Metric Type
                st.selectbox(
                    label='Metric Type',
                    options=[
                        'funnel',
                        'session_duration_per_day',
                        'view_item_cnt_per_user',
                        'view_event_cnt_per_user',
                        '{event_name}_cnt_per_user',
                        '{event_name}_adoption',
                        'ad_click_cnt_per_user'
                    ],
                    index=0,
                    key='general_metric_type'
                )
                # Funnel Input
                if st.session_state.general_metric_type == 'funnel':
                    col3, col4 = st.columns(2)
                    col3.selectbox(
                        label='Start Funnel',
                        options=[
                            'visit', 'funnel_e1', 'funnel_e2', 'funnel_interest', 'funnel_consideration', 'add_to_cart'
                        ],
                        index=0,
                        key='general_metric_start_funnel'
                    )
                    col4.multiselect(
                        label='End Funnel',
                        options=[
                            'funnel_e1', 'funnel_e2', 'funnel_interest', 'funnel_consideration', 'add_to_cart', 'funnel_purchase'
                        ],
                        default='funnel_e1',
                        key='general_metric_end_funnel'
                    )
                # Event Name Input
                if st.session_state.general_metric_type in ['{event_name}_cnt_per_user', '{event_name}_adoption']:
                    st.text_input(
                        label='Event Name',
                        placeholder='view_item, view_feed, add_to_cart, ...',
                        key='general_metric_event_name',
                    )
                # Ad Type Input
                if st.session_state.general_metric_type == 'ad_click_cnt_per_user':
                    st.selectbox(
                        label='Start Funnel',
                        options=[
                            'pl', 'pb'
                        ],
                        index=0,
                        key='general_metric_ad_type'
                    )

                general_metric_name, general_metric_input = get_general_metric_input(
                    metric_type=st.session_state.general_metric_type,
                    start_funnel=st.session_state.get('general_metric_start_funnel', ''),
                    end_funnel=st.session_state.get('general_metric_end_funnel', []),
                    event_name=st.session_state.get('general_metric_event_name', ''),
                    ad_type=st.session_state.get('general_metric_ad_type', '')
                )
                
            with col2:
                # Debug Metric Input
                if mode == 'admin':
                    st.markdown('<p style="font-size: 16px; color: orange; font-weight:bold;"> 🌞 Admin Mode - Metric Input 🌞 </p>', unsafe_allow_html=True)
                    with st.expander('Debug - Metric Input', expanded=True):
                        st.code(
                            format_metric_input_as_json(general_metric_input),
                            language="json"
                        )

            # submit button
            with st.container():

                # Initialize Output Result
                if 'general_metric_sql_query' not in st.session_state:
                    st.session_state.general_metric_sql_query = ''
                if 'general_metric_format_result' not in st.session_state:
                    st.session_state.general_metric_format_result = pd.DataFrame()
                
                if st.button("Preview Metric Test SQL", key='preview_general_metric_test_sql'):
                    if st.session_state.get('data_class'):
                        st.session_state.general_metric_sql_query = st.session_state.data_class.get_metric_data(mode='sql', metric=general_metric_input)
                
                col1, col2 = st.columns(2)
                if col1.button("Run Metric Test", type="primary", key='general_metric_run_btn'):

                    if st.session_state.get('data_class'):
                        
                        if 'general_metric_query_result' not in st.session_state:
                            st.session_state.general_metric_query_result = pd.DataFrame()

                        # Run Metric Test
                        def general_metric_run_test():
                            
                            general_metric_query_result = st.session_state.data_class.get_metric_data(mode='data', metric=general_metric_input)
                            st.session_state.general_metric_query_result = general_metric_query_result.head()
                            
                            var_id_map_reverse = {v: k for k, v in var_id_map.items()}
                            check_var_id_list = [
                                var_id_map_reverse.get('A' if st.session_state.get('metric_test_variant_1') == 'A+AA' else st.session_state.get('metric_test_variant_1')),
                                var_id_map_reverse.get('A' if st.session_state.get('metric_test_variant_2') == 'A+AA' else st.session_state.get('metric_test_variant_2')),
                            ]
                            # user cnt
                            try:
                                general_metric_user_cnt_result = count_test_result_user(
                                    df=general_metric_query_result,
                                    check_var_id_list=check_var_id_list,
                                    merge_aa=True if ((st.session_state.get('metric_test_variant_1') == 'A+AA') or (st.session_state.get('metric_test_variant_2') == 'A+AA')) else False
                                )
                            except Exception as e:
                                raise Exception(f'User Count Error: {e}')
                            
                            # test result
                            try:
                                general_metric_test_result = metric_test(
                                    df=general_metric_query_result,
                                    check_var_id_list=check_var_id_list,
                                    device='all',
                                    metrics_list=[general_metric_name],
                                    significant_level=st.session_state.get('metric_test_alpha_input'),
                                    merge_aa=True if ((st.session_state.get('metric_test_variant_1') == 'A+AA') or (st.session_state.get('metric_test_variant_2') == 'A+AA')) else False
                                )
                            except Exception as e:
                                raise Exception(f'Metric Test Error: {e}')
                            # format result
                            try:
                                general_metric_format_result = pretty_test_result(
                                    test_result=general_metric_test_result,
                                    test_variant=[
                                        st.session_state.get('metric_test_variant_1'),
                                        st.session_state.get('metric_test_variant_2')
                                    ],
                                    user_cnt_result=general_metric_user_cnt_result
                                )
                            except Exception as e:
                                raise Exception(f'Format Test Result Error: {e}')
                            
                            return general_metric_format_result
                        
                        if 'general_metric_format_result' not in st.session_state:
                            st.session_state.general_metric_format_result = pd.DataFrame()
                            
                        with st.spinner('Running ...'):
                            try:
                                st.session_state.general_metric_format_result = general_metric_run_test()
                            except Exception as e:
                                st.error(f'{e}')
                    else:
                        st.error('Please input experiment setting data first')

            with st.container():
                st.markdown('### Test Result')
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown('#### Metric SQL Query')
                    st.code(
                        st.session_state.general_metric_sql_query,
                        language="sql",
                    )
                with col2:
                    
                    st.markdown('#### Metric SQL Result')
                    st.dataframe(
                        data=st.session_state.get('general_metric_query_result', pd.DataFrame()),
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    st.markdown('#### Metric Test Result')
                    if not st.session_state.get('general_metric_format_result', pd.DataFrame()).empty:
                        st.dataframe(
                            data=st.session_state.general_metric_format_result.style.map(highlight_significant),
                            use_container_width=True,
                            hide_index=True
                        )
            
        with s3_tab2:  # Specific Metric，留給 Amy 填

            with st.container(border=True):
                
                st.markdown('### Custom Input')
            
                # metric_name
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.text_input(label='自訂指標名稱', placeholder='e.g. spec_metric', key='spec_metric_name', help='名稱結尾為e1,e2,interest,consideration,add_to_cart,purchase,rate,% 結果會自動轉為比例形式')
                with col2:
                    st.toggle('Need Trigger', value=False, key='spec_need_metric_trigger', on_change=clear_spec_metric_trigger_parameter)

                # 初始化 spec_metric_trigger
                if 'spec_metric_trigger' not in st.session_state:
                    st.session_state.spec_metric_trigger = {}
                if st.session_state.spec_need_metric_trigger:  # 當開啟 Need Trigger 時，才顯示 metric_trigger 區塊
                    with st.container(border=True):  # metric_trigger

                        # 初始化 spec_metric_trigger_saver 以保存輸入容器
                        if 'spec_metric_trigger_saver' not in st.session_state:
                            st.session_state.spec_metric_trigger_saver = {}
                        
                        spec_metric_trigger_saver_class = TrackingTriggerInput(
                            'spec_metric_trigger',
                            st.session_state.spec_metric_trigger_saver
                        )
                        
                        st.markdown('metric_trigger')
                        
                        st.selectbox(
                            label='Source Table',
                            options=['tracking', 'journey'],
                            index=0,
                            key='spec_trigger_source'
                        )
                        
                        if st.session_state.spec_trigger_source == 'journey':
                            with st.container(border=True):
                                st.markdown('Journey Condition')
                                st.multiselect(
                                    label='Journey Metric',
                                    options=['funnel_e1', 'funnel_e2', 'funnel_interest',
                                            'funnel_consideration', 'add_to_cart', 'funnel_purchase'],
                                    default=['funnel_e1'],
                                    key='spec_metric_trigger_journey_metric'
                                )

                        if st.session_state.spec_trigger_source == 'tracking':
                            with st.container(border=True):
                                st.markdown('Tracking Condition')
                                show_trigger_input(spec_metric_trigger_saver_class)
                                spec_metric_trigger_tracking_params = format_saver_output(st.session_state.spec_metric_trigger_saver)
                        
                        # metric_trigger 輸入值
                        st.session_state.spec_metric_trigger = {
                            'source': st.session_state.spec_trigger_source,
                        }
                        if st.session_state.spec_trigger_source == 'journey':
                            st.session_state.spec_metric_trigger['param'] = {
                                x: None for x in st.session_state.spec_metric_trigger_journey_metric
                            }
                        if st.session_state.spec_trigger_source == 'tracking':
                            st.session_state.spec_metric_trigger['param'] = spec_metric_trigger_tracking_params
                
                # 初始化 spec_metric_main
                if 'spec_metric_main' not in st.session_state:
                    st.session_state.spec_metric_main = {}
                with st.container(border=True):  # metric_main
                    
                    st.markdown('metric_main')
                    
                    # source
                    col1, col2 = st.columns(2)
                    with col1:
                        st.selectbox(
                            label='Source Table',
                            options=['tracking', 'journey'],
                            index=0,
                            key='spec_main_source'
                        )
                    with col2:
                        if st.session_state.spec_main_source == 'tracking':
                            st.toggle('Need Condition', value=False, key='spec_need_metric_condition', on_change=clear_spec_metric_main_condition_saver_parameter)

                    # condition
                    # 初始化 spec_metric_main_tracking_params
                    if 'spec_metric_main_tracking_params' not in st.session_state:
                        st.session_state.spec_metric_main_tracking_params = {}
                    if st.session_state.get('spec_need_metric_condition', False):  # 當開啟 Need Condition 時，才顯示 tracking_condition 區塊
                        # 初始化 session state 以保存輸入容器
                        if 'spec_metric_main_condition_saver' not in st.session_state:
                            st.session_state.spec_metric_main_condition_saver = {}
                        
                        spec_metric_main_condition_saver_class = TrackingTriggerInput(
                            'spec_metric_main_condition_saver',
                            st.session_state.spec_metric_main_condition_saver
                        )
                        
                        with st.container(border=True):
                            st.markdown('Tracking Condition')
                            show_trigger_input(spec_metric_main_condition_saver_class)
                            st.session_state.spec_metric_main_tracking_params = format_saver_output(st.session_state.spec_metric_main_condition_saver)
                    
                    col1, col2, col3, col4 = st.columns(4)
                    # 一次處理 3 個 input（method, metric, group_level）
                    if st.session_state.spec_main_source == 'journey':
                        # journey 表只支援特定 metric
                        with col1:
                            st.multiselect(
                                label='Journey Metric',
                                options=['funnel_e1', 'funnel_e2', 'funnel_interest',
                                         'funnel_consideration', 'add_to_cart', 'funnel_purchase', 'session_duration'],
                                key='spec_main_metric_journey_key',
                                default=['funnel_e1']
                            )
                            if ('session_duration' in st.session_state.spec_main_metric_journey_key) & (len(st.session_state.spec_main_metric_journey_key) > 1):
                                st.warning('如要計算 session_duration，請移除其他 funnel 選項')
                                
                            # funnel -> boolean, session_duration -> sum
                            if st.session_state.spec_main_metric_journey_key[0] == 'session_duration':
                                if 'spec_main_method' not in st.session_state:
                                    st.session_state['spec_main_method'] = 'sum'
                                st.session_state.spec_main_method = 'sum'
                            else:
                                if 'spec_main_method' not in st.session_state:
                                    st.session_state['spec_main_method'] = 'boolean'
                                st.session_state.spec_main_method = 'boolean'
                        
                        if st.session_state.spec_main_metric_journey_key[0] == 'session_duration':
                            with col2:
                                st.selectbox(
                                    label='Group Level',
                                    options=['user', 'day'],
                                    key='spec_main_group',
                                    index=0
                                )
                            with col3:
                                st.text_input(
                                    label='Operator',
                                    placeholder='>, <, >=, <=',
                                    key='spec_main_journey_operator'
                                )
                            with col4:
                                st.text_input(
                                    label='Seconds',
                                    value=None,
                                    key='spec_main_journey_value'
                                )
                        else:
                            if 'spec_main_group' not in st.session_state:
                                st.session_state['spec_main_group'] = 'user'
                            st.session_state.spec_main_group = 'user'
                    
                    if st.session_state.spec_main_source == 'tracking':
                        with col1:
                            st.selectbox(
                                label='Group Level',
                                options=['user', 'day', 'event'],
                                key='spec_main_group',
                                index=0
                            )
                        with col2:
                            st.selectbox(
                                label='Aggregate Function',
                                options=['count', 'count_distinct', 'boolean', 'sum', 'avg', 'max', 'min'],
                                key='spec_main_method',
                                index=0
                            )
                        with col3:
                            st.text_input(label='Event Name', placeholder='e.g. view_item', key='spec_main_metric_key')
                        with col4:
                            st.text_input(label='Event Parameter', placeholder='e.g. view_id', key='spec_main_metric_value')

                    st.session_state.spec_metric_main = {
                        'metric_name': st.session_state.get('spec_metric_name') if st.session_state.get('spec_metric_name') != '' else 'spec_metric',
                        'source': st.session_state.get('spec_main_source', ''),
                        'group_level': st.session_state.get('spec_main_group', ''),
                        'method': st.session_state.get('spec_main_method', '')
                    }
                    if st.session_state.get('spec_main_metric_journey_key'):
                        # Session Duration 需要特別處理
                        if st.session_state.spec_main_metric_journey_key[0] == 'session_duration':
                            if (st.session_state.get('spec_main_journey_operator', '') != '') & (st.session_state.get('spec_main_journey_value') is not None):
                                st.session_state.spec_metric_main['metric'] = {
                                    st.session_state.spec_main_metric_journey_key[0]: {
                                        'cal': st.session_state.spec_main_journey_operator,
                                        'value': st.session_state.spec_main_journey_value
                                    }
                                }
                                st.session_state.spec_metric_main['method'] = 'boolean'
                            else:
                                st.session_state.spec_metric_main['metric'] = {
                                    st.session_state.spec_main_metric_journey_key[0]: None
                                }
                        # 其他 Journey Metric
                        else:
                            st.session_state.spec_metric_main['metric'] = {
                                x: None for x in st.session_state.spec_main_metric_journey_key
                            }
                        
                    if st.session_state.get('spec_main_metric_key'):
                        st.session_state.spec_metric_main['metric'] = {
                            st.session_state.spec_main_metric_key: None if st.session_state.get('spec_main_metric_value') == '' else st.session_state.spec_main_metric_value
                        }
                    if st.session_state.spec_main_source == 'tracking':
                        st.session_state.spec_metric_main['condition'] = st.session_state.spec_metric_main_tracking_params

                # metric input
                spec_metric_input = {
                    'metric_trigger': st.session_state.get('spec_metric_trigger', {}),
                    'metric_main': st.session_state.get('spec_metric_main', {})
                }
            
            # Debug Metric Input
            if mode == 'admin':
                st.markdown('<p style="font-size: 16px; color: orange; font-weight:bold;"> 🌞 Admin Mode - Metric Input 🌞 </p>', unsafe_allow_html=True)
                with st.expander('Debug - Metric Input', expanded=True):
                    st.code(
                        format_metric_input_as_json(spec_metric_input),
                        language="json"
                    )

            # submit、clear button
            with st.container():

                # Initialize Output Result
                if 'spec_metric_sql_query' not in st.session_state:
                    st.session_state.spec_metric_sql_query = ''
                if 'spec_metric_format_result' not in st.session_state:
                    st.session_state.spec_metric_format_result = pd.DataFrame()
                
                if st.button("Preview Metric Test SQL", key='preview_spec_metric_test_sql'):
                    if st.session_state.get('data_class'):
                        st.session_state.spec_metric_sql_query = st.session_state.data_class.get_metric_data(mode='sql', metric=spec_metric_input)
                        
                col1, col2 = st.columns(2)
                if col1.button("Run Metric Test", type="primary", key='spec_metric_run_btn'):
                    
                    if 'spec_metric_query_result' not in st.session_state:
                        st.session_state.spec_metric_query_result = pd.DataFrame()

                    if st.session_state.get('data_class'):

                        # Run Metric Test
                        def spec_metric_run_test():
                            
                            spec_metric_query_result = st.session_state.data_class.get_metric_data(mode='data', metric=spec_metric_input)
                            st.session_state.spec_metric_query_result = spec_metric_query_result.head()
                            
                            var_id_map_reverse = {v: k for k, v in var_id_map.items()}
                            check_var_id_list = [
                                var_id_map_reverse.get('A' if st.session_state.get('metric_test_variant_1') == 'A+AA' else st.session_state.get('metric_test_variant_1')),
                                var_id_map_reverse.get('A' if st.session_state.get('metric_test_variant_2') == 'A+AA' else st.session_state.get('metric_test_variant_2')),
                            ]
                            # user cnt
                            try:
                                spec_metric_user_cnt_result = count_test_result_user(
                                    df=spec_metric_query_result,
                                    check_var_id_list=check_var_id_list,
                                    merge_aa=True if ((st.session_state.get('metric_test_variant_1') == 'A+AA') or (st.session_state.get('metric_test_variant_2') == 'A+AA')) else False
                                )
                            except Exception as e:
                                raise Exception(f'User Count Error: {e}')
                            
                            # test result
                            try:
                                spec_metric_test_result = metric_test(
                                    df=spec_metric_query_result,
                                    check_var_id_list=check_var_id_list,
                                    device='all',
                                    metrics_list=[st.session_state.get('spec_metric_name').lower() if st.session_state.get('spec_metric_name') != '' else 'spec_metric'],
                                    significant_level=st.session_state.get('metric_test_alpha_input'),
                                    merge_aa=True if ((st.session_state.get('metric_test_variant_1') == 'A+AA') or (st.session_state.get('metric_test_variant_2') == 'A+AA')) else False
                                )
                            except Exception as e:
                                raise Exception(f'Metric Test Error: {e}')
                            
                            # format result
                            try:
                                spec_metric_format_result = pretty_test_result(
                                    test_result=spec_metric_test_result,
                                    test_variant=[
                                        st.session_state.get('metric_test_variant_1'),
                                        st.session_state.get('metric_test_variant_2')
                                    ],
                                    user_cnt_result=spec_metric_user_cnt_result
                                )
                            except Exception as e:
                                raise Exception(f'Format Test Result Error: {e}')
                            
                            return spec_metric_format_result
                        
                        if 'spec_metric_format_result' not in st.session_state:
                            st.session_state.spec_metric_format_result = pd.DataFrame()
                            
                        with st.spinner('Running ...'):
                            try:
                                st.session_state.spec_metric_format_result = spec_metric_run_test()
                            except Exception as e:
                                st.error(f'{e}')
                    else:
                        st.error('Please input experiment setting data first')
                col2.button("Clear", on_click=clear_spec_metric_main_parameter, key='spec_metric_clear_btn')

            with st.container():
                st.markdown('### Test Result')
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown('#### Metric SQL Query')
                    st.code(
                        st.session_state.spec_metric_sql_query,
                        language="sql",
                    )
                with col2:
                    
                    st.markdown('#### Metric SQL Result')
                    st.dataframe(
                        data=st.session_state.get('spec_metric_query_result', pd.DataFrame()),
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    st.markdown('#### Metric Test Result')
                    st.markdown('<p style="color:grey; font-size: 12px;"> metric_name 結尾為 e1,e2,interest,consideration,add_to_cart,purchase,rate,% 結果會自動轉為比例形式</p>', unsafe_allow_html=True)
                    if not st.session_state.get('spec_metric_format_result', pd.DataFrame()).empty:
                        st.dataframe(
                            data=st.session_state.spec_metric_format_result.style.map(highlight_significant),
                            use_container_width=True,
                            hide_index=True
                        )
            
        with s3_tab3:  # Customized Query，留給未來的某位
            st.markdown(
                '''
                * TBC *
                '''
            )

