import awswrangler as wr
import pandas as pd
from typing import Any, Dict, List, Optional, Union, Tuple
from sample.method.other_function import list_to_str, event_map_to_query, metric_map_to_query, format_sql_query


def query_exp_id(start_date, end_date):
    
    sql_query = f'''
    SELECT
        DISTINCT exp_id
    FROM ath_experiment
    WHERE date(created) >= date'{start_date}'
    AND date(created) <= date'{end_date}'
    '''
    exp_id_df = wr.athena.read_sql_query(sql=sql_query, database="default")

    return exp_id_df['exp_id'].tolist()

 
class Dataset:
    """
    A class used to represent a Dataset for experiment analysis.

    Attributes
    ----------
    exp_id : str
        The experiment identifier.
    start : str
        The start date of the dataset (format: 'yyyy-mm-dd').
    end : str
        The end date of the dataset (format: 'yyyy-mm-dd').
    exp_start_date : Optional[str]
        An optional flexible experiment start date (format: 'yyyy-mm-dd').
    version : Union[str, Dict[str, str]]
        The app version(s) to include in the analysis, defaults to 'all'.
    device : Union[str, List[str]]
        The device(s) to include in the analysis, defaults to 'all'.
    geo : Union[str, List[str]]
        The geographical region(s) to include in the analysis, defaults to 'all'.
    lang : Union[str, List[str]]
        The language(s) to include in the analysis, defaults to 'all'.
    trigger : Dict[str, Any]
        The event triggers to include in the analysis.
    """

    def __init__(
        self,
        exp_id: str,
        start: str,
        end: str,
        exp_start_date: Optional[str] = None,
        exp_type: str = 'old',
        device: Union[str, List[str]] = 'all',
        version: Union[str, Dict[str, str]] = 'all',
        geo: Union[str, List[str]] = 'all',
        lang: Union[str, List[str]] = 'all',
        trigger: Dict[str, Any] = {}
    ) -> None:
        self.exp_id = exp_id
        self.start = start
        self.end = end
        self.version = version
        self.device = device
        self.exp_type = exp_type
        self.geo = geo
        self.lang = lang
        self.exp_start_date = exp_start_date
        self.trigger = trigger

        self.__extract_prepare()

    def __construct_device_version_query(self) -> Tuple[str]:
        '''
        ## Description
        組出 experiment 參數中與 device & app version 有關 sql query
        
        Input: device, version
        Output: device_query, version_cte, version_join
        
        ## Ouput Sample
        
        - device_query

            (buyer_journey.device in ('iphone', 'android') AND buyer_journey.visit_date >= min_update_date)
            OR
            (buyer_journey.device in ('mobile', 'web'))
        
        - version_cte

            beacon_ver as (
                select ...
                from ath_bi_beacon_with_device_info
                where device = 'iphone' and cast(app_version ....)
                ...
            )

        - version_join

            LEFT JOIN beacon_ver b
                ON buyer_journey.beacon = b.beacon
                AND buyer_journey.device = b.device
        
        '''

        device_query, version_cte, version_join = '', '', ''
        device_query_list = []
        device_version_list = []

        # check version input
        if self.version != 'all':
            if not list(set(['iphone', 'android']) & set(self.device)):
                raise ValueError('device without app shoundnt input version')
            
            # version 可以設成 {iphone:xx , android:xx} 或是單一值
            if isinstance(self.version, str):
                ios_version = self.version
                android_version = self.version
            else:
                ios_version = self.version.get('iphone')
                android_version = self.version.get('android')

            device_version_query_list = []
            if ('iphone' in self.device) & (not pd.isnull(ios_version)):
                device_version_list.append('iphone')
                device_version_query_list.append(f'''
                (
                    device = 'iphone'
                    AND (
                        CAST(
                            SPLIT(app_version, '.') [1] as INT
                        ) * 10000 + CAST(
                            SPLIT(app_version, '.') [2] as INT
                        ) * 100 + CAST(
                            SPLIT(app_version, '.') [3] as INT
                        )
                    ) >= (
                        CAST(
                            SPLIT('{ios_version}', '.') [1] as INT
                        ) * 10000 + CAST(
                            SPLIT('{ios_version}', '.') [2] as INT
                        ) * 100 + CAST(
                            SPLIT('{ios_version}', '.') [3] as INT
                        )
                    )
                )
                ''')
            if ('android' in self.device) & (not pd.isnull(android_version)):
                device_version_list.append('android')
                device_version_query_list.append(f'''
                (
                    device = 'android'
                    and (
                        CAST(
                            SPLIT(app_version, '.') [1] as INT
                        ) * 10000 + CAST(
                            SPLIT(app_version, '.') [2] as INT
                        ) * 100 + CAST(
                            SPLIT(app_version, '.') [3] as INT
                        )
                    ) >= (
                        CAST(
                            SPLIT('{android_version}', '.') [1] as INT
                        ) * 10000 + CAST(
                            SPLIT('{android_version}', '.') [2] as INT
                        ) * 100 + CAST(
                            SPLIT('{android_version}', '.') [3] as INT
                        )
                    )
                )
                ''')
            device_version_query = f'AND ({"OR".join(device_version_query_list)})'

            version_cte = f'''
            beacon_ver as (
                -- 改成 beacon 更新的最小日期
                SELECT
                    beacon,
                    device,
                    min(visit_date) min_update_date
                FROM
                    ath_bi_beacon_with_device_info
                WHERE
                    visit_date BETWEEN date '{self.start}'
                    AND date '{self.end}'
                    {device_version_query}
                GROUP BY
                    beacon,
                    device
            ),
            '''

            version_join = '''
            LEFT JOIN beacon_ver b
                ON buyer_journey.beacon = b.beacon
                AND buyer_journey.device = b.device
            '''

            device_query_list.append(f'''
            (
                buyer_journey.device in ({list_to_str(device_version_list)})
                AND buyer_journey.visit_date >= min_update_date
            )
            ''')
            
        device_ignore_list = list(set(self.device) - set(device_version_list))
        if device_ignore_list:
            device_query_list.append(f'''
            (
                buyer_journey.device in ({list_to_str(device_ignore_list)})
            )
            ''')
        device_query = f'({"OR".join(device_query_list)})'

        return device_query, version_cte, version_join

    def __construct_trigger_query(self) -> Tuple[str]:
        '''
        ## Description
        組出 experiment 參數中 lang, trigger 有關 sql query
        
        Input: lang, trigger
        Output: lang_query, all_trigger_query, all_trigger_join_query
        
        ## Output Sample
        
        - lang_query

            AND user_properties['lang'] in ('zh_TW')
        
        - all_trigger_query
        
            trigger AS (
                select beacon, device, date
                from ath_st_client_side_tracking_without_dirt
                where event_name = 'view_page'
                ....
            )
        
        - all_trigger_join_query

            INNER JOIN trigger
                ON expt_journey.device = trigger.device
                AND expt_journey.beacon = trigger.beacon
                AND expt_journey.visit_date >= trigger.date_
        
        '''
        
        # 不考慮 lang & trigger 情境
        lang_query, all_trigger_query, all_trigger_join_query = '', '', ''

        if (self.lang == 'all') & (len(self.trigger) == 0):
            return lang_query, all_trigger_query, all_trigger_join_query

        # lang
        if self.lang != 'all':
            if isinstance(self.lang, str):
                self.lang = [self.lang]
            lang_query = f"AND user_properties['lang'] in ({list_to_str(self.lang)})"

        if len(self.trigger) > 0:
            # 用 function 處理 input
            trigger_query = 'AND ' + event_map_to_query(self.trigger)
        else:
            # 如果沒有 trigger 但有 lang 就取所有 view_event 的 lang 做 filter
            trigger_query = "AND event_name LIKE 'view_%'"

        all_trigger_query = f"""
        trigger as (
            SELECT
                t.beacon,
                t.device,
                min(date(event_timestamp)) date_
            FROM
                ath_st_client_side_tracking_without_dirt t
            INNER JOIN expt_journey
                ON expt_journey.beacon = t.beacon
                AND expt_journey.device = t.device
                AND expt_journey.visit_date = date(t.event_timestamp)
            WHERE date(event_date) BETWEEN date '{self.start}'
                AND DATE_ADD('day', 1, date '{self.end}') -- event_date 要多取一天
                AND date(event_timestamp) BETWEEN date '{self.start}'
                AND date '{self.end}'
                AND t.device in ({list_to_str(self.device)})
                {lang_query}
                {trigger_query}
            GROUP BY
                t.beacon,
                t.device
        ),
        """

        # trigger 的 join 條件
        all_trigger_join_query = """
        INNER JOIN trigger
            ON expt_journey.device = trigger.device
            AND expt_journey.beacon = trigger.beacon
            AND expt_journey.visit_date >= trigger.date_
        """

        return lang_query, all_trigger_query, all_trigger_join_query

    def __extract_prepare(self):
        '''
        ## Description
        輸入完所有實驗參數後，組出與實驗參數相關的 sql query
        
        Output: sql_query_1, all_trigger_query
        
        ## Output Sample
        
        - sql_query_1
        
        With exp AS (
            參與實驗 beacon 資料
        ), buyer_journey AS (
            實驗區間內 buyer journey 資料
        ), beacon_ver AS (
            實驗 beacon app 版本資料
        ), expt_journey AS (
            實驗用戶 buyer journey 資料
        ), ....
        
        - all_trigger_query
        
        trigger AS (
            實驗指定觸發條件資料 e.g. 有到 e1
        ), ...
        
        '''

        all_device = ['iphone', 'android', 'web', 'mobile']
        if self.device == 'all':
            self.device = all_device
        elif isinstance(self.device, str) & (self.device in (all_device)):
            self.device = [self.device]
        elif isinstance(self.device, list) & (set(self.device).issubset(all_device)):
            pass
        else:
            raise ValueError('please check device input again')
            
        # device、version
        device_query, version_cte, version_join = self.__construct_device_version_query()

        # geo
        geo_query = ''
        if self.geo != 'all':
            if isinstance(self.geo, str):
                self.geo = [self.geo]
            geo_query = f"AND geo in ({list_to_str(self.geo)})"
            
        self.exp_date_query, expt_query, journey_raw_query = '', '', ''
        assert self.exp_type in ['old' , 'new'], 'check exp_type input'

        journey_raw_query = f'''
                buyer_journey AS (
                    SELECT
                        visit_date,
                        first_session_timestamp,
                        date(
                            at_timezone(
                                first_session_timestamp,
                                'Asia/Taipei'
                            )
                        ) AS session_date,
                        beacon_uid,
                        b.beacon AS beacon,
                        b.device AS device,
                        b.session_duration_sec session_duration,
                        geo,
                        max_member_status,
                        is_seller,
                        first_session_timestamp,
                        funnel_e1,
                        funnel_e2,
                        funnel_interest,
                        funnel_consideration,
                        add_to_cart,
                        funnel_purchase,
                        e2__uniq_items,
                        if(
                            funnel ['view_item'] IS NULL,
                            0,
                            funnel ['view_item'].event_cnt
                        ) view_item_cnt,
                        e2__session_duration,
                        interest__shop_list,
                        interest__item_list,
                        search__term_list
                    FROM
                        ath_buyer_journey
                        CROSS JOIN unnest(beacon) AS t(b)
                    WHERE
                        visit_date >= date '{self.start}'
                        AND visit_date <= date '{self.end}'
                ),
        '''
        if self.exp_type == 'old':
            # exp_start_date
            if self.exp_start_date:
                self.exp_date_query = f'''
                AND date(created) >= date '{self.exp_start_date}'
                '''

            expt_query = f'''
                WITH expt AS (
                    SELECT
                        beacon,
                        exp_id,
                        var_id,
                        date(created) AS created_date
                    FROM
                        ath_experiment
                    WHERE
                        exp_id = '{self.exp_id}'
                        AND date(created) <= date '{self.end}'
                        {self.exp_date_query}
                ),
            '''
            self.sql_query_1 = f'''
                {expt_query}
                {journey_raw_query}
                {version_cte}
                expt_journey AS (
                    SELECT
                        exp_id,
                        var_id,
                        buyer_journey.visit_date,
                        buyer_journey.beacon_uid,
                        buyer_journey.beacon,
                        buyer_journey.device,
                        buyer_journey.view_item_cnt,
                        buyer_journey.session_duration,
                        buyer_journey.funnel_e1,
                        buyer_journey.funnel_e2,
                        buyer_journey.funnel_interest,
                        buyer_journey.funnel_consideration,
                        buyer_journey.add_to_cart,
                        buyer_journey.funnel_purchase
                    FROM
                        buyer_journey
                    INNER JOIN expt
                        ON buyer_journey.beacon = expt.beacon
                        AND buyer_journey.visit_date >= expt.created_date --visit date 大於被分組的時間 created date
                    {version_join}
                    WHERE
                        {device_query}
                        {geo_query}
                        -- uid 中同個 device 可能會有多個 beacon 的數值， cross join 後需要先 group
                ),
            '''

        else:
            if self.exp_start_date:
                raise KeyError('New exp tool NOT allow to set additional exp start time')
            expt_query = f'''
                WITH exp_config AS (
                SELECT
                    mod_num , var_id , split_id 
                FROM 
                    ath_ig_ab_testing_config 
                WHERE 
                    exp_name = '{self.exp_id}'
                ),
                
                exp AS (
                    SELECT
                        beacon,
                        device,
                        max(is_seller) is_seller , max(is_admin) is_admin, count(distinct uid) multi_uid,
                        filter(array_agg(distinct first_beacon), x->x is not null) first_beacon_array,
                        count(distinct first_beacon) as first_beacon_cnt,
                        max(first_beacon is null) has_null_first_beacon
                        date(created) AS created_date
                    FROM
                        ath_prj_data_user_beacon
                    WHERE
            '''


        # 處理 tracking 相關的 query
        # lang、trigger
        self.lang_query, self.all_trigger_query, self.all_trigger_join_query = self.__construct_trigger_query()

    def get_exp_data(self, mode: str = 'sql') -> Union[str, pd.DataFrame]:
        '''
        ## Description
        查詢實驗 beacon_uid 分組、分組時間
        
        ## Input
        mode: sql/data
        - sql: 輸出 sql query (default)
        - data: 輸出 data table
        
        ## Output Table Columns
        - beacon_uid
        - beacon
        - uid
        - exp_id
        - var_id
        - created_date
        
        '''

        assert mode in ['sql', 'data'], 'check mode input again'


        sql_query = f'''
        SELECT
            coalesce(NULLIF(uid, ''), beacon) AS beacon_uid,
            exp_id,
            var_id,
            date(created) AS created_date
        FROM
            ath_experiment
        WHERE
            exp_id = '{self.exp_id}'
            AND date(created) <= date '{self.end}'
            {self.exp_date_query}
        '''

        if mode == 'sql':
            return sql_query

        if mode == 'data':
            data = wr.athena.read_sql_query(sql=sql_query, database="default", categories = ['exp_id'])
            data = data.fillna(0)  # avg_view_item 會有 na值, 用 0 補
            return data

        return None

    def get_raw_data(self, mode: str = 'sql') -> Union[str, pd.DataFrame]:
        '''
        ## Description
        查詢實驗 beacon_uid 分組、分組時間 + Device AA Test 指標(view_item_cnt, session_duration, e2)
        
        ## Input
        mode: sql/data
        - sql: 輸出 sql query (default)
        - data: 輸出 data table
        
        ## Output Table Columns
        - beacon_uid
        - beacon
        - uid
        - exp_id
        - var_id
        - created_date
        - device
        - device_avg_view_item
        - device_avg_duration
        - device_e2
        - all_avg_view_item
        - all_avg_duration
        - all_e2
        
        '''

        assert mode in ['sql', 'data'], 'check mode input again'

        sql_query = f'''
        {self.sql_query_1}
        {self.all_trigger_query}
        join_df as (
            SELECT
                exp_id,
                var_id,
                visit_date,
                beacon_uid,
                expt_journey.device,
                avg(view_item_cnt) view_item_cnt, --view_item_cnt per day
                sum(session_duration) session_duration, --avg session duration per day
                max(funnel_e1) e1,
                max(funnel_e2) e2,
                max(funnel_interest) interest,
                max(funnel_consideration) consider,
                max(funnel_purchase) purchase
            FROM
                expt_journey
            {self.all_trigger_join_query}
            -- uid 中同個 device 可能會有多個 beacon 的數值， cross join 後需要先 group
            GROUP BY
                exp_id,
                var_id,
                visit_date,
                beacon_uid,
                expt_journey.device
        ),
        -- 將 device, beacon_uid 統整成一個 group 主要算出不同 device 平均一天的 session_duration, view_item_cnt
        device_metric_avg AS (
            SELECT
                exp_id,
                var_id,
                beacon_uid,
                device,
                -- 不同 device 區間內 view_item 數 per user
                sum(view_item_cnt) device_avg_view_item,
                -- 不同 device 平均一日的 session_duration per user
                avg(session_duration) device_avg_duration,
                max(e2) device_e2
            FROM
                join_df
            GROUP BY
                exp_id,
                var_id,
                beacon_uid,
                device
        ),
        -- 計算該 beacon_uid 每一天 view_item 的數量以及 session_duration (包含不同 device)
        all_metric_per_day AS (
            SELECT
                exp_id,
                var_id,
                visit_date,
                beacon_uid,
                -- 該 user 該天 view_item 的數量
                avg(view_item_cnt) view_item_cnt,
                -- 該 user 該天 session_duration
                sum(session_duration) session_duration,
                max(e2) e2
            FROM
                join_df
            GROUP BY
                exp_id,
                var_id,
                visit_date,
                beacon_uid
        ),
        all_metric AS (
            SELECT
                exp_id,
                var_id,
                beacon_uid,
                -- 區間 User view_item 數
                sum(view_item_cnt) AS all_avg_view_item,
                --平均一日的 session_duration per user
                avg(session_duration) AS all_avg_duration,
                max(e2) AS all_e2
            FROM
                all_metric_per_day
            GROUP BY
                1,
                2,
                3
        )
        SELECT
            device_metric_avg.*,
            all_metric.all_avg_view_item,
            all_metric.all_avg_duration,
            all_metric.all_e2
        FROM device_metric_avg
        LEFT JOIN all_metric
            ON all_metric.var_id = device_metric_avg.var_id
            AND all_metric.beacon_uid = device_metric_avg.beacon_uid
        '''

        if mode == 'sql':
            return sql_query

        if mode == 'data':
            data = wr.athena.read_sql_query(sql=sql_query, database="default", categories = ['exp_id','device'])
            return data


    def get_metric_data(self, mode: str = 'sql', metric: dict = {}) -> Union[str, pd.DataFrame]:
        """
        ## Description
        查詢指標數據，一次只能算一個 metric，可以有多個 metric trigger
        
        ## Input
        - mode:str 輸出模式 (options:sql/data)
            - sql: 輸出 sql query (default)
            - data: 輸出 data table

        - metric:dict
            - metric_main: Dict 主要 metric 計算邏輯

                - metric_name: str 自定 metric 名稱
                - source: str 來源資料表 (options: journey/tracking)
                - group_level: str Group層級 (options: user/day/event)
                - method: str AGG邏輯 (options: max/min/avg/sum/count/boolean/count_distinct)
                - metric: dict 計算指標，舉例:
                    - {'funnel_e2': None}
                    - {'view_shop':'view_id'}

            - metric_trigger: Union[Dict, Tuple[Dict]] 計算 metric 額外的 trigger

                - source: str (options:journey/tracking)
                    - journey: 能算的 metric 僅開放 funnel, session_duration
                    - tracking
                    
                - param: Dict
                
        
        ## Input Example
        
        - e1_e2

            {
                'metric_trigger': {
                    'source': 'journey',
                    'param': {
                        'funnel_e1': None
                    }
                },
                'metric_main': {
                    'metric_name': 'e1_e2',
                    'source': 'journey',
                    'group_level': 'user',
                    'method': 'boolean',
                    'metric': {
                        'funnel_e2': None
                    }
                }
            }
            
        - view_item_cnt

            {
                'metric_main':{
                    'metric_name':'item_cnt',
                    'source':'tracking',
                    'group_level':'user',
                    'method':'count_distinct',
                    'metric':{
                        'view_item':'view_id'
                    }
                }
            }

        """

        # main 裡如果是 boolean 可以只有 condition ，裡面記錄 metric 的條件 0,1，metric 裡面記錄 method 實際計算的 column，journey 的部分特別會放在 metric 中
        # metric_main:{metric_name:xxx , 'source':'tracking' , group_level':'event' , method:'count_distinct',  condition:{} , metric:{}}
        # }

        if not metric.get('metric_main'):
            raise KeyError('metric_data should input metric')

        # ========== metric_trigger ==========
        journey_trigger_list, tracking_trigger_list = [], []
        if metric.get('metric_trigger'):
            if isinstance(metric['metric_trigger'], tuple):
                trigger = metric['metric_trigger']
            else:
                trigger = (metric['metric_trigger'],)
            journey_trigger_list = list(
                filter(lambda x: x['source'] == "journey", trigger))
            tracking_trigger_list = list(
                filter(lambda x: x['source'] == "tracking", trigger))

        # metric_trigger - journey
        journey_trigger_query = ''
        journey_metric_list = [
            'funnel_e1', 'funnel_e2',
            'funnel_interest', 'funnel_consideration',
            'add_to_cart', 'funnel_purchase',
            'session_duration'
        ]
        if (len(journey_trigger_list) == 1):
            journey_trigger = journey_trigger_list[0]['param']
            # 先限制 journey 表能算的 metric ，僅開放 funnel, session_duration
            if (len(set(journey_trigger.keys()) - set(journey_metric_list)) > 0):
                raise KeyError(
                    'Jouney can only set funnel and session_duration')
            journey_trigger_query = 'WHERE ' + \
                metric_map_to_query(
                    metric_map=journey_trigger,
                    source_type='journey',
                    journey_connector='and'
                )

        # metric_trigger - tracking
        tracking_trigger, tracking_trigger_query = {}, ''
        if (len(tracking_trigger_list) == 1):
            tracking_trigger = tracking_trigger_list[0]['param']
            tracking_trigger_query = 'AND ' + \
                event_map_to_query(trigger_map=tracking_trigger)

        if (len(tracking_trigger_list) > 1) | (len(journey_trigger_list) > 1):
            raise KeyError('check trigger input again')

        # ========== metric_main ==========
        """
        main 裡如果是 boolean 可以只有 condition ，裡面記錄 metric 的條件 0,1，metric 裡面記錄 method 實際計算的 column，journey 的部分特別會放在 metric 中
        """

        # Journey 的 metric 計算
        metric_join_query = ''
        metric_name = metric['metric_main']['metric_name']

        def metric_method(method, metric):  # 轉化 method(metric)
            if method == 'count_distinct':
                return f'count(distinct {metric})'
            elif method == 'boolean':
                return f'coalesce(max(cast({metric} as int)), 0)'
            else:
                return f'{method.replace("boolean" , "max")}({metric})'

        if metric['metric_main']['source'] == 'journey':
            # journey metric 給的方式  'metric_main':{'metric_name':'xxx','source':'tracking/journey' ,'group_level':user/event/day , method:'boolean' ,metric:{'funnel_e1':None, session_duration_sec:('>=' , 120)}
            join_df_metric = metric_map_to_query(
                metric['metric_main']['metric'], source_type='journey')

            # 有 tracking_trigger & journey_metric
            cte_tracking_metric_query = ''
            if (len(tracking_trigger_list) == 1):
                cte_tracking_metric_query = f'''
                metric_final as (
                    SELECT
                        beacon,
                        device,
                        min(date(event_timestamp)) date_
                    FROM
                        ath_st_client_side_tracking_without_dirt t
                    WHERE date(event_date) between date '{self.start}'
                    AND DATE_ADD('day', 1, date '{self.end}') -- event_date 要多取一天
                    AND date(event_timestamp) between date '{self.start}'
                    AND date '{self.end}'
                    AND device in ({list_to_str(self.device)})
                    {tracking_trigger_query}
                    GROUP BY
                        beacon,
                        device
                ),
                '''
                metric_join_query = """
                INNER JOIN metric_final
                    ON expt_journey.device = metric_final.device
                    AND expt_journey.beacon = metric_final.beacon
                    AND expt_journey.visit_date >= metric_final.date_
                """
            # journey metric 不能 group 在 event
            if metric['metric_main']['group_level'] == 'event':
                raise KeyError('journey metric cant group by event')

        elif metric['metric_main']['source'] == 'tracking':
            # 用 view 開頭判斷是 on_screen 的 metric 或是 view 的 metric
            if metric['metric_main'].get('event_type') is None:
                if metric['metric_main']['method'] == 'boolean':
                    event_type = ''
                elif str.find(list(metric['metric_main']['metric'].keys())[0], 'view') == 0:
                    event_type = 'view_event'
                else:
                    event_type = 'event_on_screen'
            else:
                # event_type 也可以手動設置，會影響最後的 metric 算法
                event_type = metric['metric_main'].get('event_type')

            event_condition = {}
            # 處理 metric 的 where clause
            if not metric['metric_main'].get('condition'):
                event_condition[list(metric['metric_main']['metric'].keys())[0]] = None
            else:
                if (event_type == "event_on_screen"):
                    if (len(set(metric['metric_main']['metric'].keys()) - set(metric['metric_main']['condition'])) > 0):
                        event_condition[list(metric['metric_main']['metric'].keys())[0]] = None
                event_condition = event_condition | metric['metric_main']['condition']

            metric_condition_query = f'{event_map_to_query(event_condition)}'

            # if the trigger's event_name = condition's event_name, deal with the dict name
            commend_key = set(event_condition.keys())&set(tracking_trigger.keys())
            if len(commend_key) >0:
                for k in commend_key:
                    print(k)
                    tracking_trigger[k+'_'] = tracking_trigger.pop(k)
            metric_trigger_query = f'and {event_map_to_query(event_condition|tracking_trigger)}'

            if metric['metric_main']['method'] == 'boolean':
                view_id = f"case when {metric_condition_query} then event_params_map['from_view_id'].string_value else event_params_map['view_id'].string_value end"
                metric_ = metric_condition_query
            elif event_type == 'view_event':
                if metric['metric_main'].get('method') not in ['count', 'count_distinct', 'boolean']:
                    raise KeyError(
                        'view_event only can be count, count_distinct, boolean')
                if (len(tracking_trigger_list) == 1):
                    view_id = f"case when {metric_condition_query} then event_params_map['from_view_id'].string_value else event_params_map['view_id'].string_value end"
                    metric_ = f"case when {metric_condition_query} then {metric_map_to_query(metric['metric_main']['metric'] , 'tracking')} else null end"
                else:
                    view_id = "event_params_map['from_view_id'].string_value"
                    metric_ = metric_map_to_query(
                        metric['metric_main']['metric'], 'tracking')

            elif event_type == 'event_on_screen':
                # event_on_screen 設置 event_trigger 目前沒想好解法，先不開放，直接設置在 trigger
                if (len(tracking_trigger_list) == 1):
                    raise KeyError(
                        'current event_on_screen cant set event trigger')
                # event_on_screen 的 condition
                if metric['metric_main']['method'] in ['count_distinct', 'count']:
                    metric_ = f"{metric_map_to_query(metric['metric_main']['metric'] , 'tracking')}"
                else:
                    metric_ = f"coalesce({metric_map_to_query(metric['metric_main']['metric'] , 'tracking')} , 0)"
                view_id = "event_params_map['view_id'].string_value"

            if metric['metric_main']['group_level'] == 'event':
                if (event_type == 'view_event') & (len(tracking_trigger_list) != 1):
                    raise KeyError(
                        'metric_type is view_event group by event should set tracking trigger')
                cte_tracking_metric_query = f"""
                metric_final as (
                    SELECT
                        beacon,
                        device,
                        date(event_timestamp) as date_,
                        {view_id},
                        {metric_method(metric['metric_main']['method'],metric_)} as "{metric_name}"
                    FROM
                        ath_st_client_side_tracking_without_dirt t
                    WHERE date(event_date) between date '{self.start}'
                    AND DATE_ADD('day', 1, date '{self.end}') -- event_date 要多取一天
                    AND date(event_timestamp) between date '{self.start}'
                    AND date '{self.end}'
                    AND device in ({list_to_str(self.device)})
                    {self.lang_query}
                    {metric_trigger_query}
                    GROUP BY
                        ({view_id}),
                        beacon,
                        device,
                        date(event_timestamp)
                ),
                """
            else:
                cte_tracking_metric_query = f"""
                metric_final as (
                    SELECT
                        beacon,
                        device,
                        date(event_timestamp) as date_,
                        {metric_} as "{metric_name}"
                    FROM
                        ath_st_client_side_tracking_without_dirt t
                    WHERE date(event_date) between date '{self.start}'
                    AND DATE_ADD('day', 1, date '{self.end}') -- event_date 要多取一天
                    AND date(event_timestamp) between date '{self.start}'
                    AND date '{self.end}'
                    AND device in ({list_to_str(self.device)})
                    {self.lang_query}
                    {metric_trigger_query}
                ),
                """

            # 有沒有 tracking trigger 決定後續的 join 方式
            if (len(tracking_trigger_list) == 1):
                metric_join_query = """
                INNER JOIN metric_final
                    ON expt_journey.device = metric_final.device
                    AND expt_journey.beacon = metric_final.beacon
                    AND expt_journey.visit_date = metric_final.date_
                """
            elif (len(tracking_trigger_list) == 0):  # tracking metric without tracking trigger
                metric_join_query = """
                LEFT JOIN metric_final
                    ON expt_journey.device = metric_final.device
                    AND expt_journey.beacon = metric_final.beacon
                    AND expt_journey.visit_date = metric_final.date_
                """

            join_df_metric = f'"{metric_name}"'
        else:
            raise KeyError(
                'Check metric_source, only allow tracking or journey')

        if metric['metric_main']['group_level'] == 'event':
            sql_query_2 = f"""
            join_df as (
                SELECT
                    exp_id,
                    var_id,
                    beacon_uid,
                    expt_journey.device,
                    {join_df_metric} as "{metric_name}"
                FROM
                    expt_journey
                {self.all_trigger_join_query}
                {metric_join_query}
                {journey_trigger_query}
                -- uid 中同個 device 可能會有多個 beacon 的數值， cross join 後需要先 group
            ),
            """
            all_metric_source = 'join_df'  # for by day metric
            method = 'avg'

        elif metric['metric_main']['group_level'] == 'day':
            sql_query_2 = f"""
            join_df as (
                SELECT
                    exp_id,
                    var_id,
                    beacon_uid,
                    expt_journey.visit_date,
                    expt_journey.device,
                    {metric_method(metric['metric_main']['method'],join_df_metric)} as "{metric_name}"
                FROM
                    expt_journey
                {self.all_trigger_join_query}
                {metric_join_query}
                {journey_trigger_query}
                -- uid 中同個 device 可能會有多個 beacon 的數值， cross join 後需要先 group
                GROUP BY
                    exp_id,
                    var_id,
                    beacon_uid,
                    expt_journey.device,
                    expt_journey.visit_date
            ),
            join_df_date_all as (
                SELECT
                    exp_id,
                    var_id,
                    beacon_uid,
                    expt_journey.visit_date,
                    {metric_method(metric['metric_main']['method'],join_df_metric)} as "{metric_name}"
                FROM
                    expt_journey
                {self.all_trigger_join_query}
                {metric_join_query}
                {journey_trigger_query}
                GROUP BY
                    exp_id,
                    var_id,
                    beacon_uid,
                    expt_journey.visit_date
            ),
            """
            all_metric_source = 'join_df_date_all'
            method = 'avg'

        elif metric['metric_main']['group_level'] == 'user':
            sql_query_2 = f"""
            join_df as (
                SELECT
                    exp_id,
                    var_id,
                    beacon_uid,
                    expt_journey.device,
                    {join_df_metric} as "{metric_name}"
                FROM
                    expt_journey
                {self.all_trigger_join_query}
                {metric_join_query}
                {journey_trigger_query}
                -- uid 中同個 device 可能會有多個 beacon 的數值， cross join 後需要先 group
            ),
            """
            all_metric_source = 'join_df'  # for by day metric
            method = metric['metric_main']['method']
        else:
            raise KeyError('check group_level input agian')

        # 最後到 join_df ->
        # event --> beacon_uid  , (metric -> count(distinct view_id), )
        # date -> beacon_uid , visit_date , metric
        # user -> beacon_uid,
        sql_query_final = f'''
        device_metric as (
            SELECT
                exp_id,
                var_id,
                beacon_uid,
                device,
                {metric_method( method, f'"{metric_name}"')} as "device_{metric_name}"
            FROM
                join_df
            GROUP BY
                exp_id,
                var_id,
                beacon_uid,
                device
        ),
        -- 計算該 beacon_uid 每一天 view_item 的數量以及 session_duration (包含不同 device)
        all_metric as (
            SELECT
                exp_id,
                var_id,
                beacon_uid,
                {metric_method( method, f'"{metric_name}"')} as "all_{metric_name}"
            FROM {all_metric_source}
            GROUP BY
                exp_id,
                var_id,
                beacon_uid
        )
        SELECT
            device_metric.*,
            all_metric."all_{metric_name}"
        FROM
            device_metric
        LEFT JOIN all_metric
            ON all_metric.var_id =device_metric.var_id
            AND all_metric.beacon_uid = device_metric.beacon_uid
        '''

        sql_query = f'''
        {self.sql_query_1}
        {self.all_trigger_query}
        {cte_tracking_metric_query}
        {sql_query_2}
        {sql_query_final}
        '''

        if mode == 'sql':
            return format_sql_query(sql_query)

        if mode == 'data':
            data = wr.athena.read_sql_query(sql=sql_query, database="default" , categories = ['exp_id','device'])
            return data

    def get_mde_data(self, mode='sql'):
        pass
