

def get_funnel_metric_input(start_funnel: str, end_funnel: list):
    
    metric_input = {}
    
    if start_funnel != 'visit':
        metric_input['metric_trigger'] = {
            'source': 'journey',
            'param': {start_funnel: None}
        }
    
    metric_name = f'{start_funnel} -> {"/".join(end_funnel)}'
    metric_input['metric_main'] = {
        'metric_name': metric_name,
        'source': 'journey',
        'group_level': 'user',
        'method': 'max',
        'metric': {x: None for x in end_funnel}
    }

    return metric_name, metric_input


def get_session_duration_per_day_metric_input():
    
    metric_name = 'Avg.Session_duration per Day'
    metric_input = {
        'metric_main': {
            'metric_name': metric_name,
            'source': 'journey',
            'group_level': 'day',
            'method': 'sum',
            'metric': {
                'session_duration': None
            }
        }
    }
    
    return metric_name, metric_input


def get_view_item_cnt_per_user_metric_input():
    
    metric_name = 'Avg.View_Item_cnt per User'
    metric_input = {
        'metric_main': {
            'metric_name': metric_name,
            'source': 'tracking',
            'group_level': 'user',
            'method': 'count_distinct',
            'metric': {
                'view_item': 'view_id'
            }
        }
    }
    
    return metric_name, metric_input


def get_view_event_cnt_per_user_metric_input():

    event_list = [
        'view_home', 'view_search_results', 'view_shop', 'view_item_recommend', 
        'view_favlist_item', 'view_feed', 'view_topic', 'view_favlist_shop',
        'view_flagship', 'view_middle_layer', 'view_favlist_collection',
        'view_collection', 'view_video_list', 'view_discover'
    ]

    metric_name = 'Avg.View_Event_cnt per User'
    metric_input = {
        'metric_main': {
            'metric_name': metric_name,
            'source': 'tracking',
            'group_level': 'user',
            'method': 'count_distinct',
            'condition': {
                x: None for x in event_list
            },
            'metric': {
                'view_search_results': 'view_id'
            }
        }
    }
    
    return metric_name, metric_input


def get_input_event_cnt_per_user_metric_input(event_name):
    '''
    - event_name: str, options: view_item, view_fav_collection, ...
    '''
    
    metric_name = f'Avg.{event_name}_cnt per User'
    metric_input = {
        'metric_main': {
            'metric_name': metric_name,
            'source': 'tracking',
            'group_level': 'user',
            'method': 'count_distinct',
            'metric': {
                f'{event_name}': 'view_id'
            }
        }
    }
    
    return metric_name, metric_input


def get_input_event_adoption_metric_input(event_name):
    '''
    - event_name: str, options: view_item, view_fav_collection, ...
    '''
    
    metric_name = f'{event_name}_adoption_rate'
    metric_input = {
        'metric_main': {
            'metric_name': metric_name,
            'source': 'tracking',
            'group_level': 'user',
            'method': 'boolean',
            'metric': {
                f'{event_name}': 'view_id'
            }
        }
    }
    
    return metric_name, metric_input


def get_ad_click_cnt_per_user_metric_input(ad_type):
    '''
    - ad_type: str, options: pb, pl
    '''

    metric_name = f'Avg.{ad_type}_ad_click_cnt per User'
    metric_input = {
        'metric_main': {
            'metric_name': metric_name,
            'source': 'tracking',
            'group_level': 'user',
            'method': 'count_distinct',
            'condition': {
                'view_item': {
                    'promoted_type': ad_type
                },
                'view_shop': {
                    'promoted_type': ad_type
                }
            },
            'metric': {
                'view_item': 'view_id'
            }
        }
    }
    
    return metric_name, metric_input
    

def get_general_metric_input(metric_type, **kwargs):
    
    if metric_type == 'funnel':
        metric_name, metric_input = get_funnel_metric_input(
            kwargs['start_funnel'], kwargs['end_funnel']
        )

    if metric_type == 'session_duration_per_day':
        metric_name, metric_input = get_session_duration_per_day_metric_input()

    if metric_type == 'view_item_cnt_per_user':
        metric_name, metric_input = get_view_item_cnt_per_user_metric_input()

    if metric_type == 'view_event_cnt_per_user':
        metric_name, metric_input = get_view_event_cnt_per_user_metric_input()

    if metric_type == '{event_name}_cnt_per_user':
        metric_name, metric_input = get_input_event_cnt_per_user_metric_input(
            kwargs['event_name']
        )

    if metric_type == '{event_name}_adoption':
        metric_name, metric_input = get_input_event_adoption_metric_input(
            kwargs['event_name']
        )

    if metric_type == 'ad_click_cnt_per_user':
        metric_name, metric_input = get_ad_click_cnt_per_user_metric_input(
            kwargs['ad_type']
        )
    
    metric_name = metric_name.lower()
    
    return metric_name, metric_input
