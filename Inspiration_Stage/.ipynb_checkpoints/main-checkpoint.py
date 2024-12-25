print('Start Import')
import setting
import pandas as pd
import numpy as np
import awswrangler as wr
##  ignore warning
import warnings
warnings.simplefilter(action='ignore')
import pandas as pd
## 分詞 package
from scipy.spatial.distance import cosine
import time
# kw embedding model
from function.embedding_function import event_enbedding_related_tag
from function.other_function import history_columns
from function.other_function import model_reload_clustering


def content_row(df,df_content , col_name):
    '''
    tranform insipration content df for row type 
    '''
    ww = pd.Series(df_content)
    ww = ww.map(lambda x:list(tuple(x.items())))
    df[col_name] = df.beacon_uid.map(ww)


t = time.localtime()
on_date = (f'{t.tm_year}-{t.tm_mon}-{t.tm_mday-1}')

print('Start Extract Data')
sql_embedding = f'''
SELECT *
from data.ath_st_user_interact_entity_emb 
where entity_type in ('sid' , 'tid' , 'subcategory_name', 'category_name')
and date(on_date) = date '{on_date}'
'''

df_embedding = wr.athena.read_sql_query(sql  = sql_embedding , database  ='default')
print('embedding finish!')

# # data extract
sql_test = f'''
with on_date_user as (
select beacon_uid , beacon , device
from ath_exploring_event 
where date(event_date) = date('{on_date}')
and geo in ('TW' , 'HK')
and not coalesce(is_seller , False) 
group by 1,2,3
having max(e1)
)
select 
visit_date , event_date , event_timestamp, e.beacon_uid, e.beacon, e.device,session_id,is_landing, event_name, event_desc, view_id, screen_name, 
from_screen, from_section, from_modal, from_view_id, search_term, total_result, page, 
filter_dict, item_id, category, subcategory, catp, view_id_rank, sid, impr_cat_map, tid_array, e1,e2,i,c,a,p
from ath_exploring_event e
inner join on_date_user
on on_date_user.beacon = e.beacon and on_date_user.device = e.device
where date(event_date) between date(date_add('day' , -6,date('{on_date}'))) and date('{on_date}')
and event_name in ('view_home',               'view_topic',
                'view_item',          'add_to_wishlist',
              'follow_shop',        'view_favlist_item',
      'view_search_results',              'add_to_cart',
                'view_cart',      'view_item_recommend',
                'view_shop',            'view_flagship',
                'view_feed',          'view_collection',
 'view_shop_search_results',                 'purchase')
and coalesce(case when event_name = 'view_search_results' and screen_name = 'category_tab' and e.device = 'iphone' and cardinality(tid_array) is null then 0 else cardinality(tid_array) end ,9999) >6
and event_desc != 'view_shop_search_results'
and view_id_rank = 1
'''

raw_data = wr.athena.read_sql_query(sql  = sql_test , database  ='default' )
raw_data.sort_values(['beacon_uid' , 'event_timestamp'], ignore_index = True ,inplace = True)
raw_data['visit_date'] = pd.to_datetime(raw_data.visit_date)
raw_data['event_date'] = pd.to_datetime(raw_data.event_date)
print('exploring event finish!')

print('Start Clean Data')
# drop duplicated
df_raw = raw_data.drop_duplicates(['beacon_uid' , 'device' , 'event_timestamp' , 'event_name' , 'view_id']).copy()
### 修正 sql 沒處理乾淨的，是 screen name 有問題的
df_raw.loc[(df_raw.event_desc == "browse_category") & (df_raw.category.isnull()) &(df_raw.catp.isnull()) &(df_raw.search_term.str.find('@') !=-1),'event_desc'] = 'orp'
df_raw.loc[(df_raw.event_desc == "browse_category") & (df_raw.category.isnull()) &(df_raw.catp.isnull()) &(df_raw.search_term.notnull()),'event_desc'] = 'search'
df_raw.loc[(df_raw.event_desc == "browse_category") & (df_raw.category.isnull()) &(df_raw.catp.isnull()),'event_desc'] = 'other_srp'
df_raw.loc[(df_raw.event_desc.isin(["search" , 'orp']))&(df_raw.search_term.isnull()) , 'event_desc'] = 'other_srp'

temp = df_raw.query('(event_name == "view_search_results")&(catp.notnull())')
## 轉化 catp
df_raw.loc[temp.index,  'category'] = temp.catp.map(lambda x: str.split(x[0] , ',')[0])
## 取 len >1 的 sub cate，因為有些catp 埋錯，所以用比較烙的方式 -> 部分 catp 埋成 jason string
ww = temp.catp.map(lambda x: str.split(x[-1] , ',')[-1]).loc[temp.catp.map(lambda x: str.split(x[-1] , ',')[0]) != temp.catp.map(lambda x: str.split(x[0] , ',')[-1])]
df_raw.loc[ww.index , 'subcategory']  =ww
# 將 subcategory fillna ，為了後續判斷
df_raw.loc[df_raw.query('event_desc=="browse_category" & subcategory.isnull()').index , 'subcategory'] = 'null'

# 將 catp 加回 filter_dict
temp = df_raw.query('event_name == "view_search_results"&category.notnull()')
temp.loc[temp.query('subcategory.isnull()|subcategory == "null"').index , 'subcategory'] = ''
filter_catp = (temp.category+','+temp.subcategory).map(lambda x:[x[:-1] if x[-1]==',' else x])
df_raw.loc[filter_catp.index , 'filter_dict'] = df_raw.loc[filter_catp.index , 'filter_dict'].map(lambda x:list(x)) + filter_catp.map(lambda x:list(x))

## 轉化 filter_dict list->str sort value
df_raw['filter_dict'] = df_raw.filter_dict.map(lambda x: x if isinstance(x, np.ndarray)|isinstance(x, list) else [])
df_raw['filter_dict'] = df_raw.filter_dict.map(lambda x: ','.join(sorted(x)))


# purchase 時間用 max 的 event_timestamp，在 +8 跟 TW 時間對其
df_raw['purchase'] = df_raw.beacon_uid.map(df_raw.query('event_name == "purchase"').groupby('beacon_uid').event_timestamp.max()) +pd.Timedelta(hours=8)
on_date_ = pd.to_datetime(on_date)

# drop掉非同一天， purchase 前的 data 
temp = df_raw.query(f'purchase.notnull() & purchase.dt.date != @on_date_')
index_1 = (temp.event_timestamp.dt.date <= temp.purchase.dt.date).loc[lambda x:x].index

# drop掉同一天， purchase 後的 data 
temp2 = df_raw.query(f'purchase.notnull() & purchase.dt.date == @on_date_')
index_2 = temp2.loc[temp2.event_timestamp >= temp2.purchase].index

df_raw.drop(list(index_1)+list(index_2) , inplace = True)
df_raw.reset_index(drop = True , inplace = True)


# drop 掉 on_date 當天沒有資料的 user -> date 時區不同導致的 bug
beacon_drop= df_raw.groupby('beacon_uid').event_date.max().loc[lambda x:x != on_date_].index
df_raw = df_raw.query('~beacon_uid.isin(@beacon_drop)')

df_raw.drop(['e1','e2','i','c','p','a'],axis = 1 , inplace = True)
print('drop duplicated finish')

# drop 沒有 impression 的 view_event
without_impr_index = df_raw.query('event_name.isin(["view_search_results" , "view_shop" , "view_item_recommend","view_shop_search_results" , "view_topic"]) & impr_cat_map.isnull()' , engine = 'python').index
## 保留後續有到 item 的 without_impr view_id
temp = df_raw.loc[without_impr_index].view_id
next_with_view_item_view_id = df_raw.query('from_view_id.isin(@temp)&event_desc == "view_item"').from_view_id.unique()
keep_index = df_raw.loc[without_impr_index].query('view_id.isin(@next_with_view_item_view_id)').index

df_path = df_raw.drop(set(without_impr_index) - set(keep_index)).reset_index(drop = True)


# 建立 event 對應的上一個 event
for pre_ in ['event_desc' , 'sid' , 'item_id' , 'search_term' , 'filter_dict','total_result' , 'category' , 'subcategory']:
    if pre_ == 'event_desc':
        w = 'event'
    else:
        w = pre_
    df_path[f'pre_{w}'] = df_path.groupby('beacon_uid')[pre_].shift(1)
df_path['pre_event'].replace({'view_item_itoi':'view_item' ,'view_item_from_item_inshop':'view_item'} , inplace = True)


## drop 重複且連續出現的 view_shop(shop_srp, flagship) / view_item / view_home (drop_data)
df_path.drop(df_path.query('event_desc == "view_shop" & pre_event == event_desc & sid == pre_sid ' , engine = 'python').index , inplace = True)
df_path.drop(df_path.query('event_name == "view_item" & pre_event == event_name & item_id == pre_item_id ' , engine = 'python').index , inplace = True)
df_path.drop(df_path.query('event_name == "view_home" & pre_event == event_desc' , engine = 'python').index , inplace = True)
df_path.drop(df_path.query('event_name == "view_topic" & pre_event == event_desc & sid == pre_sid' , engine = 'python').index , inplace = True)
df_path.drop(df_path.query('event_desc == "view_item_recommend" & pre_event == event_desc & item_id == pre_item_id ' , engine = 'python').index , inplace = True)

df_path['total_result'] = df_path.total_result.astype(float)
df_path['pre_total_result'] = df_path.pre_total_result.astype(float)

## srp 連續的 event 區分 (drop_data)
for e in ['orp' , 'search']:
    ## drop 前後 event 一一樣， term 一樣， filter 一樣的 event ,並且 total_result 跟上一個差 5 個以內，避免誤刪
    df_path.drop(df_path.query('event_desc == @e & pre_event == event_desc & pre_search_term == search_term &filter_dict == pre_filter_dict & ((total_result >= pre_total_result -5) &(total_result <= pre_total_result +5))' , engine = 'python').index , inplace = True)

## browse 用 category, subcate 判斷 (drop_data)
df_path.drop( df_path.query('event_desc == "browse_category" & pre_event == event_desc&filter_dict == pre_filter_dict & ((total_result >= pre_total_result -5) &(total_result <= pre_total_result +5))' , engine = 'python').index, inplace = True)

df_path.reset_index(inplace = True , drop = True)
df_path= df_path.reset_index().rename({'index':'merge_index'} , axis = 1)


# same view & filter narrow

# 將 filter_dict 轉化，先找完全一樣的
df_srp = df_path.query('event_desc.isin(["search" , "orp" , "browse_category"])')
df_srp['filter_dict'] = df_srp.filter_dict.str.replace(',','|').map(lambda x:[x])

# cumsum filter_dict
ww = history_columns('filter_dict',df_srp ,series_append_drop = False)
ww1 = pd.merge(left = df_srp[['merge_index','beacon_uid','filter_dict']] , right = ww , on = 'merge_index')
same_page_index_srp = ww1.loc[ww1.apply(lambda x:len(set(x.filter_dict) & set(x.final)) >0, axis =1)].merge_index

# 用主要的 param (search_term, category|subcategory)判斷 是不是 narrow 

df_srp.loc[df_srp.event_desc.isin(['search' , 'orp']) , 'event_main_param'] = df_srp.loc[df_srp.event_desc.isin(['search' , 'orp']) , 'search_term']
df_srp.loc[df_srp.event_desc.isin(['browse_category']) , 'event_main_param'] = df_srp.loc[df_srp.event_desc.isin(['browse_category'])].assign(t = lambda x:x.category +'|'+ x.subcategory).t 
df_srp['event_main_param'] = df_srp.event_main_param.map(lambda x:[x])

para_cum = history_columns('event_main_param',df_srp ,series_append_drop = False)
ww2 = pd.merge(left = df_srp[['merge_index','beacon_uid','event_main_param']] , right = para_cum , on = 'merge_index')
rough_same_page_index = ww2.loc[ww2.apply(lambda x:len(set(x.event_main_param) & set(x.final)) >0, axis =1)].merge_index

# filter not same but main params same then is_filter or delete filter
narrow_index = (set(rough_same_page_index) - set(same_page_index_srp))

df_path.loc[df_path.query('merge_index.isin(@same_page_index_srp)').index , 'event_final'] =df_path.loc[df_path.query('merge_index.isin(@same_page_index_srp)').index , 'event_desc']+'_same'
df_path.loc[df_path.query('merge_index.isin(@narrow_index)').index , 'event_final'] = df_path.loc[df_path.query('merge_index.isin(@narrow_index)').index , 'event_desc'] + '_filter' 


# same view page tag
for e in ['view_item' , 'view_shop' ,'view_topic']:
    if e == "view_item":
        event_c = 'event_name'
    else:
        event_c = 'event_desc'
    temp = df_path.query(f'{event_c} == @e')

    columns_dict = {'view_item':'item_id' , 'view_shop':'sid', 'view_item_recommend':'item_id', 'view_topic':'sid'}
    temp[columns_dict[e]] =temp[columns_dict[e]].map(lambda x:[x]) 
    ww = history_columns(columns_dict[e],temp ,series_append_drop = False)
    ww1 = pd.merge(left = temp[['merge_index','beacon_uid',columns_dict[e]]] , right = ww , on = 'merge_index')
    same_page_index = ww1.loc[ww1.apply(lambda x:len(set(x[columns_dict[e]]) & set(x.final)) >0, axis =1)].merge_index
    df_path.loc[df_path.query('merge_index.isin(@same_page_index)').index , 'event_final'] =df_path.loc[df_path.query('merge_index.isin(@same_page_index)').index , event_c]+'_same'
    print(e)


# category -> subcategory

browse_df = df_srp.query('event_desc == "browse_category" & ~merge_index.isin(@rough_same_page_index)')
browse_df['category'] = browse_df.category.map(lambda x:[x])
browse_df['subcategory'] = browse_df.subcategory.replace('null' , np.nan).map(lambda x:[(x)] if pd.notnull(x) else [])

category_cum = history_columns('category',browse_df ,series_append_drop = False)
subcategory_cum = history_columns('subcategory',browse_df ,series_append_drop = False)
cate_ = pd.concat([category_cum.rename({'final':'category_cun'} , axis =1) , subcategory_cum.drop('merge_index' , axis =1).rename({'final':'subcategory_cun'} , axis = 1)] ,axis =1)
ww = browse_df[['merge_index','beacon_uid','category' , 'subcategory']].merge(cate_ , on = 'merge_index')
cate_narrow_index = ww.loc[ww.apply(lambda x:len(set(x.category) & set(x.category_cun)) >0, axis =1)].merge_index

## 同樣是 category related 不一定是 narrow 可能是 loosen
df_path.loc[df_path.query('merge_index.isin(@cate_narrow_index)').index , 'event_final']= df_path.loc[df_path.query('merge_index.isin(@cate_narrow_index)').index , 'event_desc'] + '_cate_narrow' 


## drop 連續的 filter (drop_data)
series_narror_index = df_path.assign(event_final_shift = df_path.groupby('beacon_uid').event_final.shift(-1)).query('(event_final.str.find("filter")!=-1) &event_final == event_final_shift' , engine = 'python').merge_index
df_path = df_path.query('~merge_index.isin(@series_narror_index)')

df_path.dropna(subset = 'beacon_uid' , inplace = True)
print('tag same event finish')


print('Start Embedding Tag')
# ### shop

shop_ = event_enbedding_related_tag(df = df_path , 
                                    embedding_df = df_embedding,
                                    event_list = 'view_shop' , 
                                    base_on = 'sid')
shop_.data_prepare(check_data = False)
shop_.score_cal()
final_event = shop_.related_index()
df_path.loc[df_path.query('merge_index.isin(@final_event.merge_index)').index , 'event_final'] = final_event.event_final.values 
shop_content = shop_.store_idea_content()
print('shop finish')

# ### search 

search_ = event_enbedding_related_tag(df = df_path, 
                                      embedding_df = df_embedding,
                                    event_list = ["search"] ,
                                    base_on = 'search_term' )
search_.data_prepare(check_data = False)
search_.score_cal()
final_event_s = search_.related_index() 
df_path.loc[df_path.query('merge_index.isin(@final_event_s.merge_index)').index , 'event_final'] = final_event_s.event_final.values 
search_content = search_.store_idea_content()
print('search finish')

# ### item

df_item = df_path.query('item_id.notnull()& event_name!="view_item_recommend"')
df_item['event_name'].replace({'add_to_wishlist':'add_to_cart_wishlist' , 'add_to_cart':'add_to_cart_wishlist'} , inplace = True)

# from_same page tag first
df_item_only_view = df_item.query('event_name =="view_item" & from_view_id.notnull() & ~from_screen.isin(["home","favlist_item","cart","feed","mission_game","wall"])')
df_item_only_view['from_view_id'] = df_item_only_view['from_view_id'].map(lambda x:[x])
df_item_only_view['from_view_id_cum'] = history_columns('from_view_id',df_item_only_view , series_append_drop = False).final.values
from_same_page_index = df_item_only_view.apply(lambda x:x.from_view_id[0] in x.from_view_id_cum , axis = 1).loc[lambda x:x].index

df_item.loc[from_same_page_index , 'event_final'] =df_item.loc[from_same_page_index , 'event_final'].fillna('view_item_from_same_page')

# embedding related
item_ = event_enbedding_related_tag(df = df_item, 
                                    embedding_df = df_embedding,
                                    event_list = ['view_item' , 'add_to_cart_wishlist'] ,
                                    base_on = 'tid'  )
item_.data_prepare(split_event = True , check_data = False)
item_.score_cal()
final_event_i = item_.related_index() 
df_path.loc[df_path.query('merge_index.isin(@final_event_i.merge_index)').index , 'event_final'] = final_event_i.event_final.values 
item_content = item_.store_idea_content()
print('item finish')


# ### item_related_same_cate (item category related 不用 embedding 直接用 category 的相同比例做分數)
# related cate view_item 用 category embedding 判斷

df_item_cate = df_path.query('event_name == "view_item"')[['merge_index','beacon_uid' ,'event_name','category' , 'subcategory','item_id' , 'event_final']]
df_item_cate['subcategory'] = df_item_cate['subcategory'].map(lambda x:[x])
df_item_cate['subcategory_shift'] = df_item_cate.groupby(['beacon_uid' , 'event_name']).subcategory.shift(1).map(lambda x: x if isinstance(x , list) else [])
df_item_cate['subcategory_shift'] = df_item_cate.groupby(['beacon_uid' , 'event_name']).subcategory_shift.apply(lambda x:x.cumsum()).map(lambda x:x[-10:])
df_item_cate['subcategory'] = df_item_cate.subcategory.map(lambda x:x[0])
df_item_cate_final = df_item_cate.copy()
df_item_cate_explode = df_item_cate_final.explode('subcategory_shift')
df_item_cate_explode = df_item_cate_explode.assign(temp = range(len(df_item_cate_explode)) , rank = lambda x: x.groupby('merge_index').temp.rank(ascending = False)-1).drop('temp' , axis = 1)
# 判斷跟之前的 view_item 是不是同 subcategory
df_item_cate_explode['similar'] = (df_item_cate_explode.subcategory == df_item_cate_explode.subcategory_shift).astype(int)
k = .2
x = 1
# 用 decay 函數處理
df_item_cate_explode = df_item_cate_explode.assign(decay_ = df_item_cate_explode['rank'].map(lambda n:x *np.e**(-k*n)) ,cosine_score_adjust =lambda x:x.decay_* x.similar)
# 計算分數
# 計算平均時 smae view 不做排除，因為 重複看也代表 User 現在的意圖 
df_item_cate_explode_mean = df_item_cate_explode.groupby('merge_index').cosine_score_adjust.sum() / df_item_cate_explode.groupby('merge_index').decay_.sum()
df_item_cate['score_adjuct'] = df_item_cate.merge_index.map(df_item_cate_explode_mean)
# 標記 releated
item_cate_index = df_item_cate.query('event_final.isnull() &score_adjuct>.43').merge_index
df_path.loc[df_path.query('merge_index.isin(@item_cate_index)').index , 'event_final'] = 'view_item_related_cate'


# ### related impr
impr_ = event_enbedding_related_tag(df = df_path, 
                                    embedding_df = df_embedding,
                                    event_list = ["view_search_results" , "view_shop" , "view_item_recommend","view_shop_search_results" , "view_topic"] ,
                                    base_on = 'tid_array' )
impr_.data_prepare(split_event = False , check_data = False)
impr_.score_cal()
final_event_impr = impr_.related_index(related_col = 'event_desc') 
df_path.loc[df_path.query('merge_index.isin(@final_event_impr.merge_index)').index , 'event_final'] = final_event_impr.event_final.values 
print('related impr finish')

# # check final event
view_item_event_on_screen = df_path.query('event_desc.isin(["view_item_from_item_inshop" , "view_item_itoi"])')
view_item_event_on_screen['merge_index'] = view_item_event_on_screen.merge_index-0.5
view_item_event_on_screen['event_final'] = view_item_event_on_screen['event_desc'].replace({'view_item_itoi':'click_item_similar_item' , 'view_item_from_item_inshop':'click_item_inshop'})
view_item_event_on_screen['event_name'] = view_item_event_on_screen['event_final']
view_item_event_on_screen['event_desc'] = view_item_event_on_screen['event_final']

# 將 item 頁上的 click 額外拉出 event 描述
df_final = pd.concat([df_path,view_item_event_on_screen] , axis = 0).sort_values('merge_index').reset_index(drop = True)

# 整併類似 home 的集合頁
df_final.loc[df_final.query('event_desc.isin(["view_home" ,"view_feed","other_srp"])').index , 'event_final'] = 'view_home_other'
df_final.loc[df_final.query('event_final.isnull() & event_desc.isin(["add_to_cart" ,"add_to_wishlist"])').index , 'event_final'] = 'add_to_cart_wishlist_not_related'

# 不做描述的 event
not_change_index = df_final.query('event_desc.isin(["view_cart" ,"view_favlist_item","view_item_recommend","purchase" , "follow_shop"])').index
df_final.loc[not_change_index , 'event_final'] = df_final.loc[not_change_index , 'event_desc']
# 剩下的 view_item
else_item_index = df_final.query('event_final.isnull() & event_name == "view_item"').index
df_final.loc[else_item_index , 'event_final'] = 'view_item_not_related'
# 剩下的 event
else_event_index = df_final.query('event_final.isnull()').index
df_final.loc[else_event_index , 'event_final'] = df_final.loc[else_event_index , 'event_desc']+'_not_related'
df_final['view_rank'] = df_final.groupby('beacon_uid').merge_index.rank()
df_final = df_final[['event_timestamp' , 'beacon_uid' , 'device' ,'view_rank', 'event_final' , 'view_id','from_view_id' , 'search_term' ,'category' ,'subcategory','item_id' , 'sid', 'total_result' , 'filter_dict' ,'event_name', 'event_desc']]

df_final = df_final.query('event_desc !="purchase"' ,engine = 'python' )

# 將 event_final 轉換成簡單的形式
rough_dict = {'view_related_shop':'view_shop__related' ,'view_shop__related_impr':'view_shop__related', 
              'search_related_kw':'search__related', 'search__related_impr':'search__related', 'search_filter':'search__related',
              'browse_category_cate_narrow':'browse_category__related', 'browse_category__related_impr':'browse_category__related', 'browse_category_filter':'browse_category__related',
             'orp__related_impr':'orp__related' ,'orp_filter':'orp__related'}

simple_dict = {'add_to_cart_wishlist__related_item':'add_to_fav/cart&follow_shop' ,'add_to_cart_wishlist_not_related':'add_to_fav/cart&follow_shop','follow_shop':'add_to_fav/cart&follow_shop', 
              'view_cart':'view_cart/fav', 'view_favlist_item':'view_cart/fav',
              'orp__related':'orp_topic__related', 'view_topic__related_impr':'orp_topic__related',
             'orp_same':'orp_topic__same' ,'view_topic_same':'orp_topic__same',
              'orp_not_related':'orp_topic__not_related' ,'view_topic_not_related':'orp_topic__not_related',
              'view_item__related_item':'view_item__related' ,'view_item_from_same_page':'view_item__related','view_item_related_cate':'view_item__related'}

df_final['event_final_rough'] = df_final.event_final
temp = df_final.loc[df_final.event_final_rough.isin(rough_dict.keys()) , 'event_final_rough'].map(rough_dict)
df_final.loc[temp.index , 'event_final_rough'] = temp

df_final['event_final_simple'] =df_final['event_final_rough']
temp = df_final.loc[df_final.event_final_simple.isin(simple_dict.keys()) , 'event_final_simple'].map(simple_dict)
df_final.loc[temp.index , 'event_final_simple'] = temp

print('Start fit model')
# # Reload Model
df_model = df_final.query('event_final_simple != "add_to_fav/cart&follow_shop"').groupby('beacon_uid').event_final_simple.apply(list)
cluster_result_tfidf = model_reload_clustering(df_model)

df_final['uu_group'] = df_final.beacon_uid.map(cluster_result_tfidf)
df_final['on_date'] = on_date

print('Start storage df_final')
# df_final to athena analysis for test
BUCKET_NAME = 'pinkoi-analysis'
PREFIX= 'athena_data/inspiration_stage/event_final' 
S3_PATH = f's3://{BUCKET_NAME}/{PREFIX}/'
database = 'analysis'
table = 'buyer_explore_event_final'
partition = ['on_date'] 

wr.s3.to_parquet(
    df=df_final, 
    path=S3_PATH,
    dataset=True,
    mode="overwrite_partitions", 
    database= database,
    table= table,
    partition_cols = partition
)


# processing final dataset
df_raw_copy = df_raw.copy()
df_raw_copy['uu_group'] =df_raw_copy.beacon_uid.map(cluster_result_tfidf)
df_raw_copy['on_date'] = on_date_

df_uu_ =  df_raw_copy.groupby(['beacon_uid' , 'uu_group' , 'beacon' , 'device']).on_date.max().reset_index()
# 轉化 beacon -> athena array<row>
df_uu = df_uu_.groupby(['beacon_uid' , 'uu_group' , 'on_date']).apply(lambda x:x[['beacon','device']].to_dict(orient='records')).rename('beacon_item').reset_index()

df_raw_copy['event_date'] = df_raw_copy.event_date.astype(str)
visit_date_ = df_raw_copy.groupby('beacon_uid').event_date.unique()

# 儲存來訪日期 last 7 day
df_uu['event_date_array'] = df_uu.beacon_uid.map(visit_date_)

# 標注大的 stage
group_type = {}

for i in [1,3,14,15]:
    group_type[i] = 'G0'
    
for i in [13,8,10,4]:
    group_type[i] = 'G1'

for i in [2,9,0]:
    group_type[i] = 'G2'

for i in [6,7,5]:
    group_type[i] = 'G3'  

for i in [11,12]:
    group_type[i] = 'G4'

df_uu['Inspiration_stage'] = df_uu.uu_group.map(group_type)
df_uu['uu_group'] = df_uu['uu_group'].map(lambda x: 'group_'+str(int(x)))
df_uu['on_date'] = df_uu.on_date.astype(str)



# inspiration content
col_list = ['tid_inspiration_content' , 'sid_inspiration_content' , 'search_term_inspiration_content']
content = [item_content , shop_content ,search_content ]

for c, d in zip(col_list , content):
    content_row(df_uu ,d , c )
    # 轉化爲 athena map 的資料
    df_uu[c] = df_uu[c].map(lambda x:x if isinstance(x , list) else [('NaN',0.0)])

print('Start Storage df_uu')
# 將結果 upload to s3
BUCKET_NAME = 'pinkoi-analysis'
PREFIX= 'athena_data/inspiration_stage/user_data_former' 
S3_PATH = f's3://{BUCKET_NAME}/{PREFIX}/'
print(S3_PATH)

database = 'default'
table = 'ath_inspiration_stage'
dtype = {'uu_group':'string','beacon_uid':'string', 'on_date':'date', 'Inspiration_stage':'string',
         'beacon_item':'array<STRUCT<beacon:string,device:string>>' ,
         'tid_inspiration_content':'map<string,DOUBLE>' , 
         'sid_inspiration_content':'map<string,DOUBLE>' ,
         'search_term_inspiration_content':'map<string,DOUBLE>'  , 'event_date_array':'array<string>'
        }
partition = ['on_date'] 


wr.s3.to_parquet(
    df=df_uu, 
    path=S3_PATH,
    dataset=True, 
    mode="overwrite_partitions", 
    database= database,
    table= table,
    dtype= dtype,
    partition_cols = partition)

