import spacy
nlp_zh = spacy.load("zh_core_web_lg", exclude=("tagger", "parser", "senter", "attribute_ruler", "ner"))
import numpy as np
import pandas as pd
import jieba
import re
import seaborn as sns
from scipy.spatial.distance import cosine


class event_enbedding_related_tag:
    '''
    用 embedding 相關的算法標記 related 的資料
    '''
    def __init__(self ,df,embedding_df, event_list , base_on ):
        if isinstance(event_list , str):
            self.event_list = [event_list]
        else:
            self.event_list = event_list
        self.base_on = base_on
        self.base_on_event = 'event_desc'

        if base_on in ('tid' , 'item_id' ):
            self.base_on = "item_id"
            self.embedding = embedding_df.query('entity_type == "tid"')
            self.base_on_event = 'event_name'

        
        elif base_on in ('tid_array'):
            self.base_on = "tid_array"
            self.embedding = embedding_df.query('entity_type == "tid"')
            self.base_on_event = 'event_name'


        elif base_on in ('search_term' , 'kw' , 'keyword'):
            self.base_on = "search_term"
            
            
        elif base_on in ('sid' , 'shop' , 'shop_id'):
            self.base_on = "sid"
            self.embedding = embedding_df.query('entity_type == "sid"')
            
        self.df_ =df.query(f'{self.base_on_event}.isin({self.event_list}) & {self.base_on}.notnull()')[['merge_index','beacon_uid' ,self.base_on ,'event_name' , 'event_desc', 'event_final']]

    def data_prepare(self ,n_shift = 10,check_data = True , split_event = False):
        # kw 不用 emdebbing table
        if self.base_on == 'search_term':
            # 先將 search_term 作處理
            rule = re.compile(u"[^a-zA-Z0-9\u4e00-\u9fa5 ]")
            serch_term_list = pd.Series(self.df_[self.base_on].unique()).rename('search_term').to_frame()
            # 去除特殊字符
            serch_term_list['search_term_fix'] = serch_term_list[self.base_on].map(lambda x:rule.sub(' ',x))
            # 用 jieba 做分詞，全分詞
            serch_term_list['search_term_split'] = serch_term_list[self.base_on].map(lambda x: list(set([y for y in jieba.lcut_for_search(x) if y!=" "])))
            ## 將分詞失敗的 drop 掉（一些日文）
            serch_term_list_explode = serch_term_list.explode('search_term_split').dropna()
            ## 用 nlp model 會跑比較久
            term_split = serch_term_list_explode.search_term_split.unique()
            term_embedding_map = dict(zip(term_split,list(map(lambda x:nlp_zh(x).vector , term_split))))
            serch_term_list_explode['embedding'] = serch_term_list_explode.search_term_split.map(term_embedding_map)

            # 將分詞做 mean
            self.embedding = serch_term_list_explode.groupby('search_term').embedding.mean().to_frame()
            self.embedding['isnull'] = self.embedding.embedding.map(sum)

            if check_data:
                print('沒有 embedding 的資料比例',len(self.embedding.query('isnull ==0')) / len(self.embedding))
            self.embedding = self.embedding.query('isnull != 0')
            embedding_ = self.embedding.embedding
            
        else:
            embedding_ = self.embedding.set_index('entity').embedding
        
        # 判斷後續計算 shift 要不要區分 event
        if split_event:
            group_by_ = ['beacon_uid',self.base_on_event]
        else:
            group_by_ = ['beacon_uid']
        
        # tid_array 需要額外處理
        if self.base_on == 'tid_array':
            temp = self.df_.explode(self.base_on)
            temp['embedding'] = temp[self.base_on].map(embedding_)
            temp.dropna(subset = 'embedding' , inplace = True)
            
            # tid_array 的 embedding 用加權平均先處理
            temp_cate_embedding = temp.groupby(['beacon_uid','merge_index']).embedding.sum() / temp.groupby(['beacon_uid' , 'merge_index']).embedding.count()
            self.df_['embedding']  = self.df_.merge_index.map(temp_cate_embedding.reset_index('beacon_uid' , drop = True).map(lambda w:[w]))
            self.df_.dropna(subset = 'embedding' , inplace = True)
            
            # 直接 shift embedding 不留 base on 的 shift
            self.df_['embedding_shift'] = self.df_.groupby(group_by_)['embedding'].shift(1).map(lambda x: x if isinstance(x , list) else [])
            self.df_['embedding_shift'] = self.df_.groupby(group_by_)['embedding_shift'].apply(lambda x:x.cumsum()).map(lambda x:x[-n_shift:])
            self.df_final = self.df_.copy()
            self.df_final['embedding'] = self.df_final['embedding'].map(lambda x:x[0])
            self.df_explode = self.df_final.loc[self.df_final['embedding_shift'].map(len)>0].explode('embedding_shift')       
        else:
            # 需要先轉化 list 為後續 cumsum 做準備
            self.df_[self.base_on] = self.df_[self.base_on].map(lambda x:[x])
            self.base_on_shift = f'{self.base_on}_shift'
            
            # 其他的需要儲存內容，所以要存 shift 的資料
            self.df_[self.base_on_shift] = self.df_.groupby(group_by_)[self.base_on].shift(1).map(lambda x: x if isinstance(x , list) else [])
            self.df_[self.base_on_shift] = self.df_.groupby(group_by_)[self.base_on_shift].apply(lambda x:x.cumsum()).map(lambda x:x[-n_shift:])
                        
            self.df_final = self.df_.copy()
            self.df_final[self.base_on] = self.df_final[self.base_on].map(lambda x:x[0])

            self.df_explode = self.df_final.loc[self.df_final[self.base_on_shift].map(len)>0].explode(self.base_on_shift)

            self.df_explode['embedding'] = self.df_explode[self.base_on].map(embedding_)
            self.df_explode['embedding_shift'] = self.df_explode[self.base_on_shift].map(embedding_)

            if check_data:
                print('沒有 embedding 的 search_term 比例',self.df_explode.query('embedding.isnull()')[self.base_on].nunique() /self.df_explode[self.base_on].nunique())
            self.df_explode.dropna(subset = ['embedding' , 'embedding_shift'] , inplace = True)
        
    # k,x 共同判斷 decay 
    def score_cal(self ,k = None,x = None ):
        
        if (k == None) & (self.base_on in ('search_term' , 'tid_array')):
            k = .5
            x = 1
        elif (k == None):
            k = .2
            x = 1
        
        self.df_explode['cosine_score'] = self.df_explode.apply(lambda x: 1-cosine(x.embedding , x.embedding_shift) , axis =1).rename('cosine_score')
        self.df_explode = self.df_explode.assign(temp = range(len(self.df_explode)) , rank = lambda x: x.groupby('merge_index').temp.rank(ascending = False)-1).drop('temp' , axis = 1)

        # score weigh mean -> 將 score 計算衰退後，做加權平均

        self.df_explode = self.df_explode.assign(decay_ = self.df_explode['rank'].map(lambda n:x *np.e**(-k*n)) ,cosine_score_adjust =lambda x:x.decay_* x.cosine_score)


        # 計算平均時 smae view 不做排除，因為 重複看也代表 User 現在的意圖
        mean_ = self.df_explode.groupby('merge_index').cosine_score_adjust.sum() / self.df_explode.groupby('merge_index').decay_.sum()
        self.df_final['score_adjuct'] = self.df_final.merge_index.map(mean_)
        
    def score_check(self ,check_type = 'raw_score'):
        if check_type == "raw_score":
            ww = self.df_explode.query(f'event_final.isnull() & {self.base_on_event} != "view_item_recommend"')
            sns.kdeplot(ww.cosine_score)
            print(ww.cosine_score.quantile([0,.25,.5,.75,.9,1]))
            print(ww.cosine_score.mean() , ww.cosine_score.mean()+1*ww.cosine_score.std())
        elif check_type == "adjust_score":
            ww = self.df_final.query(f'event_final.isnull() & score_adjuct.notnull() & {self.base_on_event} != "view_item_recommend"')
            sns.kdeplot(ww.score_adjuct)
            print(ww.score_adjuct.quantile([0,.25,.5,.75,.9,1]))
            print(ww.score_adjuct.mean() , ww.score_adjuct.mean()+1*ww.score_adjuct.std())
        else: 
            print('only access raw_score/adjust_score')
                                                                                     
    def related_index(self, threshold = None, related_col = None):
        threshold_dict = {'sid':.59 , 'search_term':.43 , 'item_id':.5, 'tid_array':.8, }
        if threshold == None:
            self.threshold = threshold_dict[self.base_on]
        else:
            self.threshold = threshold
        
        self.condition = (self.df_final.score_adjuct>=self.threshold)&(self.df_final.event_final.isnull() & (self.df_final[self.base_on_event] != "view_item_recommend")) 
        temp = self.df_final.copy()
        if self.base_on == 'tid_array':
            tag = '__related_impr'
        elif self.base_on == 'tid':
            tag = '__related_item'
        else:
            tag = '__related'
        
        if related_col == None:
            related_col = self.base_on_event
        
        temp.loc[self.condition, 'event_final'] = temp.loc[self.condition, related_col]+ tag
        
        return temp
                                                         
                                                                                     
    def store_idea_content(self):
        # 將 related 的 score 取出        
        content = self.df_final.query(f'score_adjuct >= {self.threshold}')
        ww1 = content[['beacon_uid' , self.base_on , 'score_adjuct']].groupby(['beacon_uid',self.base_on]).score_adjuct.mean()
        # 原先的 related 是往前曲 10 個計算 score，這邊同樣往後取 10 個，並作加權平均 
        ww2 = self.df_explode.groupby(['beacon_uid',self.base_on_shift]).cosine_score_adjust.sum() / self.df_explode.groupby(['beacon_uid',self.base_on_shift]).decay_.sum()
        ww2 = ww2.loc[lambda x:x>self.threshold]
        ww2.index = ww2.index.rename({self.base_on_shift:self.base_on})
        content_final  =pd.concat([ww1 , ww2]).groupby(['beacon_uid' ,self.base_on]).mean()
        groups = content_final.groupby('beacon_uid')
        content_dict = {key1: (group.reset_index(level = 0 , drop = True).to_dict()) for key1, group in groups}

        return content_dict
