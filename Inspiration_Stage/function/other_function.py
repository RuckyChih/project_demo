import numpy as np
import pandas as pd
from gensim.models.word2vec import Word2Vec
from gensim.models import TfidfModel
from gensim import corpora
from gensim.models.doc2vec import Doc2Vec
from gensim.models.doc2vec import TaggedDocument
import joblib
import os


def history_columns(column , df , series_append_drop = True):
    '''
    將 shift 的資料按順序存成 list
    '''
## 指定的 columns 抽出
    test1 = df[['merge_index','beacon_uid',column]].copy()
    test1[column] = test1[column].map(lambda x: [str(i) for i in x] if isinstance(x, list)|isinstance(x, np.ndarray) else [])

    # 針對 list 做 Unique 編號
    test2 = test1.eval(f'{column} = {column}.str.join(",")' , engine = 'python')
    ## session unique concum
    unique_count = test2.drop_duplicates(subset = ['beacon_uid' ,column]).query(f'{column} !=""').groupby(['beacon_uid']).cumcount() + 1
    test2['rank_'] = unique_count.reindex(test2.index).ffill()
    ## session columns rank
    test2['s_c_rank_'] = test2.groupby(['beacon_uid',column]).merge_index.rank()
    ## columns 的第一個不 append
    test2.loc[test2.s_c_rank_ == 1  , 'rank_'] = test2.loc[test2.s_c_rank_ == 1  , 'rank_'] -1
    test2.loc[test2[column] == '' , 'rank_'] = 0

    # 針對 list 做 unique append
    ww = test2.query(f'{column} != ""').groupby(['beacon_uid'])[column].unique()
    test2 = pd.merge(test2,ww.rename('cum_').reset_index() , on ='beacon_uid' , how = 'left')
    test2['cum_'] = test2['cum_'].map(lambda x: x if isinstance(x, list)|isinstance(x, np.ndarray) else [])
    ## 連續重複的行為不 apppend
    if series_append_drop:
        test2['shift_'] = test2.groupby('beacon_uid')[column].shift(1)
        test2['rank_session'] = test2.groupby('beacon_uid').merge_index.rank(method = 'dense')
        test2['rank_session_shift'] = test2.groupby(['beacon_uid' , column]).rank_session.shift(1)
        test2['d'] = (test2.rank_session - test2.rank_session_shift).fillna(0).replace(1 , np.nan).ffill()
        test2.loc[(test2[column] != '')&(test2[column] == test2['shift_'])&(test2['d'] == 0) , 'rank_'] -=1
    else:
        pass


    # 針對每一 row 取出指定的資料
    test2['tuple_'] = test2[['rank_' , 'cum_']].apply(tuple,axis = 1)
    test2['final'] = test2['tuple_'].map(lambda x:x[1][:int(x[0])])

    # final returen
    test3 = test2.final.map(lambda x:str.split(','.join(x) , ',') if list(x) else [])
    test2['final'] = test3.map(lambda x:list(set(x)))
    return test2[['merge_index','final']]



current_dir = os.path.dirname(os.path.abspath(__file__))
def model_reload_clustering(df_model ,
                            word_to_vec_model_ = f'{current_dir}/../inspiration_model/inspiration_word2vec_0423.model' ,
                            tfidf_model_ = f'{current_dir}/../inspiration_model/inspiration_tf_idf_modal_0423.model',
                           doc2vec_model_ = f'{current_dir}/../inspiration_model/inspiration_doc2vec_0423.model',
                            pca_model_ = f'{current_dir}/../inspiration_model/inspiration_pca_model_0423.joblib',
                           kmeans_model_ = f'{current_dir}/../inspiration_model/inspiration_kmeans_16_0424.pkl'):
    '''
    用訓練好的 model 得到組別
    '''
    # load word2vec 
    w2v_model = Word2Vec.load(word_to_vec_model_)
    wv = w2v_model.wv
    vs = 100
    
    # data -> corpus
    dictionary = corpora.Dictionary(df_model)
    corpus = [dictionary.doc2bow(text) for text in df_model]
    word_id = dictionary.token2id

    # load tfidf model 
    tfidf_model = TfidfModel.load(tfidf_model_)

    # transform tfidf 
    corpus_tfidf = [tfidf_model[doc] for doc in corpus]
    corpus_id_tfidf = list(map(dict, corpus_tfidf))
    df_tf_idf = pd.DataFrame(corpus_id_tfidf , index = df_model.index)

    word_id_r = {}
    for k,v in (word_id.items()):
        word_id_r[v] = k

    df_tf_idf.columns = df_tf_idf.columns.map(word_id_r)
    df_tf_idf = df_tf_idf.reset_index()
    
    # conbine word2vec and tfidf 
    text_vec = np.zeros((df_model.shape[0], vs))
    for ind, text in enumerate(df_model):
        wlen = len(text)
        vec = np.zeros((1, vs))
        for w in text:
            try:
                if word_id.get(w, False):
                    vec += (wv[w] * corpus_id_tfidf[ind][word_id[w]])
                else:
                    vec += wv[w]
            except:
                pass
        text_vec[ind] = vec/wlen
    tfidf = pd.DataFrame(data=text_vec , index = df_model.index)
    
    # prepare for doc2vec
    tagdoc = []
    for i , b in (df_model.items()):
        tagdoc.append(TaggedDocument(b,[i]))
    
    # doc2vec transform
    doc2vec_model = Doc2Vec.load(doc2vec_model_)
    dv = doc2vec_model.docvecs

    tagged_data = [TaggedDocument(words=words, tags=[str(i)]) for i, words in enumerate(df_model)]
    df_doc_vec = pd.DataFrame([doc2vec_model.infer_vector(doc.words) for doc in tagged_data])
    df_doc_vec.columns = 'doc' + df_doc_vec.columns.astype(str)
    df_doc_vec.index =df_model.index
    
    # combine all data 
    fit_doc = pd.concat([df_doc_vec,tfidf] , axis = 1)
    
    # load pca model
    pca = joblib.load(pca_model_)
    df_pca = pca.transform(np.array(fit_doc))
    df_pca = pd.DataFrame(df_pca , index = fit_doc.index)

    # load kmeans
    kmeans = joblib.load(kmeans_model_)
    kmeans_fit = kmeans.predict(df_pca)
    cluster_result_tfidf = pd.Series(kmeans_fit , index= df_pca.index)
    return cluster_result_tfidf

# for test
def model_path_test(word_to_vec_model_ = f'{current_dir}/../inspiration_model/inspiration_word2vec_0423.model' ,
                            tfidf_model_ = f'{current_dir}/../inspiration_model/inspiration_tf_idf_modal_0423.model',
                           doc2vec_model_ = f'{current_dir}/../inspiration_model/inspiration_doc2vec_0423.model',
                            pca_model_ = f'{current_dir}/../inspiration_model/inspiration_pca_model_0423.joblib',
                           kmeans_model_ = f'{current_dir}/../inspiration_model/inspiration_kmeans_16_0424.pkl'):

    w2v_model = Word2Vec.load(word_to_vec_model_)
    tfidf_model = TfidfModel.load(tfidf_model_)
    doc2vec_model = Doc2Vec.load(doc2vec_model_)
    pca = joblib.load(pca_model_)
    kmeans = joblib.load(kmeans_model_)

    print(w2v_model ,tfidf_model,doc2vec_model,pca,kmeans, 'test_success')
