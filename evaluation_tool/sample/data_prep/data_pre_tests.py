from sample.method.hypothesis_testing import z_test, chi_square_test
from sample.method.other_function import data_processing_agg
import seaborn as sns
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
from scipy import stats 
from sample.method.other_function import _device_transform



class InvalidInputError(Exception):
    pass
class GroupNotMatchError(Exception):
    pass
class RatioListSumError(Exception):
    pass



## input type {df: dataframe , check_var_id_list: list[0,1,2], ratio_list: list[1:1:1] , device: list or str or all }
def srm_check(df,check_var_id_list,ratio_list, device = ['all'] ,significant_level = .01):
    ## 轉化 ratio type
    ratio_list_ = np.array(ratio_list)/sum(ratio_list)
    if len(list(filter(lambda x: x >= 100,check_var_id_list))) >0:
        raise InvalidInputError('Input var_id should < 100')

    #檢查輸入的 ratio_list 和 data 組別是否一樣
    if len(ratio_list)!=len(check_var_id_list):
        raise GroupNotMatchError('your data group :',df.var_id.unique())
    
    df = data_processing_agg(df , device) ## 轉化 raw_data - 1 個 user 1 筆
    device = _device_transform(device)
    result_dict = {}
    
    for d in device:
        if d == 'all':
            filtered_df = df.query('var_id.isin(@check_var_id_list)' ,engine = 'python')
        else:
            if 'device' not in df.columns: ## check df 有沒有 device 可以 query
                raise ValueError('your dataframe dont have device columns, you can only check "all"')
            filtered_df = df.query('var_id.isin(@check_var_id_list) & device == @d',engine = 'python')
        # Check Input group and dataframe group count
        if filtered_df['var_id'].nunique()!= len(check_var_id_list):
            raise GroupNotMatchError('your data group cnt：',filtered_df['var_id'].nunique())

        ratio_dict = dict(zip(check_var_id_list , ratio_list_))
        test_data = filtered_df.groupby('var_id').beacon_uid.nunique().rename('obs').to_frame()
        all_uu_cnt = test_data.obs.sum()
        test_data['expected'] = test_data.index.map(ratio_dict) * all_uu_cnt
        result = chi_square_test(test_data['obs'].tolist() , test_data['expected'].tolist() , significant_level)
        
        result_dict[f'{d}'] = result

    return(result_dict)





## input type {df: dataframe , check_var_id_list: list[0,1,2], metric_list: list[m1,m2] , device: list or str or all }
## merge aa 用來合併 a<>aa
def metric_test(df,check_var_id_list ,metrics_list = ['avg_view_item','avg_duration','e2'] , device = ['all'] , significant_level = .01 , merge_aa = False):
    if len(check_var_id_list)>2:
        raise InvalidInputError('僅能進行 2 組的檢定')
    df = data_processing_agg(df , device , metrics_list)
    device = _device_transform(device)
    result_dict = {}
    if merge_aa:
        if 0 in check_var_id_list:
            raise InvalidInputError('Merge aa var_id cant be 0')
        df['var_id'] = df.var_id.replace({0:1})
    for d in device:
        if d != "all":
            filtered_df = df.query('device == @d' , engine = 'python')
        else:
            filtered_df = df
        for metric in metrics_list:
            group1 = filtered_df.query(f'var_id == {check_var_id_list[0]}' , engine = 'python')[metric]
            group2 = filtered_df.query(f'var_id == {check_var_id_list[1]}' , engine = 'python')[metric]
            result = z_test(group1 , group2 , significant_level)
            result_dict[f'{d}_{metric}'] = result ##儲存測試結果
    return result_dict
        


## 不會用到，先保留
def _split_data(df, metrics = None):  #private function : split raw data by metrics, if metrics is none keep only var_id, device, beacon_uid
    if metrics == None:
        split_df = df[['var_id', 'device' , 'beacon_uid']]
    else:
        split_df = df[['var_id', 'device', metrics]]

    split = {}
    for i in df['var_id'].unique().tolist():
        if(i==0):
            split['A'] = split_df[split_df['var_id']==i]
        elif(i==1):
            split['AA'] = split_df[split_df['var_id']==i]
        elif(i==2):
            split['B'] = split_df[split_df['var_id']==i]
        else:
            split[i] = split_df[split_df['var_id']==i]
    return split



## 不會用到，先保留
def perform_A_AAtest(df, device = 'all' , significant_level = .01): #分 device 
    
    A_AA_metrics = ['avg_view_item','avg_duration','e2']
    device = _device_transform(device)
    for d in device:
        not_pass_cnt=0
        if d == 'all':
            print('\n AA Test for ALL')
            A_data = _split_data(metric)['A']
            AA_data = _split_data(metric)['AA']
        else:
            print('\n AA Test for :',d)
            A_data = _split_data(metric)['A'].query('device == @d',engine = 'python')
            AA_data = _split_data(metric)['AA'].query('device == @d',engine = 'python')

        for metric in A_AA_metrics:
            a_values = A_data[metric].dropna().astype(int)
            aa_values = AA_data[metric].dropna().astype(int)
            result = z_test(a_values, aa_values , significant_level)
            
            if result['p_value'] < significant_level : 
                print('[',metric,'] AA test result :','AA test FAILED')
                not_pass_cnt+=1
            else: 
                print('[',metric,'] AA test result :','AA test PASS')
        print('-------------------------------------------------------')
        if(not_pass_cnt==0):
            a = 'All default metrics PASS Test'
        else:
            a = str(not_pass_cnt)+' default metrics NOT PASS Test'
    return a




## 常態性檢測
class normality_test:
    def __init__(self,group1 , group2 ,num_resamples = 1000):
        self.group1 = pd.Series(group1)
        self.group2 = pd.Series(group2)
        self.num_resamples = num_resamples
        self.combined_data = pd.concat([self.group1 , self.group2]) ## 將 a/b 的樣本合併，模擬零分布
        self.resamples = [np.random.choice(self.combined_data, len(self.combined_data), replace=True) for i in range(self.num_resamples)] ## 生成 1000 組放回抽樣的零分布樣本 boostrap
        self.resampled_group1 = [self.resample[:len(self.group1)] for self.resample in self.resamples] ##將其均分成兩組
        self.resampled_group2 = [self.resample[len(self.group1):] for self.resample in self.resamples]
        self.test_statistics = [z_test(self.resampled_group1[i], self.resampled_group2[i])[0] for i in range(self.num_resamples)] ## 計算 1000 組 a/b 的 z score
        self.statistics = [np.mean(self.resampled_group1[i]) - np.mean(self.resampled_group2[i]) for i in range(self.num_resamples)] ## 另一種方式，直接計算平均差異，就是原始統計量
   
    def plot_distribution(self , test_type = 'test_statistics'): ## 畫 qqplot
        if test_type == "test_statistics":
            self.fig,self.ax = plt.subplots(ncols=2 , figsize = (14,6))
            sns.kdeplot(self.test_statistics , ax = self.ax[0])
            ww = pd.Series(self.test_statistics)
            sm.qqplot((ww - (ww.mean())) / ww.std(),ax = self.ax[1] , line = '45')
            sns.despine(ax = self.ax[0])
            sns.despine(ax = self.ax[1])
        elif test_type == "statistics":
            self.fig,self.ax = plt.subplots(ncols=2 , figsize = (14,6))
            sns.kdeplot(self.statistics , ax = self.ax[0])
            ww = pd.Series(self.statistics)
            sm.qqplot((ww - (ww.mean())) / ww.std() ,ax = self.ax[1] , line = '45')
            sns.despine(ax = self.ax[0])
            sns.despine(ax = self.ax[1])

        
    def shapiro_wilk_test(self , test_type = 'test_statistics'):
        if test_type == "test_statistics":
            temp = stats.shapiro(self.test_statistics)
        elif test_type == "statistics":
            temp = stats.shapiro(self.statistics)
        return temp



