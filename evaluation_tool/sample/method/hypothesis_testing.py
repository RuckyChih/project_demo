import numpy as np
from scipy import stats 


def check_equal_variance(sample1, sample2 ,alpha = 0.05 ):
    ## 檢查變異數是否有同質性
    _, p_value = stats.levene(sample1, sample2)
    return p_value >= alpha


def t_test(sample1, sample2 , significant_level = .05):
    equal_var = check_equal_variance(sample1, sample2) ## 檢查變異數同值性
    t_stat, p_value = stats.ttest_ind(sample1, sample2, equal_var=equal_var)
    is_significant = p_value < significant_level
    return {'t_stat':t_stat, 'p_value':p_value, 'is_significant':is_significant}

def z_test( sample1, sample2 , significant_level = .05):
    sample1_mean = np.mean(sample1)
    sample2_mean = np.mean(sample2)
    sample1_std = np.std(sample1, ddof=1)  # ddof=1 for sample standard deviation
    sample2_std = np.std(sample2, ddof=1)
    n1 = len(sample1)
    n2 = len(sample2)
    # Calculate z-score
    se = np.sqrt((sample1_std**2 / n1) + (sample2_std**2 / n2))
    z_stat = (sample2_mean - sample1_mean) / np.sqrt((sample1_std**2 / n1) + (sample2_std**2 / n2))
    # Calculate p-value
    p_value = 2 * (1 - stats.norm.cdf(abs(z_stat)))
    diff_ = sample2_mean - sample1_mean
    is_significant = p_value < significant_level
    return {'group_1_mean':sample1_mean ,'group_2_mean':sample2_mean  ,'mean_diff' : diff_,'z_stat':z_stat, 'p_value':p_value, 'is_significant':is_significant , 'se':se}

def chi_square_test( observed, expected=None , significant_level = .05):
    chi2_stat, p_value = stats.chisquare(observed, f_exp=expected)
    is_significant = p_value < significant_level
    return {'chi2_stat':chi2_stat, 'p_value':p_value, 'is_significant':is_significant}