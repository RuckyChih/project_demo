import os
print(os.getcwd())

import pandas as pd
# params_type = pd.read_csv('docs/params.csv').drop_duplicates(subset='param',keep = False )
# print(params_type)
from sample.data_prep.data_extract import Dataset
