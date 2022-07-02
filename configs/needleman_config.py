import pandas as pd
from os.path import join, split

çsv_path = join(split(__file__)[0], 'needleman_costs.csv')
df = pd.read_csv(çsv_path, index_col=0)

alpha = df.columns.tolist()

miss_cost = -0.9

