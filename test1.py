import pandas as pd
temp = pd.Series([7,5,6]).sort_values()[:2].index
print(temp)