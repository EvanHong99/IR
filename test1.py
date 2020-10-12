a=[1,3,2]
a.insert(len(a), 8)
# a.sort()
print(a)
import pandas as pd
b=pd.Series(a)
b[-1]=-1
print(b.sort_values()[:2].values)