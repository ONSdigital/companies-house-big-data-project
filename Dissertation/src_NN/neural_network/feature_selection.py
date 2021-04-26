import gcsfs
import os
import pandas as pd
import numpy as np
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2
#import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sklearn.preprocessing import LabelEncoder

from sklearn.ensemble import ExtraTreesClassifier

# Read in the data
df_raw = pd.read_csv("gs://basic_company_data/700_sample_per_class.csv")#("gs://basic_company_data/diss_basic_comp_data.csv")
df = df_raw.copy()
print(df.head())
print(df.columns.values)

df.isna().sum()
df = df.dropna(how='any')
df.info()

df['name'] = df['name'].astype('string')

labelEncoder = LabelEncoder()
df['CompanyCategory'] = labelEncoder.fit_transform(df['CompanyCategory'])
df['name'] = labelEncoder.fit_transform(df['name'])
df['_CompanyNumber'] = labelEncoder.fit_transform(df['_CompanyNumber'])

#df = df[['CompanyCategory', 'name', 'CompanyStatus', '_CompanyNumber',
#         'RegAddress_PostCode', 'UpdatedDate']]
df = df[['CompanyCategory', 'name', '_CompanyNumber']]
#df = df.drop(['RegAddress_PostCode', 'UpdatedDate'], axis=1)

#X = df.iloc[:, 1:3]
#y = df.iloc[:, 0]
df_t = df.transpose()
print(df_t)

#df_final = pd.concat([df, df_t], axis=1)

#X = df.iloc[:, 1:3]
#y = df.iloc[:, 0]

X = df_t.iloc[1, :]
y = df_t.iloc[0, :]

print(X)
print(y) # y is a column and lots of rows
print(X.shape)
print(y.shape)

"""
bestfeatures = SelectKBest(score_func=chi2, k=10).fit_transform(X, y)
fit = bestfeatures(X, y)
dfscores = pd.DataFrame(fit.scores_)
#dfcolumns = pd.DataFrame(X.columns)
dfrows = pd.DataFrame(X.rows)

featureScores = pd.concat([dfrows, dfscores], axis=0)
featureScores.columns = ['Specs', 'Score']
print(featureScores.nlargest(20, 'Score'))
"""

X = df.iloc[:, 1:3]
y = df.iloc[:, 0]

X1 = X.to_numpy()
y1 = y.to_numpy()

t_X1 = X1.transpose()
X1.reshape(-1, 1)

model = ExtraTreesClassifier()
model.fit(X1, y1)
print(model.feature_importances_)
feat_importances = pd.Series(model.feature_importances_, index=X.columns)
feat_importances.nlargest(20).plot(kind="barh")
plt.savefig("ETC_X1,y1.png")


# toward datascience feature selection technique
# toward datascience machine learning algorithms - kmeans clustering example
# datacamp.com kmeans clustering python