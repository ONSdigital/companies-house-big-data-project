import gcsfs
import os
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
#import seaborn as sns
import matplotlib
matplotlib.use('Agg')
#matplotlib.use('TKAgg')
import matplotlib.pyplot as plt
#matplotlib.use('GTK3Agg')

from sklearn.model_selection import train_test_split
#%matplotlib inline


# Read in the data
df_raw = pd.read_csv("gs://basic_company_data/diss_basic_comp_data.csv")
df = df_raw.copy()
print(df.head())
print(df.columns.values)

df.isna().sum()
df = df.dropna(how='any')
df.info()

df = df.drop(['UpdatedDate', 'CompanyStatus', 'RegAddress_PostCode'], axis=1)
df['name'] = df['name'].astype('category')



labelEncoder = LabelEncoder()
df['name'] = labelEncoder.fit_transform(df['name'])
df['CompanyCategory'] = labelEncoder.fit_transform(df['CompanyCategory'])

print(df.head())

train, test = train_test_split(df, test_size=0.2, random_state=8)
print(train.shape, test.shape)

test = test.drop('CompanyCategory', axis=1)
test

X = np.array(train.drop('CompanyCategory', axis=1))
y = np.array(train['CompanyCategory'])

scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X)
## Add more scaled data?

wcss = []

kmeans = KMeans(n_clusters=5, random_state=111) #the number of classes i.e. company categories
kmeans.fit(X_scaled)

label = kmeans.fit_predict(X_scaled)
print(label) #returns the array of cluster labels each data point belongs to


plt.scatter(X_scaled[:, 0], X_scaled[:, 1], c=label,
            s=50, cmap='viridis')
plt.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=300,
            c='yellow', label='centres')
#plt.legend()
#plt.interactive(False)
plt1 = plt.gcf()
plt1.savefig("clustering2.png")
#plt.show()
#plt.close()
