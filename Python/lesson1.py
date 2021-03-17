# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 20:03:56 2021

@author: luzi_
"""

#Importing required packages
import re
import os
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt 
#%matplotlib inline
import seaborn as sns
from IPython.core.display import display, HTML
from IPython.display import HTML
import json
import sys
sys.path.insert(0,'..')
import folium
from matplotlib.colors import Normalize, rgb2hex
import pymongo
from pymongo import MongoClient, GEO2D

# Data import from csv
total_crime = pd.read_csv('Map_of_Police_Department_Incidents.csv')
print(total_crime.shape)

total_crime.head(10)
total_crime.tail(10)

d_crime = total_crime.head(600000)


# Reduce dataset
#d_crime = total_crime.sample(frac=0.4)
print(d_crime.shape)

# Delete old dataframe with complete data
del total_crime

d_crime.dtypes
d_crime.index
d_crime.columns
d_crime.values

d_crime.dtypes

# Data cleaning. Transform Data from string to date type and delta date
date = pd.to_datetime(d_crime['Date'])

print(date.min())
print(date.max())

# Create a new colum "days" with timedelta format
t_delta=(date-date.min()).astype('timedelta64[D]')
d_crime['days']=t_delta
d_crime.head(1)

cat = 'Category'
l = d_crime.groupby(cat).size()

l.sort_values(inplace=True)
l.plot(kind='bar',fontsize=12,color='b',)
plt.xlabel('')
plt.ylabel('Number of reports',fontsize=8)

# Plotting bargraph
def plotdat(data,cat):
    l=data.groupby(cat).size()
    l.sort_values(inplace=True)
    fig=plt.figure(figsize=(10,5))
    plt.yticks(fontsize=8)
    l.plot(kind='bar',fontsize=12,color='b', )
    plt.xlabel('')
    plt.ylabel('Number of reports',fontsize=10)


plotdat(d_crime,'PdDistrict')
plotdat(d_crime,'Category')
plotdat(d_crime,'DayOfWeek')
plotdat(d_crime,'Descript')

l=d_crime.groupby('Descript').size()
l.sort_values()
print(l.shape)


# Heatmap and hierarchical clustering
def types_districts(d_crime,per):

    # Group by crime type and district
    hoods_per_type=d_crime.groupby('Descript').PdDistrict.value_counts(sort=True)
    t=hoods_per_type.unstack().fillna(0)
    
    # Sort by hood sum
    hood_sum=t.sum(axis=0)
    hood_sum.sort_values(ascending=False)
    t=t[hood_sum.index]
    
    # Filter by crime per district
    crime_sum=t.sum(axis=1)
    crime_sum.sort_values(ascending=False)
    
    # Large number, so let's slice the data.
    p=np.percentile(crime_sum,per)
    ix=crime_sum[crime_sum>p]
    t=t.loc[ix.index]
    return t

t=types_districts(d_crime,98)

sns.clustermap(t,cmap="mako", robust=True)

sns.clustermap(t,standard_scale=1,cmap="mako", robust=True)

sns.clustermap(z,t_score=0,cmap="viridis",robust=True)

#***********************************************************

print("Mongo version", pymongo.__version__)
client=MongoClient('localhost', 27017,username="dbatest", password="dPMd#Vg%lBpb", authSource ='test')
db = client.test
collection = db.crimesf

#Clean collection
collection.drop()

#Import data into the database. First, transform to JSON records
records = json.loads(d_crime.to_json(orient='records'))

collection.delete_many({})
collection.insert_many(records)

#Chack if we can access data from Mongodb
cursor = collection.find().sort('Category',pymongo.ASCENDING).limit(30)

for doc in cursor:
    print(doc)
    
# stablish a pipeline to select all rows matching attribute "Category" = "DRUG/NARCOTIC"
pipeline = [
{"$match": {"Category":"DRUG/NARCOTIC"}},
]

#Query the collection with the pipeline filter.
aggResult = collection.aggregate(pipeline)
df2 = pd.DataFrame(list(aggResult))
df2.head()

collection.find({'Category':"DRUG/NARCOTIC"}).count()

# Organize incidents' descriptions versus Districts where they were detected
def types_districts(d_crime,per):

    # Group by crime type and district
    hoods_per_type=d_crime.groupby('Descript').PdDistrict.value_counts(sort=True)
    t=hoods_per_type.unstack().fillna(0)
    
    # Sort by hood sum
    hood_sum=t.sum(axis=0)
    hood_sum.sort_values(ascending=False)
    t=t[hood_sum.index]
    
    # Filter by crime per district
    crime_sum=t.sum(axis=1)
    crime_sum.sort_values(ascending=False)
    
    # Large number, so let's slice the data.
    p=np.percentile(crime_sum,per)
    ix=crime_sum[crime_sum>p]
    t=t.loc[ix.index]
    return t

# Filter outliers up to 75 percentile

t=types_districts(df2,75)
sns.clustermap(t,standard_scale=1)
sns.clustermap(t,standard_scale=0, annot=True)

# Bin crime by 30 day window. That is, obtain new colum with corresponding months
df2['Month']=np.floor(df2['days']/30) # Approximate month (30 day window)

# Default
district='All'

def timeseries(dat,per):
    ''' Category grouped by month '''
    
    # Group by crime type and district
    cat_per_time=dat.groupby('Month').Descript.value_counts(sort=True)
    t=cat_per_time.unstack().fillna(0)
    
    # Filter by crime per district
    crime_sum=t.sum(axis=0)
    crime_sum.sort_values()
    
    # Large number, so let's slice the data.
    p=np.percentile(crime_sum,per)
    ix=crime_sum[crime_sum>p]
    t=t[ix.index]
    
    return t

t_all=timeseries(df2,0)

#Find incidents descriptions relatted to word pattern "BARBITUATES"
pat = re.compile(r'BARBITUATES', re.I)
pipeline = [
    {"$match": {"Category":"DRUG/NARCOTIC", 'Descript':{'$regex':pat} }},   
]

aggResult = collection.aggregate(pipeline)
df3 = pd.DataFrame(list(aggResult))
df3.head()

barbituates = df3.groupby('Descript').size()
s = pd.Series(barbituates)
print(s)
s = s[s != 1]
barbituate_features = list(s.index)
print(barbituate_features)

#Find inciden's descriptions related to word patter "BARBITUATES"
pat = re.compile(r'BARBITUATES', re.I)

pipeline = [
{"$match": {"Category":"DRUG/NARCOTIC" , 'Descript': {'$regex': pat}}},
]

aggResult = collection.aggregate(pipeline)
df3 = pd.DataFrame(list(aggResult))
df3.head()

barbituates = df3.groupby('Descript').size()
s = pd.Series(barbituates)
print(s)
s = s[s != 1]
barituate_features = list(s.index)
print(barituate_features)


#Let's generate a function to constructu subsets of descriptions according to patterns: COCAINE, MARIJUANA, METHADONE, etc.
def descriptionsAccordingToPattern(pattern):
    pat = re.compile(pattern, re.I)
    
    pipeline = [
    {"$match": {"Category":"DRUG/NARCOTIC" , 'Descript': {'$regex': pat}}},
    ]
    
    aggResult = collection.aggregate(pipeline)
    df3 = pd.DataFrame(list(aggResult))
    drug = df3.groupby('Descript').size()
    s = pd.Series(drug)
    s = s[s != 1] # filter those descriptions with value less equal 1
    features = list(s.index)
    
    return features

coke_features = descriptionsAccordingToPattern('COCAINE')

weed_features = descriptionsAccordingToPattern('MARIJUANA')
metadone_features = descriptionsAccordingToPattern('METHADONE')
hallu_features = descriptionsAccordingToPattern('HALLUCINOGENIC')
opium_features = descriptionsAccordingToPattern('OPIUM')
opiates_features = descriptionsAccordingToPattern('OPIATES')
meth_features = descriptionsAccordingToPattern('AMPHETAMINE')
heroin_features = descriptionsAccordingToPattern('HEROIN')
crack_features = descriptionsAccordingToPattern('BASE/ROCK')

# Lets use real dates for plotting
days_from_start=pd.Series(t_all.index*30).astype('timedelta64[D]')
dates_for_plot=date.min()+days_from_start
time_labels=dates_for_plot.map(lambda x: str(x.year)+'-'+str(x.month))

# Analytics per drug tipology according to descriptions
def drug_analysis(t,district,plot):
    t['BARBITUATES']=t[barituate_features].sum(axis=1)
    t['HEROIN']=t[heroin_features].sum(axis=1)
    t['HALLUCINOGENIC']=t[hallu_features].sum(axis=1)
    t['AMPHETAMINE']=t[meth_features].sum(axis=1)
    t['WEED']=t[weed_features].sum(axis=1)
    t['COKE']=t[coke_features].sum(axis=1)
    t['METHADONE']=t[metadone_features].sum(axis=1)
    t['CRACK']=t[crack_features].sum(axis=1)
    t['OPIUM']=t[opium_features].sum(axis=1)
    t['OPIATES']=t[opiates_features].sum(axis=1)
    
    drugs=t[['BARBITUATES','HEROIN','HALLUCINOGENIC','AMPHETAMINE','WEED','COKE','METHADONE','CRACK','OPIUM','OPIATES']]
    if plot:
        drugs.index=[int(i) for i in drugs.index]
    colors = plt.cm.jet(np.linspace(0, 1, drugs.shape[1]))
    drugs.plot(kind='bar', stacked=True, figsize=(20,5), color=colors, width=1, title=district,fontsize=6)
    return drugs


drug_df_all=drug_analysis(t_all,district,True)

def drug_analysis_rescale(t,district,plot):
    t['BARBITUATES']=t[barituate_features].sum(axis=1)
    t['HEROIN']=t[heroin_features].sum(axis=1)
    t['HALLUCINOGENIC']=t[hallu_features].sum(axis=1)
    t['AMPHETAMINE']=t[meth_features].sum(axis=1)
    t['WEED']=t[weed_features].sum(axis=1)
    t['COKE']=t[coke_features].sum(axis=1)
    t['METHADONE']=t[metadone_features].sum(axis=1)
    t['CRACK']=t[crack_features].sum(axis=1)
    t['OPIUM']=t[opium_features].sum(axis=1)
    t['OPIATES']=t[opiates_features].sum(axis=1)
    
    drugs=t[['BARBITUATES','HEROIN','HALLUCINOGENIC','AMPHETAMINE','WEED','COKE','METHADONE','CRACK','OPIUM','OPIATES']]
    if plot:
        drugs=drugs.div(drugs.sum(axis=1),axis=0)
    drugs.index=[int(i) for i in drugs.index]
    colors = plt.cm.GnBu(np.linspace(0, 1, drugs.shape[1]))
    colors = plt.cm.jet(np.linspace(0, 1, drugs.shape[1]))
    drugs.plot(kind='bar', stacked=True, figsize=(20,5), color=colors, width=1, title=district, legend=True)
    plt.ylim([0,1])
    return drugs

drug_df_all=drug_analysis_rescale(t_all,district,True)

dates_for_plot.index=dates_for_plot
sns.set_context(rc={"figure.figsize": (25.5,5.5)})

for d,c in zip(['AMPHETAMINE','CRACK','HEROIN','WEED'],['b','r','c','g']):
    plt.plot(dates_for_plot.index,drug_df_all[d],'o-',color=c,ms=6,mew=1.5,mec='white',linewidth=0.5,label=d,alpha=0.75)

plt.legend(loc='upper left',scatterpoints=1,prop={'size':8})

# filter outliers in 2016
dates_for_plot.index=dates_for_plot
sns.set_context(rc={"figure.figsize": (25.5,5.5)})
for d,c in zip(['AMPHETAMINE','CRACK','HEROIN','WEED'],['b','r','c','g']):
    plt.plot(dates_for_plot.head(155).index,drug_df_all[d].head(155),'o-',color=c,ms=6,mew=1.5,mec='white',linewidth=0.5,label=d,alpha=0.75)
plt.legend(loc='upper left',scatterpoints=1,prop={'size':8})

dates_for_plot.index=dates_for_plot
sns.set_context(rc={"figure.figsize": (25.5,5.5)})
for d,c in zip(['BARBITUATES','HALLUCINOGENIC','COKE','METHADONE'],['b','r','c','g']):
    plt.plot(dates_for_plot.head(155).index,drug_df_all[d].head(155),'o-',color=c,ms=6,mew=1.5,mec='white',linewidth=0.5,label=d,alpha=0.75)
plt.legend(loc='upper left',scatterpoints=1,prop={'size':8})

# Eliminate COKe as it has different range
dates_for_plot.index=dates_for_plot
sns.set_context(rc={"figure.figsize": (25.5,5.5)})
for d,c in zip(['BARBITUATES','HALLUCINOGENIC','METHADONE'],['b','r','g']):
    plt.plot(dates_for_plot.head(155).index,drug_df_all[d].head(155),'o-',color=c,ms=6,mew=1.5,mec='white',linewidth=0.5,label=d,alpha=0.75)
plt.legend(loc='upper left',scatterpoints=1,prop={'size':8})

dates_for_plot.index=dates_for_plot
sns.set_context(rc={"figure.figsize": (25.5,5.5)})
for d,c in zip(['METHADONE','CRACK','OPIUM','OPIATES'],['b','r','c','g']):
    plt.plot(dates_for_plot.head(155).index,drug_df_all[d].head(155),'o-',color=c,ms=6,mew=1.5,mec='white',linewidth=0.5,label=d,alpha=0.75)
plt.legend(loc='upper left',scatterpoints=1,prop={'size':8})

# Examine possible correlation between COKE and CRACK
dates_for_plot.index=dates_for_plot
sns.set_context(rc={"figure.figsize": (25.5,5.5)})
for d,c in zip(['CRACK','COKE'],['c','g']):
    plt.plot(dates_for_plot.head(155).index,drug_df_all[d].head(155),'o-',color=c,ms=6,mew=1.5,mec='white',linewidth=0.5,label=d,alpha=0.75)
plt.legend(loc='upper left',scatterpoints=1,prop={'size':8})

stor=[]
stor_time=[]

for d in set(d_crime['PdDistrict']):
    # Specify district and group by time
    dist_dat=df2[df2['PdDistrict']==d]
    t=timeseries(dist_dat,0)
    # Merge to ensure all categories are preserved!
    t_merge=pd.DataFrame(columns=t_all.columns)
    m=pd.concat([t_merge,t],axis=0).fillna(0)
    m.reset_index(inplace=True)
    # Plot
    drug_df=drug_analysis(m,d,True)
    plt.show()
    s=drug_df.sum(axis=0)
    stor=stor+[s]
    drug_df.columns=cols=[c+"_%s"%d for c in drug_df.columns]
    stor_time=stor_time+[drug_df]

drug_dat_time=pd.concat(stor_time,axis=1)
drug_dat=pd.concat(stor,axis=1)
drug_dat.columns=[list(set(d_crime['PdDistrict']))]

##We can also look at correlations between areas for different drugs.

sns.set_context(rc={"figure.figsize": (20,20)})
corr = drug_dat_time.corr()

# Generate a mask for the upper triangle
mask = np.zeros_like(corr, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True

# Set up the matplotlib figure
f, ax = plt.subplots(figsize=(19, 19))

# Generate a custom diverging colormap
sns.set_context(rc={"figure.figsize": (20,20)})
cmap = sns.diverging_palette(220, 10, as_cmap=True)

# Plot the correlation heatmap
sns.heatmap(corr, mask=mask, cmap=cmap, vmax=0.3, center=0, square=True, linewidths=.5, cbar_kws={"shrink": 0.5})


#With this in mind, we can examine select timeseries data.

drug_dat_time.index=dates_for_plot.head(159)
sns.set_context(rc={"figure.figsize": (7.5,5)})
for d,c in zip(['CRACK_MISSION','COKE_MISSION'],['b','r']):
    plt.plot(drug_dat_time.index,drug_dat_time[d],'o-',color=c,ms=6,mew=1,mec='white',linewidth=0.5,label=d,alpha=0.75)
plt.legend(loc='upper left',scatterpoints=1,prop={'size':8})


drug_dat_time.index=dates_for_plot.head(159)
sns.set_context(rc={"figure.figsize": (7.5,5)})
for d,c in zip(['CRACK_MISSION','CRACK_TENDERLOIN','CRACK_BAYVIEW','CRACK_SOUTHERN'],['b','r','g','c']):
    plt.plot(drug_dat_time.index,drug_dat_time[d],'o-',color=c,ms=6,mew=1,mec='white',linewidth=0.5,label=d,alpha=0.75)
plt.legend(loc='upper left',scatterpoints=1,prop={'size':8})

#We can now summarize this data using clustered heatmaps.
sns.clustermap(drug_dat,standard_scale=1,cmap="viridis",robust=True,annot=True)

#Let's isolate all crack-related records.

tmp=df2.copy()
tmp.set_index('Descript',inplace=True)

crack_dat=tmp.loc[crack_features]
crack_pts=crack_dat[['X','Y','Month']]


#Plot the crack regimes.

d=pd.DataFrame(crack_pts.groupby('Month').size())
d.index=dates_for_plot.head(157)
d.columns=['Count']

diff=len(d.index)-120

plt.plot(d.index,d['Count'],'o-',color='k',ms=6,mew=1,mec='white',linewidth=0.5,label=d,alpha=0.75)
plt.axvspan(d.index[40-diff],d.index[40],color='cyan',alpha=0.5)
plt.axvspan(d.index[80-diff],d.index[80],color='red',alpha=0.5)
plt.axvspan(d.index[120],d.index[-1],color='green',alpha=0.5)

oldest_crack_sums=d.loc[(d.index>d.index[40-diff]) & (d.index old_crack_sums=d.loc[(d.index>d.index[80-diff]) & (d.index new_crack_sums=d.loc[d.index>d.index[120]]

old_crack_sums['Count'].mean()/float(new_crack_sums['Count'].mean())

oldest_crack=crack_pts[(crack_pts['Month']>(40-diff)) & (crack_pts['Month']<40)]
oldest_crack.columns=['longitude','latitude','time']
old_crack=crack_pts[(crack_pts['Month']>(80-diff)) & (crack_pts['Month']<80)]
old_crack.columns=['longitude','latitude','time']
new_crack=crack_pts[crack_pts['Month']>120]
new_crack.columns=['longitude','latitude','time']

######################################################################################################################

# Let's create MongoBD collections to manage and query data
col1 = db.cracko
col2 = db.crackn
#Import data into the database
col1.drop()
col2.drop()

# Collection for new crack
data_json1 = json.loads(new_crack.to_json(orient='records'))
col1.delete_many({})
col1.insert_many(data_json1)

# Collection for old crack
data_json2 = json.loads(old_crack.to_json(orient='records'))
col2.delete_many({})
col2.insert_many(data_json2)

#Check if you can access the data from the MongoDB.
cursor = col1.find().sort('time',pymongo.ASCENDING).limit(3)
for doc in cursor:
    print(doc)

cursor2 = col2.find().sort('time',pymongo.ASCENDING).limit(10)
for doc in cursor2:
    print(doc)

# Create a new collection to store districts geo points
col3 = db.districts
col3.drop()

# mongoBD import from geojson file containing geopoints in form of multipopygons
os.system('"C:\\Program Files\\MongoDB\\Server\\4.2\\bin\\mongoimport" -d test -c districts --file Analysis_Neighborhoods.geojson')

cursor3 = col3.find().limit(1)
for doc in cursor3:
    print(doc)

# Get information about the index
col2.index_information()

col_temp = db.crackn2
cursor = col2.find()
for doc in cursor:
    col_temp.insert_one({
    "loc": {
    "type": "Point",
    "coordinates": [doc["longitude"], doc["latitude"]]
    }
    });

cursor = col_temp.find().limit(5)
for doc in cursor:
    print(doc)

cursor2 = col2.find_one()

query = {"features.geometry":
{ "$geoIntersects":
{ "$geometry":
{"type": "Point",
"coordinates": [cursor2["longitude"],cursor2["latitude"]]
}
}
}
}

cursor3 = col3.find_one(query)

# Generate new collection with features filteres
collection_features = db.feat
collection_features.delete_many({})
collection_features.insert_many(cursor3["features"])

cursor4 = collection_features.find_one()

# Spatial query implementing getoIntersects operation
query_feat = {"geometry":
{ "$geoIntersects":
{ "$geometry":
{"type": "Point",
"coordinates": [cursor2["longitude"],cursor2["latitude"]]
}
}
}
}

# Have a look if every thing is OK
for doc in collection_features.find(query_feat):
    print(doc)

cursor_feat = collection_features.find_one(query_feat)
print(cursor_feat["properties"])

##########################################################################


# Set general coordinates of San Francisco city
SF_COORDINATES = (37.76, -122.45) ## San Francisco Coordinates

MAX_RECORDS = 100

m = folium.Map(location=SF_COORDINATES, zoom_start=12)

#Display neighborhoods by polygons
geo_json_data = json.load(open('Analysis_Neighborhoods.geojson'))
folium.GeoJson(geo_json_data).add_to(m)

# Display Old Crack points (Red)
cursor = col1.find().limit(MAX_RECORDS)
for doc in cursor:
    folium.Marker(location = [doc["latitude"],doc["longitude"]],
    popup='Old Crack',
    icon=folium.Icon(color='red')).add_to(m)

# Display New Crack Points (Green)
cursor = col2.find().limit(MAX_RECORDS)
for doc in cursor:
folium.Marker(location = [doc["latitude"],doc["longitude"]],
º
icon=folium.Icon(color='green')).add_to(m)

# Display queried point in spatial query (Blue)
folium.Marker(location = [cursor2["latitude"],cursor2["longitude"]],
popup='Selected Point',
icon=folium.Icon(color='blue')).add_to(m)

folium.LayerControl().add_to(m)

m.save(outfile='mapa-de-crack.html')

map2 = folium.Map(location=SF_COORDINATES, zoom_start=12)

folium.GeoJson(
cursor_feat["geometry"],
name='Selected Neighborhood'
).add_to(map2)

folium.Marker(location = [cursor2["latitude"],cursor2["longitude"]],
popup='Selected Point',
icon=folium.Icon(color='blue')).add_to(map2)

folium.LayerControl().add_to(map2)
map2.save(outfile='map-intersect.html')

