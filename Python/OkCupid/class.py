# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 19:53:27 2021

@author: luzi_
"""

from IPython.display import display,HTML
import pandas as pd
import seaborn as sns
from scipy.stats import kendalltau
import numpy as np
import math
import matplotlib.pyplot as plt
from prettypandas import PrettyPandas
sns.set(style="ticks")
sns.set_context(context="notebook",font_scale=1)
import string
import tqdm # a cool progress bar
import re
import json
import pymongo
from pymongo import MongoClient

#############################################

d = pd.read_csv("profiles.csv")
print("The database contains {} records".format(len(d)))

print("Mongo version", pymongo.__version__)
client = MongoClient('localhost', 27017,username="dbatest", password="dPMd#Vg%lBpb", authSource ='test')
db= client.test
collection=db.okcupid

#Import data into the database
collection.drop()

# Transform dataframe to Json and store in MongoDB
records = json.loads(d.to_json(orient='records'))
collection.delete_many({})
collection.insert_many(records)

#Check if you can access the data from the MongoDB.
cursor = collection.find().sort('sex',pymongo.ASCENDING).limit(10)
for doc in cursor:
    print(doc)

pipeline = [
{"$match": {"sex":"m"}},
]

aggResult = collection.aggregate(pipeline)
male = pd.DataFrame(list(aggResult))
male.head()


pipeline = [
{"$match": {"sex":"f"}},
]

aggResult = collection.aggregate(pipeline)
female = pd.DataFrame(list(aggResult))
female.head(2)

print("{} males ({:.1%}), {} females ({:.1%})".format(
len(male),len(male)/len(d),
len(female),len(female)/len(d)))

# Ignore columns with "essay" in the name (they are long)
PrettyPandas(d # Prettyprints pandas dataframes
.head(10) # Sample the first 10 rows
[[c for c in d.columns if "essay" not in c]]) # Ignore columns with "essay" in the name (they are long)


d["age"].describe()

print("Age statistics:\n{}".format(d["age"].describe()))
print()
print("There are {} users older than 80".format((d["age"]>80).sum()))

collection.find({"age":{ "$gt": 80 }}).count()


##Let's assume the 110-year-old lady and the athletic 109-year-old gentleman (who's working on a masters program) are outliers: we get rid of them so the following plots look better. They didn't say much else about themselves, anyway.
##We then remove them
collection.delete_many({"age":{ "$gt": 80 }})
collection.find({"age":{ "$gt": 80 }}).count()
print("The dataset now contains {} records".format(collection.find({"age":{ "$lt": 80 }}).count()))

cursor = collection.find().sort('sex',pymongo.ASCENDING).limit(10)
for doc in cursor:
    print(doc)

PrettyPandas(d[d["age"]>80])

# Isolate male's dataset
aggResult = collection.aggregate([{"$match": {"sex":"m"}}])
male = pd.DataFrame(list(aggResult))

# Isolate female's dataset
aggResult = collection.aggregate([{"$match": {"sex":"f"}}])
female = pd.DataFrame(list(aggResult))

print("{} males ({:.1%}), {} females ({:.1%})".format(
len(male),len(male)/len(d),
len(female),len(female)/len(d)))

# Transform dataframe to Json and store in MongoDB
records = json.loads(d.to_json(orient='records'))
collection.delete_many({})
collection.insert_many(records)

#Check if you can access the data from the MongoDB.
cursor = collection.find().sort('sex',pymongo.ASCENDING).limit(10)
for doc in cursor:
print(doc)

pipeline = [
{"$match": {"sex":"m"}},
]

aggResult = collection.aggregate(pipeline)
male = pd.DataFrame(list(aggResult))
male.head()


pipeline = [
{"$match": {"sex":"f"}},
]

aggResult = collection.aggregate(pipeline)
female = pd.DataFrame(list(aggResult))
female.head(2)

print("{} males ({:.1%}), {} females ({:.1%})".format(
len(male),len(male)/len(d),
len(female),len(female)/len(d)))

# Ignore columns with "essay" in the name (they are long)
PrettyPandas(d # Prettyprints pandas dataframes
.head(10) # Sample the first 10 rows
[[c for c in d.columns if "essay" not in c]]) # Ignore columns with "essay" in the name (they are long)


d["age"].describe()

print("Age statistics:\n{}".format(d["age"].describe()))
print()
print("There are {} users older than 80".format((d["age"]>80).sum()))

collection.find({"age":{ "$gt": 80 }}).count()


##Let's assume the 110-year-old lady and the athletic 109-year-old gentleman (who's working on a masters program) are outliers: we get rid of them so the following plots look better. They didn't say much else about themselves, anyway.
##We then remove them
collection.delete_many({"age":{ "$gt": 80 }})
collection.find({"age":{ "$gt": 80 }}).count()
print("The dataset now contains {} records".format(collection.find({"age":{ "$lt": 80 }}).count()))

cursor = collection.find().sort('sex',pymongo.ASCENDING).limit(10)
for doc in cursor:
    print(doc)

PrettyPandas(d[d["age"]>80])

# Isolate male's dataset
aggResult = collection.aggregate([{"$match": {"sex":"m"}}])
male = pd.DataFrame(list(aggResult))

# Isolate female's dataset
aggResult = collection.aggregate([{"$match": {"sex":"f"}}])
female = pd.DataFrame(list(aggResult))

print("{} males ({:.1%}), {} females ({:.1%})".format(
len(male),len(male)/len(d),
len(female),len(female)/len(d)))

d=pd.DataFrame(list(collection.find()))

print("Age statistics:\n{}".format(d["age"].describe()))
print()
print("There are {} users older than 80".format((d["age"]>80).sum()))


###################################################

fig,(ax1,ax2) = plt.subplots(ncols=2,figsize=(10,3),sharey=True,sharex=True)
sns.distplot(male["age"], ax=ax1,
bins=range(d["age"].min(),d["age"].max()),
kde=False,
color="g")
ax1.set_title("Age distribution for males")
sns.distplot(female["age"], ax=ax2,
bins=range(d["age"].min(),d["age"].max()),
kde=False,
color="b")
ax2.set_title("Age distribution for females")
ax1.set_ylabel("Number of users in age group")
for ax in (ax1,ax2):
sns.despine(ax=ax)
fig.tight_layout()

print("Mean and median age for males: {:.2f}, {:.2f}".format(male["age"].mean(),male["age"].median()))
print("Mean and median age for females: {:.2f}, {:.2f}".format(female["age"].mean(),female["age"].median()))

##############################################################

fig,(ax1,ax2) = plt.subplots(nrows=2,figsize=(10,6),sharex=True)
# Plot the age distributions of males and females on the same axis
sns.distplot(male["age"], ax=ax1,
bins=range(d["age"].min(),d["age"].max()),
kde=False,
color="g",
label="males")
sns.distplot(female["age"], ax=ax1,
bins=range(d["age"].min(),d["age"].max()),
kde=False,
color="b",
label="females")
ax1.set_ylabel("Number of users in age group")
ax1.set_xlabel("")
ax1.legend()

# Compute the fraction of males for every age value
fraction_of_males=(male["age"].value_counts()/d["age"].value_counts())
# Ignore values computed from age groups in which we have less than 100 total users (else estimates are too unstable)
fraction_of_males[d["age"].value_counts()<100]=None
barlist=ax2.bar(x=fraction_of_males.index,
height=fraction_of_males*100-50,
bottom=50, width=1, color="gray")
for bar,frac in zip(barlist,fraction_of_males):
bar.set_color("g" if frac>.5 else "b")
bar.set_alpha(0.4)
ax2.set_xlim([18,70])
ax2.set_xlabel("age")
ax2.set_ylabel("percentage of males in age group")
ax2.axhline(y=50,color="k")

for ax in (ax1,ax2):
sns.despine(ax=ax)
fig.tight_layout()


