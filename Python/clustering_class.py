# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 20:17:41 2021

@author: luzi_
"""

from sklearn.datasets import load_iris #libreria de mineria de datos
from sklearn.cluster import KMeans
from matplotlib import pyplot as plt
from sklearn.decomposition import PCA
from sklearn import preprocessing
import numpy as np

X, ytrue = load_iris(return_X_y=True)

miModelo = KMeans(n_clusters=3, random_state=0)
miModelo.fit(X)
clusters = miModelo.labels_

X[:,0] = 400*X[:,0]
plt.boxplot(X)

#Vemos que hay que normalizar, asi que normalizamos

# minX = np.min(X,0)
# maxX = np.max(X,0)
# ncols = np.shape(X)[1]

# for col in range(np.shape(X)[1]):
#     X[:,col] = (X[:,col]-minX[col])/())


#Vamos a pintar
    
plt.figure()
plt.subplot(1,2,1)
plt.scatter(X[:,0],X[:,1],c=ytrue)
plt.title('Color=ytrue')

plt.subplot(1,2,2)
plt.scatter(X[:,0],X[:,1],c=clusters)
plt.title('Color=clusters')



#Estudiar rango de valores, si hay que normalizar se normaliza
#Buscar la manera de pintarlo en 3D

scaler = preprocessing.StandardScaler().fit(X)
X_scaled = scaler.transform(X)
miModelo_scaled = KMeans(n_clusters=3, random_state=0)
miModelo_scaled.fit(X_scaled)
clusters_scaled = miModelo_scaled.labels_
