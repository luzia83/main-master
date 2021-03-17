# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 19:56:11 2021

@author: luzi_
"""
#Librerias

from sklearn.datasets import make_blobs #libreria de mineria de datos
from sklearn.cluster import DBSCAN
from matplotlib import pyplot as plt
from sklearn.decomposition import PCA

import numpy as np

#INICIO DEL SCRIPT

X,ytrue = make_blobs(n_samples=1000,n_features=2,cluster_std=0.5, random_state=0)

plt.scatter(X[:,0],X[:,1],c=ytrue)

miModelo = DBSCAN(eps=0.5,min_samples=3)
miModelo.fit(X)
clusters = miModelo.labels_

#Vamos a pintar
    
plt.figure()
plt.subplot(1,2,1)
plt.scatter(X[:,0],X[:,1],c=ytrue,s=150)
plt.title('Color=ytrue')

plt.subplot(1,2,2)
plt.scatter(X[:,0],X[:,1],c=clusters,s=150)
plt.title('Color=clusters')

#Calidad del clustering

from sklearn.metrics import silhouette_score

