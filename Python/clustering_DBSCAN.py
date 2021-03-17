# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 19:06:05 2021

@author: luzi_
"""

from sklearn.datasets import load_iris #libreria de mineria de datos
from sklearn.cluster import DBSCAN
from matplotlib import pyplot as plt
from sklearn.decomposition import PCA
from sklearn import preprocessing
import numpy as np

X, ytrue = load_iris(return_X_y=True)

miModelo = DBSCAN(eps=0.5,min_samples=3)
miModelo.fit(X)
clusters = miModelo.labels_

miPCA = PCA(n_components=2)

Xtr = miPCA.fit_transform(X)

#Vamos a pintar
    
plt.figure()
plt.subplot(1,2,1)
plt.scatter(Xtr[:,0],Xtr[:,1],c=ytrue)
plt.title('Color=ytrue')

plt.subplot(1,2,2)
plt.scatter(Xtr[:,0],Xtr[:,1],c=clusters)
plt.title('Color=clusters')

ax = plt.gca()

for i in range(len(Xtr)):
    circle= plt.Circle((Xtr[i,0],Xtr[i,1]),miModelo.eps,color='k',fill=False)
    ax.add_patch(circle)
    
#Calidad del clustering

from sklearn.metrics import silhouette_score
