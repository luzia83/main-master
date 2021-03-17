# =============================================================================
# Librerias
# =============================================================================

from sklearn.datasets import make_blobs
from sklearn.cluster import DBSCAN
from matplotlib import pyplot as plt
from sklearn.decomposition import PCA
import numpy as np

# =============================================================================
# INICIO DEL SCRIPT
# =============================================================================

X, ytrue = make_blobs(n_samples=10000,n_features=2,centers=6,cluster_std=0.05,random_state=0)

#plt.scatter(X[:,0],X[:,1],c=y)


miModelo = DBSCAN(eps=0.5,min_samples=5)
miModelo.fit(X)
clusters = miModelo.labels_

plt.figure()
plt.subplot(1,2,1)
plt.scatter(X[:,0],X[:,1],c=ytrue,s=150)
plt.title('Color = ytrue')

plt.subplot(1,2,2)
plt.scatter(X[:,0],X[:,1],c=clusters,s=150)
plt.title('Color = clusters')

#ax = plt.gca()
#
#for i in range(len(X)):
#    circle = plt.Circle((X[i,0],X[i,1]),miModelo.eps, color='k', fill=False)
#    ax.add_patch(circle)

# =============================================================================
# Calidad del clustering
# =============================================================================

from sklearn.metrics import silhouette_score

sc = silhouette_score(X,clusters)


