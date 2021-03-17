# =============================================================================
# Librerias
# =============================================================================

from sklearn.datasets import load_iris
from sklearn.cluster import DBSCAN
from matplotlib import pyplot as plt
from sklearn.decomposition import PCA
import numpy as np

# =============================================================================
# INICIO DEL SCRIPT
# =============================================================================

X, ytrue = load_iris(return_X_y=True)

# =============================================================================
# Asumo que solo voy a jugar con X, para ver si descubro ytrue
# =============================================================================

#miModelo = DBSCAN(eps=0.5,min_samples=5)
#miModelo.fit(X)
#clusters = miModelo.labels_


#plt.figure()
#plt.subplot(1,2,1)
#plt.scatter(X[:,0],X[:,1],c=ytrue)
#plt.title('Color = ytrue')
#
#plt.subplot(1,2,2)
#plt.scatter(X[:,0],X[:,1],c=clusters)
#plt.title('Color = clusters')

# =============================================================================
# Vamos a pintar reduciendo la dimensionalidad con PCA
# =============================================================================

miPCA = PCA(n_components=2)

Xtr = miPCA.fit_transform(X)
miModelo = DBSCAN(eps=0.1,min_samples=5)
miModelo.fit(Xtr)
clusters = miModelo.labels_

plt.figure()
plt.subplot(1,2,1)
plt.scatter(Xtr[:,0],Xtr[:,1],c=ytrue,s=150)
plt.title('Color = ytrue')

plt.subplot(1,2,2)
plt.scatter(Xtr[:,0],Xtr[:,1],c=clusters,s=150)
plt.title('Color = clusters')

ax = plt.gca()

for i in range(len(Xtr)):
    circle = plt.Circle((Xtr[i,0],Xtr[i,1]),miModelo.eps, color='k', fill=False)
    ax.add_patch(circle)

# =============================================================================
# Calidad del clustering
# =============================================================================

from sklearn.metrics import silhouette_score

sc = silhouette_score(Xtr,clusters)



