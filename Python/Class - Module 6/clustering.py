# =============================================================================
# Librerias
# =============================================================================

from sklearn.datasets import load_iris
from sklearn.cluster import KMeans
from matplotlib import pyplot as plt
from sklearn.decomposition import PCA

# =============================================================================
# INICIO DEL SCRIPT
# =============================================================================

X, ytrue = load_iris(return_X_y=True)

# =============================================================================
# Asumo que solo voy a jugar con X, para ver si descubro ytrue
# =============================================================================

miModelo = KMeans(n_clusters=3,random_state=0)
miModelo.fit(X)
clusters = miModelo.labels_

# =============================================================================
# Queremos pintar
# =============================================================================

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

plt.figure()
plt.subplot(1,2,1)
plt.scatter(Xtr[:,0],Xtr[:,1],c=ytrue)
plt.title('Color = ytrue')

plt.subplot(1,2,2)
plt.scatter(Xtr[:,0],Xtr[:,1],c=clusters)
plt.title('Color = clusters')



