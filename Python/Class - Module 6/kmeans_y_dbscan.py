from sklearn.datasets import load_iris

data = load_iris()

X = data['data']
y = data['target']

from sklearn.decomposition import PCA

myPCA = PCA(n_components=2)
Xp = myPCA.fit_transform(X)

from sklearn.cluster import KMeans

myKMeans = KMeans(n_clusters=45,random_state=13)

myKMeans.fit(X)

from sklearn.cluster import DBSCAN

myDBSCAN = DBSCAN(eps=0.2)
myDBSCAN.fit(X)

print(myKMeans.labels_)

from matplotlib import pyplot as plt

plt.subplot(222)
plt.scatter(Xp[:,0],Xp[:,1],c=myKMeans.labels_)
plt.title('Proyeccion con clusters detectados por KMeans')

plt.subplot(221)
plt.scatter(Xp[:,0],Xp[:,1],c=y)
plt.title('Realidad del dataset')

plt.subplot(224)
plt.scatter(Xp[:,0],Xp[:,1],c=myDBSCAN.labels_)
plt.title('Proyeccion con clusters detectados por DBSCAN')

plt.subplot(223)
plt.scatter(Xp[:,0],Xp[:,1],c=y)
plt.title('Realidad del dataset')

from sklearn.metrics import silhouette_score

print("Silueta para KMeans: ",silhouette_score(X, myKMeans.labels_))
print("Silueta para DBSCAN: ",silhouette_score(X, myDBSCAN.labels_))
print("Silueta para realidad: ",silhouette_score(X, y))




