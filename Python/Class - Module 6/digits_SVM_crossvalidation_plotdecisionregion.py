from sklearn.datasets import load_digits
import numpy as np
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.svm import SVC

# =============================================================================
# SVC con 1 particion estratificadas de train/test, y con validacion cruzada
# =============================================================================

X, y = load_digits(return_X_y=True)

from matplotlib import pyplot as plt
from sklearn.decomposition import PCA

miPCA = PCA(n_components=2)
X = miPCA.fit_transform(X)

#plt.scatter(X[:,0],X[:,1],c=y)

sss = StratifiedShuffleSplit(n_splits=1,test_size=0.3,random_state=13)

for train_index, test_index in sss.split(X, y):
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

params = {'C':[0.1,1,10],
          'kernel':['rbf'],
#          'degree':[2,3,4],
          'gamma':['scale', 'auto', 0.1],
          'coef0':[0.0]}

from sklearn.model_selection import GridSearchCV

miModelo = SVC()

# =============================================================================
# https://scikit-learn.org/stable/modules/model_evaluation.html
# =============================================================================

gs = GridSearchCV(estimator=miModelo,param_grid=params,scoring='accuracy',cv=5,verbose=2)

gs.fit(X_train,y_train)
resultsCV = gs.cv_results_

clfBest = gs.best_estimator_
clfBest.fit(X_train,y_train)

y_pred = clfBest.predict(X_test)

from sklearn.metrics import accuracy_score

print(accuracy_score(y_test,y_pred))

#from sklearn.metrics import plot_confusion_matrix
#
#plot_confusion_matrix(clfBest,X_test,y_test)


plt.figure()
h=100

x_min, x_max = X[:, 0].min(), X[:, 0].max()
y_min, y_max = X[:, 1].min(), X[:, 1].max()
xx, yy = np.meshgrid(np.linspace(x_min, x_max, h),
                     np.linspace(y_min, y_max, h))

Xgrid = np.c_[xx.ravel(), yy.ravel()]
Z = clfBest.predict(Xgrid)

# Put the result into a color plot


#plt.scatter(Xgrid[:,0],Xgrid[:,1],c=Z)

Z = Z.reshape(xx.shape)
plt.contourf(xx, yy, Z, alpha=0.8)
plt.scatter(X[:,0],X[:,1],c=y)











