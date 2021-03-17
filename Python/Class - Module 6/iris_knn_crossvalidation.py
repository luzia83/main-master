from sklearn.datasets import load_iris
import numpy as np
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.neighbors import KNeighborsClassifier

# =============================================================================
# KNN con 1 particion estratificadas de train/test, y con validacion cruzada
# =============================================================================

X, y = load_iris(return_X_y=True)

sss = StratifiedShuffleSplit(n_splits=1,test_size=0.3,random_state=13)

for train_index, test_index in sss.split(X, y):
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

params = {'n_neighbors':[3,5,7,9,11,13],'weights':['uniform','distance'],'p':[1,2,3,4]}

from sklearn.model_selection import GridSearchCV

miModelo = KNeighborsClassifier()

# =============================================================================
# https://scikit-learn.org/stable/modules/model_evaluation.html
# =============================================================================

gs = GridSearchCV(estimator=miModelo,param_grid=params,scoring='accuracy',cv=5)

gs.fit(X_train,y_train)
resultsCV = gs.cv_results_

clfBest = gs.best_estimator_
clfBest.fit(X_train,y_train)

y_pred = clfBest.predict(X_test)

from sklearn.metrics import accuracy_score

print(accuracy_score(y_test,y_pred))
