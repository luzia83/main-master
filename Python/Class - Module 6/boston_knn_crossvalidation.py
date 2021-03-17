from sklearn.datasets import load_boston
import numpy as np
from sklearn.model_selection import ShuffleSplit
from sklearn.neighbors import KNeighborsRegressor

# =============================================================================
# KNN con 1 particion estratificadas de train/test, y con validacion cruzada
# =============================================================================

X, y = load_boston(return_X_y=True)

sss = ShuffleSplit(n_splits=1,test_size=0.1,random_state=13)

for train_index, test_index in sss.split(X, y):
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

params = {'n_neighbors':[3,5,7,9,11,13],'weights':['uniform','distance'],'p':[1,2,3,4]}

from sklearn.model_selection import GridSearchCV

miModelo = KNeighborsRegressor()

# =============================================================================
# https://scikit-learn.org/stable/modules/model_evaluation.html
# =============================================================================

gs = GridSearchCV(estimator=miModelo,param_grid=params,scoring='r2',cv=5)

gs.fit(X_train,y_train)
resultsCV = gs.cv_results_

clfBest = gs.best_estimator_
clfBest.fit(X_train,y_train)

y_pred = clfBest.predict(X_test)

from sklearn.metrics import r2_score

print(r2_score(y_test,y_pred))

import matplotlib.pyplot as plt

plt.scatter(y_test,y_pred,s=60)
plt.plot([min(y_test),max(y_test)],[min(y_test),max(y_test)],'r')
plt.xlabel('ytest')
plt.ylabel('ypred')
