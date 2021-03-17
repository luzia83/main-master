from sklearn.datasets import load_digits
import numpy as np
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.tree import DecisionTreeClassifier

# =============================================================================
# KNN con 1 particion estratificadas de train/test, y con validacion cruzada
# =============================================================================

X, y = load_digits(return_X_y=True)

#x_plot = np.reshape(X[4,:],(8,8))
#from matplotlib import pyplot as plt
#plt.imshow(x_plot)

sss = StratifiedShuffleSplit(n_splits=1,test_size=0.3,random_state=13)

for train_index, test_index in sss.split(X, y):
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

params = {'criterion':["gini", "entropy"],
          'max_depth':[3],
          'min_samples_split':[2,4,8,16]}

from sklearn.model_selection import GridSearchCV

miModelo = DecisionTreeClassifier()

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

from sklearn.metrics import plot_confusion_matrix

plot_confusion_matrix(clfBest,X_test,y_test)

from sklearn.tree import plot_tree

plot_tree(clfBest)