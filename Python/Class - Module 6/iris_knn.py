from sklearn.datasets import load_iris
import numpy as np
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.neighbors import KNeighborsClassifier

# =============================================================================
# KNN con multiples particiones estratificadas de train/test
# =============================================================================

X, y = load_iris(return_X_y=True)

sss = StratifiedShuffleSplit(n_splits=10,test_size=0.1,random_state=0)
n_vecinos = 1
accuracies = []

for train_index, test_index in sss.split(X, y):
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]
    
    clf = KNeighborsClassifier(n_neighbors=n_vecinos)
    clf.fit(X_train,y_train)
    y_pred = clf.predict(X_test)
    accuracies.append(100.*sum(y_pred==y_test)/len(y_test))
    
print("Acierto promedio:",str(np.mean(accuracies)),'%')

