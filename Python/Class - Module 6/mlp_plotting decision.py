from sklearn.datasets import load_iris
from matplotlib import pyplot as plt
from sklearn.utils.multiclass import unique_labels
import numpy as np

data = load_iris()

X = data['data']
y = data['target']

def plot_confusion_matrix(y_true, y_pred, classes,
                          normalize=False,
                          title=None,
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    if not title:
        if normalize:
            title = 'Normalized confusion matrix'
        else:
            title = 'Confusion matrix, without normalization'

    # Compute confusion matrix
    cm = confusion_matrix(y_true, y_pred)
    # Only use the labels that appear in the data
    classes = classes[unique_labels(y_true, y_pred)]
    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        print("Normalized confusion matrix")
    else:
        print('Confusion matrix, without normalization')

    print(cm)

    fig, ax = plt.subplots()
    im = ax.imshow(cm, interpolation='nearest', cmap=cmap)
    ax.figure.colorbar(im, ax=ax)
    # We want to show all ticks...
    ax.set(xticks=np.arange(cm.shape[1]),
           yticks=np.arange(cm.shape[0]),
           # ... and label them with the respective list entries
           xticklabels=classes, yticklabels=classes,
           title=title,
           ylabel='True label',
           xlabel='Predicted label')

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
             rotation_mode="anchor")

    # Loop over data dimensions and create text annotations.
    fmt = '.2f' if normalize else 'd'
    thresh = cm.max() / 2.
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            ax.text(j, i, format(cm[i, j], fmt),
                    ha="center", va="center",
                    color="white" if cm[i, j] > thresh else "black")
    fig.tight_layout()
    return ax


from sklearn.model_selection import train_test_split, StratifiedKFold

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state = 0)

from sklearn.neural_network import MLPClassifier

param_grid = {'hidden_layer_sizes':[(100,),(10,),(10,10,10,),(5,10,20,10,5,)],'activation':['tanh','relu']}

from sklearn.model_selection import GridSearchCV

#miKNN = KNeighborsClassifier(n_neighbors=10, weights='uniform', metric='euclidean')

miKNN = MLPClassifier()
miGSCV = GridSearchCV(estimator=miKNN,param_grid=param_grid,scoring='accuracy',cv=StratifiedKFold(n_splits=10,random_state=3))

miGSCV.fit(X_train, y_train)

miGSCV.best_estimator_.fit(X_train, y_train)

y_pred = miGSCV.predict(X_test)

#print(str(100-(100*sum(y_pred!=y_test)/len(y_test))),'%')

from sklearn.metrics import confusion_matrix

print(confusion_matrix(y_test,y_pred))

plot_confusion_matrix(y_test, y_pred, classes=data['target_names'], normalize=True,
                      title='Javi es el amo del mundo')

#miKNN.fit(X_train,y_train)
#
#y_pred = miKNN.predict(X_test)
#
#print(str(100-(100*sum(y_pred!=y_test)/len(y_test))),'%')
