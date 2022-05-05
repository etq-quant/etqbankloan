from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.metrics import classification_report, confusion_matrix
import scipy.stats as stats
import numpy as np

def anova_test(fdf, numerical_cols):
    '''
    Examples
    --------
    nume_features = anova_test(fdf, x_num)
    '''
    nume_features = []
    
    for i in numerical_cols:
        fvalue, pvalue = stats.f_oneway(fdf.query('label == 1')[i], fdf.query('label == 0')[i])
        if pvalue < 0.05:
            print(i, round(pvalue, 2))
            nume_features.append(i)
    return nume_features

def cm(y_test, y_pred, target_names=None):
    cm = confusion_matrix(y_test, y_pred)
    print(cm)
    print(classification_report(y_pred, y_test, target_names=target_names))
    
def classification(X, y, clf_model, test_size=0.3, kfold=10, pred_method=None, target_names=None):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size)
    
    clf_model.fit(X_train.values, y_train)

    cv = KFold(n_splits=kfold, random_state=1, shuffle=True)
    scores = cross_val_score(clf_model, X, y, scoring='accuracy', cv=cv, n_jobs=-1)
    print('Accuracy: %.3f (%.3f)' % (np.mean(scores), np.std(scores)))

    if pred_method:
        y_pred = pred_method(clf_model, X_test.values)
    else:
        y_pred = clf_model.predict(X_test.values)
    cm(y_test, y_pred, target_names)
    return clf_model, y_pred