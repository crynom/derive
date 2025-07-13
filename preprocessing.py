import pandas as pd, numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split


def cut(stdVal: float, subDivision: int = 4) -> float: return (np.floor(stdVal * subDivision) if stdVal > 0 else np.ceil(stdVal * subDivision)) / subDivision

def readData(path: str='./history.xlsx', shortInterval: int=4, longInterval: int=24, subDivision: int=4):
    history = pd.read_excel(path)

    history['futureLogReturn'] = np.log(history.close_price.shift(-1) / history.close_price)
    history['logReturn'] = np.log(history.close_price / history.open_price)

    for i in range(len(history)):
        history.loc[i, 'longSMA'] = history.lastPrice.iloc[i-longInterval:i].mean()
        history.loc[i, 'longStd'] = history.lastPrice.iloc[i-longInterval:i].std()
        history.loc[i, 'shortSMA'] = history.lastPrice.iloc[i-shortInterval:i].mean()
        history.loc[i, 'shortStd'] = history.lastPrice.iloc[i-shortInterval:i].std()

    features = history.dropna(how='any', axis=0)\
    .drop(columns=['normalizedTs', 'datetime'])\
    .reset_index(drop=True)

    # creation of the label df
    features['stdLabel'] = features.futureLogReturn.apply(lambda log: log / features.futureLogReturn.std())
    features['logBin'] = features.stdLabel.apply(cut, args=(subDivision,))
    labelDf = features[['futureLogReturn', 'stdLabel', 'logBin']]
    labels = pd.get_dummies(labelDf.logBin)
    features.drop(columns=labelDf.columns, inplace=True)

    featuresTrain, featuresTest, labelsTrain, labelsTest = train_test_split(features, labels, test_size=.25)

    ct = ColumnTransformer([('only numeric', StandardScaler(), features.columns)], remainder='passthrough')
    featuresTrain, featuresTest = ct.fit_transform(featuresTrain), ct.fit_transform(featuresTest)
    featuresAll = ct.fit_transform(features)

    dataDict = {
        'history': history,
        'features': features,
        'labelDf': labelDf,
        'labels': labels,
        'featuresAll': featuresAll
    }

    return featuresTrain, featuresTest, labelsTrain, labelsTest, dataDict
    
