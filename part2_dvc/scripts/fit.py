# scripts/fit.py

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from catboost import CatBoostRegressor
import yaml
import os
import joblib
import logging

# обучение модели
def fit_model():
    # Прочитайте файл с гиперпараметрами params.yamlimport yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd) 

    # загрузите результат предыдущего шага: inital_data.csv
    data = pd.read_csv('data/initial_data.csv')
    
    # features
    bool_features = data.select_dtypes(include='bool')
    potential_binary_features = bool_features.nunique() == 2
    binary_bool_features = bool_features[potential_binary_features[potential_binary_features].index]
    logging.info(f"Selected binary features: {binary_bool_features.columns.tolist()}")

    num_features = data.select_dtypes(['float', 'int']).columns.tolist()
    num_features = [col for col in num_features if col not in [params['target_col']]]
    logging.info(f"Selected numeric features: {num_features}")

    cat_features = ['building_type_int']
    logging.info(f"Selected categorical features: {cat_features}")

    preprocessor = ColumnTransformer(
        [
            ('binary', OneHotEncoder(drop=params['binary_drop'], sparse_output=False), binary_bool_features.columns.tolist()),
            ('cat', OneHotEncoder(drop=params['cat_drop'], sparse_output=False), cat_features),
            ('num', StandardScaler(), num_features)
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    model = CatBoostRegressor(verbose=100)

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )

    pipeline.fit(data, data[params['target_col']]) 

    # сохраните обученную модель в models/fitted_model.pkl
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd)


if __name__ == '__main__':
	fit_model()