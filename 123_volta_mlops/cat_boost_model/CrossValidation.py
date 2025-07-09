# Databricks notebook source
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from catboost import Pool, CatBoostRegressor, EFstrType, cv
import math as m
import datetime
import mlflow

# COMMAND ----------

class CrossValidation:

    def __init__(self, cv_type, model, patterns, category_features, logger, verbosity=False):
        self.model = model
        self.patterns = patterns
        self.cv_type = cv_type
        self.logger = logger
        self.category_features = category_features
        self.verbosity = verbosity
        self.K = 10

        self.holdout_cv_results_df = None
        self.feature_importance_df = None
        self.kfold_cv_results_df = None

    def validate(self):
        """ Ejecuta el proceso de validación cruzada y registra los resultados en MLflow. """
        try:
            self.logger.info("Start cross-validation process for model ...")

            if (self.cv_type == "all") or (self.cv_type == "hold_out"):
                self.holdout_cv(percentage=0.2)

            if (self.cv_type == "all") or (self.cv_type == "k_fold"):
                self.kfold_cv(K=self.K)

            # 📊 Guardar métricas en MLflow
            if self.holdout_cv_results_df is not None:
                mlflow.log_metrics({
                    "holdout_MAE": self.holdout_cv_results_df["MeanAbsoluteError"][0],
                    "holdout_RMSE": self.holdout_cv_results_df["RootMeanSquareError"][0],
                    "holdout_NMSE": self.holdout_cv_results_df["NormalizedMeanSquareError"][0],
                    "holdout_MEDAE": self.holdout_cv_results_df["MedianAbsoluteError"][0]
                })

            if self.kfold_cv_results_df is not None:
                mlflow.log_metrics({
                    "kfold_mean_MAE": self.kfold_cv_results_df["test-MAE-mean"].values[0],
                    "kfold_mean_RMSE": self.kfold_cv_results_df["test-RMSE-mean"].values[0]
                })

            self.logger.info("Cross-validation done successfully!")
        except Exception as err:
            self.logger.error(f"An error occurred during cross-validation: {str(err)}")
            raise

    def holdout_cv(self, percentage):
        targetColumns = self.patterns.Price
        X = self.patterns.drop(['Price'], axis=1)
        Y = targetColumns

        train_features_with_model, test_features_with_model, train_labels, test_labels = train_test_split(X, Y,
                                                                                                          test_size=percentage)

        train_features = train_features_with_model.drop(['Model'], axis=1)
        test_features = test_features_with_model.drop(['Model'], axis=1)

        # initialize Pool
        train_pool = Pool(train_features,
                          train_labels,
                          cat_features=self.category_features)
        test_pool = Pool(test_features,
                         test_labels,
                         cat_features=self.category_features)

        # train the cat_boost_model
        self.model.fit(train_pool,
                  eval_set=test_pool,
                  verbose=self.verbosity,
                  plot=False)

        train_best_score_df = pd.DataFrame.from_dict(self.model.best_score_)
        train_best_score_df['InventoryDate'] = datetime.date.today()

        # test cat_boost_model
        predictions = self.model.predict(test_pool)

        errors = abs(predictions - test_labels)
        sqErrors = errors * errors
        diff_mean_test = (test_labels - np.mean(test_labels)) ** 2

        NMSE = np.sum(sqErrors) / np.sum(diff_mean_test)
        MAE = round(np.mean(errors), 2)
        RMSE = round(m.sqrt(np.mean(sqErrors)), 2)
        MEDAE = round(np.median(errors), 2)

        holdout_results = {'MeanAbsoluteError': MAE,
                           'RootMeanSquareError': RMSE,
                           'NormalizedMeanSquareError': NMSE,
                           'MedianAbsoluteError': MEDAE}
        self.holdout_cv_results_df = pd.DataFrame(holdout_results, index=[0])
        self.holdout_cv_results_df['InventoryDate'] = datetime.date.today()

        self.feature_importance_df = self.model.get_feature_importance(data=test_pool,
                                                             prettified=True,
                                                             type=EFstrType.FeatureImportance)
        self.feature_importance_df['InventoryDate'] = datetime.date.today()
        try:
            self.feature_importance_df = self.feature_importance_df.rename(columns={'Feature Id': 'FeatureName'}, errors="raise")
        except:
            self.feature_importance_df = self.feature_importance_df.rename(columns={'Feature Index': 'FeatureName'}, errors="raise")


    def kfold_cv(self, K):
        targetColumns = self.patterns.Price
        X = self.patterns.drop(['Price', 'Model'], axis=1)
        Y = targetColumns

        params=self.model.get_params()

        cv_dataset = Pool(data=X,
                          label=Y,
                          cat_features=self.category_features)

        scores = cv(cv_dataset,
                    params=params,
                    fold_count=K,
                    plot=False,
                    verbose=self.verbosity)

        self.kfold_cv_results_df = scores[-1:]
        self.kfold_cv_results_df = self.kfold_cv_results_df.copy()
        self.kfold_cv_results_df.loc[:, 'InventoryDate'] = datetime.date.today()
