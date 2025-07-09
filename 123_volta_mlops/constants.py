# Databricks notebook source
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, BooleanType, BinaryType

params = {
'iterations': 7531,
'depth': 8,
'learning_rate': 0.0747,
'l2_leaf_reg':	0.04734,
'loss_function': 'RMSE',
'eval_metric': 'MAE',
'early_stopping_rounds': 100,
'category_features':['Brand', 'Fuel', 'Gearbox', 'Segment', 'Sale']}

vr_s_hacienda = pd.Series([100, 84, 67, 56, 47, 39, 34, 28, 24, 19, 17, 13, 10 ])


datatypes_feature_importance = StructType([
    StructField("FeatureName", StringType(), True),  # NVARCHAR(200) -> StringType()
    StructField("Importances", DoubleType(), True),  # Float() -> DoubleType()
    StructField("InventoryDate", DateType(), True)   # Date() -> DateType()
])

datatypes_holdout_cv = StructType([
    StructField("MeanAbsoluteError", DoubleType(), True),
    StructField("RootMeanSquareError", DoubleType(), True),
    StructField("NormalizedMeanSquareError", DoubleType(), True),
    StructField("MedianAbsoluteError", DoubleType(), True),
    StructField("InventoryDate", DateType(), True)
])

datatypes_kfold_cv = StructType([
    StructField("iterations", DoubleType(), True),
    StructField("test_MAE-mean", DoubleType(), True),
    StructField("test-MAE-std", DoubleType(), True),
    StructField("train-MAE-mean", DoubleType(), True),
    StructField("train-MAE-std", DoubleType(), True),
    StructField("test_RMSE_mean", DoubleType(), True),
    StructField("test-RMSE-std", DoubleType(), True),
    StructField("train-RMSE-mean", DoubleType(), True),
    StructField("train-RMSE-std", DoubleType(), True),
    StructField("InventoryDate", DateType(), True)
])

datatypes_model_cv={
    'feature_importance':datatypes_feature_importance,
    'hold_out':datatypes_holdout_cv,
    'k_fold':datatypes_kfold_cv,
}

datatypes_persistence_model = StructType([
    StructField("endDate", DateType(), True),
    StructField("active", BooleanType(), True),
    StructField("executionDate", DateType(), True),
    StructField("inputFeaturesForTraining", StringType(), True),  # NVARCHAR(4000) -> StringType()
    StructField("hyperparameters", StringType(), True),           # NVARCHAR(4000) -> StringType()
    StructField("model", BinaryType(), True)                      # VARBINARY('max') -> BinaryType()
])


table_names = {
    'feature_importance':'volta_IUC_VO_001_FeatureImportance',
    'hold_out':'volta_IUC_VO_001_CrossValidation_HoldOut_Results',
    'k_fold':'volta_IUC_VO_001_CrossValidation_KFold_Results'
    }

estados = ['Impecable', 'Muy Buena', 'Buena', 'Regular']
condition_coefficients = {'less_than_2_years':{estados[0]:1.05, estados[1]:1.03, estados[2]:1.0, estados[3]:0.954},
                          'more_than_2_years':{estados[0]:1.067, estados[1]:1.03, estados[2]:1.0, estados[3]:0.924}}

text_info_aggregator0 = "La tabla de los anuncios se puede mover horizontalmente (scroll horizonal) para ver todas las columnas."

text_info_aggregator1 = "En los anuncios procedentes de la web de CICAR no se especifican los Kms. " \
                       "En estos casos los Kms se muestran cómo una estimación en función de la edad del vehículo, " \
                       "pero no son valores reales."


text_info_aggregator2 = "Activando la comparativa con el precio de mercado en la " \
                       "columna 'DifPM[%]' obtenemos el desvio porcentual del precio" \
                       " del vehículo en el anuncio con respecto al precio del mercado calculado con el mismo modelo que se emplea " \
                       "en la pestaña 'Cálculo de Estimación de precio de Sale para un vehículo'"


text_info_price_estimator1 = "Para calcular el VR: poner Antigüedad = 0, Kms = 0, rellenar Precio VN y volver a tasar el vehículo."
text_info_price_estimator2 = "Para calcular el VR según modelo de Hacienda tiene que rellenar Precio VN"


text_graph_aggregator_price_scatter = 'Gráfico de dispersión de precios '
text_graph_aggregator_price_evolution = 'Gráfico de evolución precio medio '
text_graph_distributions1 = 'Distribución Brand, modelo, kms, antigüedad '
text_graph_distributions2 = 'Distribución Fuel, Gearbox, segment '
text_graph_distributions3 = 'Distribución web, provincia, tipo de Sale '
text_graph_aggregator_offer_volume = 'Gráfico de evolución volúmen oferta '
text_price_nv = "Valor inicial/Precio VN (€): "
text_no_price_nv = "tiene que especificar un precio VN"



