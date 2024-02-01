############################################################
# Comment Section
############################################################
#Cluster Based On TOP 10-20-30 Percentile
#Cluster Based On TOP 30-70-Others Percentile
#
#Clustering method: [H2O K-means]
#[Estimate K]=true lets the node select the optimal K.
#AFter analysis, we have now set [Estimate K] = False & K=3
#
#Save to databricks uses Append to save the results of all iterations
#Previous workflows wipe the table before running this workflow.
############################################################

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("3A Determine Clusters H2O Loop").getOrCreate()
#Read DB pg_unlisted_so_filtered

df1_readdbpg_unlisted_so_filtered= spark.sql("SELECT * FROM portfolio_gap_peru.pg_unlisted_so_percentile_filtered")



# Row Filter

df37_rowfilter= df1_readdbpg_unlisted_so_filtered.filter("""SALES_GSU > 0""")



# Drop Columns

df27_dropcolumns = df37_rowfilter.drop(*("COUNTRY", "SALES_CS", "SALES_LC", "ADHERENCIA_FMODELO", "ADHERENCIA_TEORICO", "FORMATTEORICO", "Top_10_Percentile_GSU", "Top_20_Percentile_GSU", "Top_30_Percentile_GSU", "Top_10_Percentile_SKU", "Top_20_Percentile_SKU", "Top_30_Percentile_SKU", "CONCATENATEDEANCODES"))


# Execute In Loop

from pyspark.sql import Row 
loop_cols = ["Format"]
distinct_values_df = df27_dropcolumns.select(loop_cols).distinct() 
distinct_values_count = distinct_values_df.count() 
dist_values = distinct_values_df.collect() 
df27_dropcolumns.createOrReplaceTempView("temp_table_28") 
for i in range(len(dist_values)):
    row: Row = dist_values[i]
    where_clause = ""
    key = "loop_"
    value = ""
    for j in range(len(loop_cols)):
        if j == 0:
            where_clause = where_clause +" "+loop_cols[j]+"= \"" + str(row.__getitem__(j)) + "\""
            key = key + loop_cols[j]
            value = value + str(row.__getitem__(j))
        else:
            where_clause = where_clause +" and "+loop_cols[j]+"= \"" + str(row.__getitem__(j)) + "\""
            key = key + loop_cols[j]
            value = value + str(row.__getitem__(j))
    temp_df = spark.sql("select * from temp_table_28 where "+ where_clause) 
    df28_executeinloop  = temp_df


    # H2O Score



    # H2O K-Means

    from ai.h2o.sparkling import H2OContext
    from pysparkling.ml import H2OKMeans, H2OKMeansMOJOModel
    hc = H2OContext.getOrCreate()
    estimator = H2OKMeans( columnsToCategorical=[],featuresCols=["SALES_GSU", "AVGDISTSKU", "CALC_CATEGORY"],keepBinaryModels=False,withContributions=False,modelId=None,standardize=True,seed=-1,maxIterations=10,withStageResults=False,splitRatio=1.0,init="Furthest",k=4,nfolds=0,dataFrameSerializer="ai.h2o.sparkling.utils.JSONDataFrameSerializer",withLeafNodeAssignments=False,namedMojoOutputColumns=True,validationDataFrame=None,convertInvalidNumbersToNa=False,convertUnknownCategoricalLevelsToNa=False,keepCrossValidationModels=True,keepCrossValidationPredictions=False,keepCrossValidationFoldAssignment=False,exportCheckpointsDir=None)
    model: H2OKMeansMOJOModel = estimator.fit(df28_executeinloop)
    trainOutputDF = model.transform(df28_executeinloop)
    trainMetricsMap = model.getTrainingMetrics()
    print(trainMetricsMap)
    df29_h2okmeans = df28_executeinloop


    # H2O Score

    from ai.h2o.sparkling.ml.models import H2OMOJOModel
    mojoModel: H2OMOJOModel = model
    df22_h2oscore = mojoModel.transform(df29_h2okmeans)


    # Select Columns

    df31_selectcolumns=df22_h2oscore.select(["PK_STORE", "prediction"])



    # Save Databricks Table

    df31_selectcolumns.write.saveAsTable("portfolio_gap_peru.pg_unlisted_pos_clusters_sk_h2o", format="parquet", mode="Append")



    # H2O K-Means

    from ai.h2o.sparkling import H2OContext
    from pysparkling.ml import H2OKMeans, H2OKMeansMOJOModel
    hc = H2OContext.getOrCreate()
    estimator = H2OKMeans( columnsToCategorical=[],featuresCols=["SALES_GSU", "AVGDISTSKU", "CALC_CATEGORY_3_7"],keepBinaryModels=False,withContributions=False,modelId=None,standardize=True,seed=-1,maxIterations=10,withStageResults=False,splitRatio=1.0,init="Furthest",k=3,nfolds=0,dataFrameSerializer="ai.h2o.sparkling.utils.JSONDataFrameSerializer",withLeafNodeAssignments=False,namedMojoOutputColumns=True,validationDataFrame=None,convertInvalidNumbersToNa=False,convertUnknownCategoricalLevelsToNa=False,keepCrossValidationModels=True,keepCrossValidationPredictions=False,keepCrossValidationFoldAssignment=False,exportCheckpointsDir=None)
    model: H2OKMeansMOJOModel = estimator.fit(df28_executeinloop)
    trainOutputDF = model.transform(df28_executeinloop)
    trainMetricsMap = model.getTrainingMetrics()
    print(trainMetricsMap)
    df33_h2okmeans = df28_executeinloop


    # H2O Score

    from ai.h2o.sparkling.ml.models import H2OMOJOModel
    mojoModel: H2OMOJOModel = model
    df34_h2oscore = mojoModel.transform(df33_h2okmeans)


    # Select Columns

    df35_selectcolumns=df34_h2oscore.select(["PK_STORE", "prediction"])



    # Save Databricks Table

    df35_selectcolumns.write.saveAsTable("portfolio_gap_peru.pg_unlisted_pos_clusters_sk_h2o_temp", format="parquet", mode="Append")



    # H2O Score





