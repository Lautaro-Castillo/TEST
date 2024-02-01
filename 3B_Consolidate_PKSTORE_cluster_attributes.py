############################################################
# Comment Section
############################################################
#Bring all POS metrics (dropped columns in the clustering step)
#Determine POS best_performer.
#Create SCORE based on adherence, and GP
#
#need to add GP in score
#if there is a unique best_perfomer, then SUM(best_performer)=1
#SELECT CITY, format, prediction, COUNT(PK_STORE), SUM(best_performer) FROM pg_pos_clustered
#group by CITY, format, prediction
############################################################

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("3B Consolidate PKSTORE cluster attributes").getOrCreate()

p_Adherencia = 0.7

#Read DB pg_unlisted_pos_clusters_sk_h2o
df1_readdbpg_unlisted_pos_clusters_sk_h2o= spark.sql("SELECT * FROM portfolio_gap_peru.pg_unlisted_pos_clusters_sk_h2o")

# Join On Common Column
#Read DB pg_unlisted_so_filtered
df2_readdbpg_unlisted_so_filtered= spark.sql("SELECT * FROM portfolio_gap_peru.pg_unlisted_so_percentile_filtered")

# Join On Common Column
joindf = df2_readdbpg_unlisted_so_filtered
joindf = df2_readdbpg_unlisted_so_filtered.join(df1_readdbpg_unlisted_pos_clusters_sk_h2o.[PK_STORE],inner) 
df3_joinoncommoncolumn=joindf 

# Imputing With Constant
df5_imputingwithconstant = df3_joinoncommoncolumn
df5_imputingwithconstant = df5_imputingwithconstant.na.fill(99, ["prediction"])

# Math Score
df5_imputingwithconstant.createOrReplaceTempView("mathexpr_276e2491_d424_4578_88ef_760ac29a3f23")
df8_mathscore= spark.sql("""select *  , CAST(ADHERENCIA_FMODELO*${p_Adherencia} as DOUBLE) as SCORE from mathexpr_276e2491_d424_4578_88ef_760ac29a3f23""")

# Cast To double
from pyspark.sql.types import *
df_cast = df8_mathscore.withColumn("SCORE", df8_mathscore["SCORE"].cast(DoubleType()))
df10_casttodouble= df_cast 

# Join On Columns
# Select Columns
df22_selectcolumns=df10_casttodouble.select(["COUNTRY", "Format", "prediction", "SALES_GSU", "SCORE"])

# Group By formatteorico/prediction
df22_selectcolumns.createOrReplaceTempView("groupby_table_9")
df9_groupbyformatteoricoprediction= spark.sql("select Format ,prediction  , max(SCORE) as max_score  from groupby_table_9  group by Format ,prediction") 

# Add  Columns
import pyspark.sql.functions as F
temp_df = df9_groupbyformatteoricoprediction
from pyspark.sql.types import IntegerType
temp_df = temp_df.withColumn("temp", F.lit(str("1").strip())).withColumn("best_performer", F.col("temp").cast(IntegerType())).drop("temp")
df11_addcolumns = temp_df

# Columns Rename
df13_columnsrename=df11_addcolumns.withColumnRenamed("max_score", "SCORE")

# Join On Columns
leftTableJoinColumns = ["Format", "prediction", "SCORE"]
rightTableJoinColumns = ["Format", "prediction", "SCORE"]
leftDataFrameColumns = df10_casttodouble.columns
rightDataFrameColumns = df13_columnsrename.columns
pythonstmt = []

for i in range(len(leftTableJoinColumns)):
    if (rightTableJoinColumns.__contains__(leftTableJoinColumns[i])):
        pythonstmt.append("temp_left_table_join_14."+leftTableJoinColumns[i]+" as left_"+leftTableJoinColumns[i])
    else:
        pythonstmt.append("temp_left_table_join_14."+leftTableJoinColumns[i])

for i in range(len(rightTableJoinColumns)):
    if (leftTableJoinColumns.__contains__(rightTableJoinColumns[i])):
        pythonstmt.append("temp_right_table_join_14."+rightTableJoinColumns[i]+" as right_"+leftTableJoinColumns[i])
    else:
        pythonstmt.append("temp_right_table_join_14."+rightTableJoinColumns[i])

for i in range(len(leftDataFrameColumns)):
    if not(leftTableJoinColumns.__contains__(leftDataFrameColumns[i])):
        if rightDataFrameColumns.__contains__(leftDataFrameColumns[i]):
            pythonstmt.append("temp_left_table_join_14."+leftDataFrameColumns[i]+" as left_"+leftDataFrameColumns[i])
        else:
            pythonstmt.append("temp_left_table_join_14."+leftDataFrameColumns[i])

for i in range(len(rightDataFrameColumns)):
    if not(rightTableJoinColumns.__contains__(rightDataFrameColumns[i])):
        if leftDataFrameColumns.__contains__(rightDataFrameColumns[i]):
            pythonstmt.append("temp_right_table_join_14."+rightDataFrameColumns[i]+" as right_"+rightDataFrameColumns[i])
        else:
            pythonstmt.append("temp_right_table_join_14."+rightDataFrameColumns[i])

df10_casttodouble.createOrReplaceTempView("temp_left_table_join_14")
df13_columnsrename.createOrReplaceTempView("temp_right_table_join_14")
stmt = ','.join(pythonstmt)
df14_joinoncolumns = spark.sql(" SELECT " + stmt + " " " FROM temp_left_table_join_14 LEFT OUTER JOIN temp_right_table_join_14 ON (  temp_left_table_join_14.Format = temp_right_table_join_14.Format  AND  temp_left_table_join_14.prediction = temp_right_table_join_14.prediction  AND  temp_left_table_join_14.SCORE = temp_right_table_join_14.SCORE  )  " " ") 

# Drop Columns
df15_dropcolumns = df14_joinoncolumns.drop(*("right_prediction", "left_SCORE", "FORMATTEORICO", "Top_10_Percentile_GSU", "Top_20_Percentile_GSU", "Top_30_Percentile_GSU", "Top_10_Percentile_SKU", "Top_20_Percentile_SKU", "Top_30_Percentile_SKU", "Top_70_Percentile_GSU", "Top_70_Percentile_SKU", "CALC_CATEGORY_3_7", "right_Format"))

# Columns Rename
df16_columnsrename=df15_dropcolumns.withColumnRenamed("left_Format", "Format").withColumnRenamed("left_prediction", "prediction").withColumnRenamed("right_SCORE", "SCORE")

# Concat CLUSTER_NAME
from pyspark.sql.functions import concat_ws
df21_concatcluster_name = df16_columnsrename.withColumn("CLUSTER_NAME",concat_ws("|","COUNTRY", "Format", "prediction"))

# Cast To Single Type
from pyspark.sql.types import * 
df_cast = df21_concatcluster_name.withColumn("SALES_CS", df21_concatcluster_name["SALES_CS"].cast(DoubleType())).withColumn("SALES_GSU", df21_concatcluster_name["SALES_GSU"].cast(DoubleType())).withColumn("SALES_LC", df21_concatcluster_name["SALES_LC"].cast(DoubleType())).withColumn("AVGDISTSKU", df21_concatcluster_name["AVGDISTSKU"].cast(DoubleType()))
df27_casttosingletype=df_cast

# SQL
df27_casttosingletype.createOrReplaceTempView("fire_temp_table")
df28_sql= spark.sql("""SELECT 
    *,
    AVG(SALES_GSU) OVER (PARTITION BY Format, prediction) AS CENTR_SALES_GSU,
    AVG(AVGDISTSKU) OVER (PARTITION BY Format, prediction) AS CENTR_AVGDISTSKU
FROM 
    fire_temp_table;""") 

# Save DB pg_pos_clustered
df28_sql.write.saveAsTable("portfolio_gap_peru.pg_pos_clustered", format="parquet", mode="Overwrite")

# Coalesce
#fire.nodes.etl.NodeCoalesce not yet implemented!.
df23_coalesce=df28_sql

# Save CSV
df23_coalesce.write.csv(path="dbfs:/FileStore/shared_uploads/saju.kattuparambil@kcc.com/ClusterTOP123",mode="Overwrite",header="true")