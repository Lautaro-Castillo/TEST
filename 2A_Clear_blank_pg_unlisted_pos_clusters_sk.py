############################################################
# Comment Section
############################################################
#Read structure of pg_unlisted_pos_clusters_sk_h2o
#apply row filter that returns 0 rows (prediction = -1)
#save dataframe with 0 rows
#
#Cannot write directly to [pg_unlisted_pos_clusters_sk_h2o]
#sparkflows does not allow reading and writing to the same databricks table in the same workflow.
#
#Truncates Data From table portfolio_gap.pg_unlisted_pos_clusters_sk_h2o
#To be used before each Loop Execution
############################################################

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("2A Clear blank_pg_unlisted_pos_clusters_sk").getOrCreate()

#Read DB pg_unlisted_pos_clusters_sk_h2o 
df5_readdbpg_unlisted_pos_clusters_sk_h2o= spark.sql("SELECT * FROM portfolio_gap.pg_unlisted_pos_clusters_sk_h2o ")


# Row Filter
df6_rowfilter= df5_readdbpg_unlisted_pos_clusters_sk_h2o.filter("""prediction = -1""")

# Save DB blank_pg_unlisted_pos_clusters_sk
df6_rowfilter.write.saveAsTable("portfolio_gap_peru.blank_pg_unlisted_pos_clusters_sk", format="parquet", mode="Overwrite")