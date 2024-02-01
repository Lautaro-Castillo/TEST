############################################################
# Comment Section
############################################################
#copy blank table
#cannot directly read and write to the same databricks table
############################################################

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("2B reset pg_unlisted_pos_clusters_sk_h2o").getOrCreate()

#Read DB blank_pg_unlisted_pos_clusters_sk
df4_readdbblank_pg_unlisted_pos_clusters_sk= spark.sql("SELECT * FROM portfolio_gap_peru.blank_pg_unlisted_pos_clusters_sk")

# Save DB pg_unlisted_pos_clusters_sk_h2o
df4_readdbblank_pg_unlisted_pos_clusters_sk.write.saveAsTable("portfolio_gap_peru.pg_unlisted_pos_clusters_sk_h2o", format="parquet", mode="Overwrite")

# Save DB pg_unlisted_pos_clusters_sk_h2o_temp
df4_readdbblank_pg_unlisted_pos_clusters_sk.write.saveAsTable("portfolio_gap_peru.pg_unlisted_pos_clusters_sk_h2o_temp", format="parquet", mode="Overwrite")