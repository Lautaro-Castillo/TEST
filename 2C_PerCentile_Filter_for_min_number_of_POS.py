############################################################
# Comment Section
############################################################
#Filter records where for a Formato_Original we have atleast 1 POS
#Capture TOP 10-20-30 Percentile & TOP 30-70-Others Percentile data
############################################################

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("2C PerCentile Filter for min number of POS").getOrCreate()

p_min_count = 1

#Read DB pg_unlisted_so
df1_readdbpg_unlisted_so= spark.sql("SELECT * FROM portfolio_gap_peru.pg_unlisted_so")

# Add  Columns
import pyspark.sql.functions as F
temp_df = df1_readdbpg_unlisted_so
from pyspark.sql.types import IntegerType
temp_df = temp_df.withColumn("temp", F.lit(str("1").strip())).withColumn("COUNT", F.col("temp").cast(IntegerType())).drop("temp")
df3_addcolumns = temp_df

# Group By format
df3_addcolumns.createOrReplaceTempView("groupby_table_2")
df2_groupbyformat= spark.sql("select Format  , sum(COUNT) as sum_count  from groupby_table_2  group by Format") 

# Row Filter
df4_rowfilter= df2_groupbyformat.filter("""sum_count > (${p_min_count})""")

# Drop Columns
df6_dropcolumns = df4_rowfilter.drop(*("sum_count"))

# Join On format
joindf = df1_readdbpg_unlisted_so.join(df6_dropcolumns.["Format"],inner) 
df5_joinonformat=joindf 

# Save DB pg_unlisted_so_filtered
df5_joinonformat.write.saveAsTable("portfolio_gap_peru.pg_unlisted_so_filtered", format="parquet", mode="Overwrite")

# Math Expression
df5_joinonformat.createOrReplaceTempView("mathexpr_47ae39a6_8c9d_44e5_95af_5771d39d703e")
df12_mathexpression= spark.sql("""select *  , CAST(percentile_approx(SALES_GSU, 0.1) OVER (PARTITION BY Format) as DOUBLE) as Top_10_Percentile_GSU , CAST(percentile_approx(SALES_GSU, 0.2) OVER (PARTITION BY Format) as DOUBLE) as Top_20_Percentile_GSU , CAST(percentile_approx(SALES_GSU, 0.3) OVER (PARTITION BY Format) as DOUBLE) as Top_30_Percentile_GSU , CAST(percentile_approx(AVGDISTSKU, 0.1) OVER (PARTITION BY Format) as DOUBLE) as Top_10_Percentile_SKU , CAST(percentile_approx(AVGDISTSKU, 0.2) OVER (PARTITION BY Format) as DOUBLE) as Top_20_Percentile_SKU , CAST(percentile_approx(AVGDISTSKU, 0.3) OVER (PARTITION BY Format) as DOUBLE) as Top_30_Percentile_SKU , CAST(percentile_approx(SALES_GSU, 0.7) OVER (PARTITION BY Format) as DOUBLE) as Top_70_Percentile_GSU , CAST(percentile_approx(AVGDISTSKU, 0.7) OVER (PARTITION BY Format) as DOUBLE) as Top_70_Percentile_SKU from mathexpr_47ae39a6_8c9d_44e5_95af_5771d39d703e""")

# Expressions
import pyspark.sql.functions as F
df12_mathexpression=df12_mathexpression.withColumn("CALC_CATEGORY ", F.expr("IF(AND(AVGDISTSKU>=Top_10_Percentile_SKU,SALES_GSU>=Top_10_Percentile_GSU),"TOP 10",IF(AND(AVGDISTSKU>=Top_20_Percentile_SKU,SALES_GSU>=Top_20_Percentile_GSU),"TOP 20",IF(AND(AVGDISTSKU>=Top_30_Percentile_SKU,SALES_GSU>=Top_30_Percentile_GSU),"TOP 30","OTHERS")))").cast("string"))
df12_mathexpression=df12_mathexpression.withColumn("CALC_CATEGORY_3_7 ", F.expr("IF(AND(AVGDISTSKU>=Top_30_Percentile_SKU,SALES_GSU>=Top_30_Percentile_GSU),"TOP 30",IF(AND(AVGDISTSKU>=Top_70_Percentile_SKU,SALES_GSU>=Top_70_Percentile_GSU),"TOP 70","OTHERS"))").cast("string"))
df15_expressions=df12_mathexpression

# Save DB pg_unlisted_so_filtered
df15_expressions.write.saveAsTable("portfolio_gap_peru.pg_unlisted_so_percentile_filtered", format="parquet", mode="Overwrite")