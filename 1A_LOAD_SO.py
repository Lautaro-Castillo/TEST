############################################################
# Comment Section
############################################################
#Aggregate SellOut data for [X] months.
#SALES_CS: Cases
#SALES_GSU: GSU
#SALES_LC: LC
#p_MONTHS: parameter for number of months for Historical SO
#p_COUNTRY: parameter for list of countries
#
#Load [pg_profiles_extended] for format and adherence to format
#
############################################################

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("1A Load SO").getOrCreate()
p_COUNTRY = 'GT'
p_MONTHS = 3
#Execute SF T_ALL_SELLOUT_SALES

sfUser = dbutils.secrets.get(scope = db_secrets_scope, key = sf_user_key) 
sfPassword =dbutils.secrets.get(scope = db_secrets_scope, key = sf_password_key) 
sfUrl =dbutils.secrets.get(scope = db_secrets_scope, key = sf_url_key) 
options = {"sfUrl":sfUrl,  "sfUser": sfUser, "sfPassword": sfPassword, "sfDatabase": "AI_FACTORY",  "sfSchema":"STAGING", "sfWarehouse": "AI_FACTORY_ETL_WH"}
df1_executesft_all_sellout_sales= spark.read.format("snowflake").options(**options).option("query","""SELECT SELLOUT_PARTIAL.COUNTRY, MD_STORE.CITY, SELLOUT_PARTIAL.STORE_ID, MD_STORE.PK_STORE, MD_STORE.POS_NAME,
      SELLOUT_PARTIAL.SOLDTO, CUST_HIER.KUNNR_H_TXT, CUST_HIER.KUNNR_LVL04, CUST_HIER.KUNNR_LVL04_TXT,
      MD_STORE.CHANNEL, SELLOUT_PARTIAL.MATNR_PAK_EAN,
      SELLOUT_PARTIAL.SUM_SALES_CS, SELLOUT_PARTIAL.SUM_SALES_GSU, SELLOUT_PARTIAL.SUM_AMOUNT_LC -- ,SELLOUT_PARTIAL.COUNT_EXT_SKU_ID
FROM 
   (SELECT SELLOUT_DATA.COUNTRY, SELLOUT_DATA.SOLD_TO_ID SOLDTO, SELLOUT_DATA.STORE_ID, MD_MATERIAL.MATNR_PAK_EAN,
   SUM(SALES_CS) AS SUM_SALES_CS, SUM(SALES_GSU) AS SUM_SALES_GSU, SUM(TOT_AMOUNT) AS SUM_AMOUNT_LC -- , count(distinct EXT_SKU_ID) as COUNT_EXT_SKU_ID
   FROM CUSTOMER_SALES.REPORTING_LA_SELLOUT.V_ALL_SELL_OUT_SNAP SELLOUT_DATA

   INNER JOIN CUSTOMER_SALES.REPORTING_LA_CONSUMER. T_LA_ALL_MD_MATERIAL MD_MATERIAL
   ON SELLOUT_DATA.SKU_ID = MD_MATERIAL.MATNR

   WHERE DATE >= DATEADD(MONTH, -${p_MONTHS}, GETDATE()) AND SELLOUT_DATA.COUNTRY IN (${p_COUNTRY}) AND  CURTYPE = 'LC'
   GROUP BY SELLOUT_DATA.COUNTRY, SELLOUT_DATA.SOLD_TO_ID, SELLOUT_DATA.STORE_ID, MD_MATERIAL.MATNR_PAK_EAN
   ) SELLOUT_PARTIAL

INNER JOIN CUSTOMER_SALES.INT_LA_SELLOUT. T_ALL_MD_STORE MD_STORE
ON SELLOUT_PARTIAL.STORE_ID = MD_STORE.STORE_ID

INNER JOIN CUSTOMER_SALES.REPORTING_LA_CONSUMER. T_LA_ALL_MD_CUSTOMER_HIERARCHY CUST_HIER
ON SELLOUT_PARTIAL.SOLDTO = CUST_HIER.KUNNR_H

ORDER BY SELLOUT_PARTIAL.COUNTRY, SELLOUT_PARTIAL.STORE_ID, SELLOUT_PARTIAL.MATNR_PAK_EAN""").load()

# Group By EAN
df1_executesft_all_sellout_sales.createOrReplaceTempView("groupby_table_9")
df9_groupbyean= spark.sql("select COUNTRY ,PK_STORE  , sum(SUM_SALES_CS) as sum_sum_sales_cs , sum(SUM_SALES_GSU) as sum_sum_sales_gsu , sum(SUM_AMOUNT_LC) as sum_sum_amount_lc  from groupby_table_9  group by COUNTRY ,PK_STORE") 

# Columns Rename
df10_columnsrename=df9_groupbyean.withColumnRenamed("sum_sum_sales_cs", "SALES_CS").withColumnRenamed("sum_sum_sales_gsu", "SALES_GSU").withColumnRenamed("sum_sum_amount_lc", "SALES_LC")

# Join On PK_STORE
#Execute SF T_ALL_SELLOUT_SALES SNOWFLAKE
sfUser = dbutils.secrets.get(scope = db_secrets_scope, key = sf_user_key) 
sfPassword =dbutils.secrets.get(scope = db_secrets_scope, key = sf_password_key) 
sfUrl =dbutils.secrets.get(scope = db_secrets_scope, key = sf_url_key) 
options = {"sfUrl":sfUrl,  "sfUser": sfUser, "sfPassword": sfPassword, "sfDatabase": "AI_FACTORY",  "sfSchema":"STAGING", "sfWarehouse": "AI_FACTORY_ETL_WH"}
df14_executesft_all_sellout_salessnowflake= spark.read.format("snowflake").options(**options).option("query","""-- Calculate the total sales for each product for each store
WITH total_sales AS (
    SELECT POS_PK_STORE, EAN_PAK, COUNT(*) as sales_count
    FROM CUSTOMER_SALES.REPORTING_LA_SELLOUT. V_ALL_SELL_OUT_SNAP
WHERE DATE >= DATEADD(MONTH, -${p_MONTHS}, GETDATE()) AND COUNTRY IN (${p_COUNTRY}) AND  CURTYPE = 'LC'
    -- AND POS_PK_STORE = 'GT-0040002538-10-01-000000000000022-7090'
    GROUP BY POS_PK_STORE, EAN_PAK
),

-- Calculate the average count of distinct products sold per month for each store
average_count AS (
    SELECT POS_PK_STORE, AVG(DstCnt) as avg_count
    FROM (
        SELECT POS_PK_STORE, COUNT(distinct EAN_PAK) as DstCnt
        FROM CUSTOMER_SALES.REPORTING_LA_SELLOUT. V_ALL_SELL_OUT_SNAP
		WHERE DATE >= DATEADD(MONTH, -${p_MONTHS}, GETDATE()) AND COUNTRY IN (${p_COUNTRY}) AND  CURTYPE = 'LC'
          -- AND POS_PK_STORE = 'GT-0040002538-10-01-000000000000022-7090'
        GROUP BY POS_PK_STORE, substr(DATE,6,2)
    )
    GROUP BY POS_PK_STORE
),

-- Rank the products for each store by sales count
ranked_sales AS (
    SELECT total_sales.POS_PK_STORE, EAN_PAK, sales_count, ROW_NUMBER() OVER(PARTITION BY total_sales.POS_PK_STORE ORDER BY sales_count DESC) as rn
    FROM total_sales
)

-- Select the top products with the most sales for each store, limited by the average count
SELECT rs.POS_PK_STORE AS PK_STORE,
       ac.avg_count AS AVGDISTSKU,
       LISTAGG(EAN_PAK || ':' || sales_count, ',') WITHIN GROUP (ORDER BY sales_count DESC) as CONCATENATEDEANCODES
FROM ranked_sales rs
JOIN average_count ac ON rs.POS_PK_STORE = ac.POS_PK_STORE
WHERE rn <= avg_count
GROUP BY rs.POS_PK_STORE, ac.avg_count

/*
with CTE as
(
Select STORE_ID as STORE_ID  , count (distinct EAN_PAK) as DstCnt , substr(DATE,6,2)  as Mth ,
LISTAGG(DISTINCT EAN_PAK, ',') WITHIN GROUP (ORDER BY EAN_PAK) AS ConcatenatedEANCodes
 FROM 
	CUSTOMER_SALES.REPORTING_LA_SELLOUT. V_ALL_SELL_OUT_SNAP
WHERE DATE >= DATEADD(MONTH, -${p_MONTHS}, GETDATE()) AND COUNTRY IN (${p_COUNTRY}) AND  CURTYPE = 'LC'
-- AND POS_PK_STORE = 'GT-0040001915-10-01-000000000006301-7090'
group by STORE_ID ,  substr(DATE,6,2)
) 
Select  
    Distinct PK_STORE ,AVG(DstCnt) OVER (PARTITION BY PK_STORE) AS AvgDistSKU, ConcatenatedEANCodes FROM CTE 
INNER JOIN CUSTOMER_SALES.INT_LA_SELLOUT. T_ALL_MD_STORE MD_STORE
ON CTE.STORE_ID = MD_STORE.STORE_ID */""").load()

# Join On PK_STORE
#Read CSV
from pyspark.sql.types import *
ss = StructType([StructField("COUNTRY",StringType(), True),StructField("PKSTORE",StringType(), True),StructField("Format",StringType(), True),StructField("POS_EAN_COUNT",StringType(), True),StructField("POS_EAN_TOTAL",StringType(), True),StructField("POS_SELLOUT",StringType(), True),StructField("POS_SELLOUT_TOTAL",StringType(), True),StructField("FORMAT_EAN_COUNT",StringType(), True),StructField("FORMAT_SELLOUT_AVG",StringType(), True),StructField("FORMAT_ORIGINAL",StringType(), True),StructField("RANK_FORMAT",StringType(), True),StructField("RANK_VOLUME",StringType(), True),StructField("ADHERENCIA_FMODELO",IntegerType(), True),StructField("FORMATTEORICO",StringType(), True),StructField("TEORICO_FLAG",StringType(), True),StructField("POS_EAN_TEORICO_COUNT",StringType(), True),StructField("TEORICO_EAN_COUNT",StringType(), True),StructField("ADHERENCIA_TEORICO",IntegerType(), True),StructField("SOLDTO",StringType(), True),StructField("KUNNR_H_TXT",StringType(), True),StructField("CHANNEL",StringType(), True)])
df17_readcsv= spark.read.format("csv").option("header", "true").option("inferSchema", "false").option("sep", ",").schema(ss).load("dbfs:/PortfolioGap/pg_pos_profiles_extended/pg_profiles_extended.csv") 

# Select Columns
df6_selectcolumns=df17_readcsv.select(["PKSTORE", "ADHERENCIA_FMODELO", "ADHERENCIA_TEORICO", "Format", "FORMATTEORICO"])

# Columns Rename
df7_columnsrename=df6_selectcolumns.withColumnRenamed("PKSTORE", "PK_STORE")

# Join On PK_STORE
joindf = df10_columnsrename
joindf = df10_columnsrename.join(df7_columnsrename.[PK_STORE],inner) 
df5_joinonpk_store=joindf 

# Join On PK_STORE
joindf = df5_joinonpk_store
joindf = df5_joinonpk_store.join(df14_executesft_all_sellout_salessnowflake.[PK_STORE],inner) 
df15_joinonpk_store=joindf 

# pg_unlisted_so
df15_joinonpk_store.write.saveAsTable("portfolio_gap_peru.pg_unlisted_so", format="parquet", mode="Overwrite")