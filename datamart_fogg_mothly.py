#DATAMART FOGG 

#Mengimport library

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql import functions as f
from pyspark.sql.functions import col, least
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
import pandas as pd
import numpy as np


#Membuat spark session
appName = "datamart_fogg_monthly"
master = "yarn"

spark = (
    SparkSession.builder.appName(appName)
    .master(master)
    .enableHiveSupport()
    .getOrCreate()
)

#Membuat variabel untuk menentukan periode data yang digunakan
end_date=spark.sql("select last_day(add_months(current_date, -1)) as end_date ").collect()[0]["end_date"] #(current month -1)
start_var=spark.sql("select date_add(last_day(add_months(current_date, -4)),1) as start ").collect()[0]["start"] #(current month -4)


#membuat spark dataframe trx in - frekuensi dan amount nasabah 3 bulan
fm_in_3m=spark.sql("""SELECT t2.cif_no, freq_trx_in_last3m, amt_trx_in_last3m
FROM (
select cif_no, count(recid) as freq_trx_in_last3m, sum(amt_trx) as amt_trx_in_last3m
from db_intensifikasi.master_trx_in_inc
where batch_date between '{0}' and '{1}'
group by cif_no ) t2
""".format(start_var,end_date))


# membuat spark dataframe trx out - frekuensi dan monetary amount nasabah 3 bulan
fm_out_3m=spark.sql("""SELECT t2.cif_no, freq_trx_out_last3m, amt_trx_out_last3m
FROM (
select cif_no, max(batch_date) as last_trx, count(recid) as freq_trx_out_last3m, sum(amt_trx) as amt_trx_out_last3m
from db_intensifikasi.master_trx_out_inc
where channel = 'MOBILE' and batch_date between '{0}' and '{1}'
group by cif_no ) t2
""".format(start_var,end_date))

#-----------------------------------------------------------
#membuat spark dataframe trx nonmb - frekuensi dan monetary amount nasabah 3 bulan
fm_nonmb_3m=spark.sql("""SELECT t2.cif_no, freq_trx_nonmb_last3m, amt_trx_nonmb_last3m
FROM (
select cif_no, count(recid) AS freq_trx_nonmb_last3m, sum(amt_trx) AS amt_trx_nonmb_last3m
from db_intensifikasi.master_trx_out_inc
WHERE channel != 'MOBILE' AND batch_date between '{0}' and '{1}'
GROUP BY cif_no ) t2
""".format(start_var,end_date))

#-----------------------------------------------------------------------
#join untuk semua dataframe in,out, nonmb
datamart1=fm_in_3m.join(fm_out_3m,fm_in_3m.cif_no==fm_out_3m.cif_no,"full").drop(fm_out_3m.cif_no)
datamart2=datamart1.join(fm_nonmb_3m,datamart1.cif_no==fm_nonmb_3m.cif_no,"full").drop(fm_nonmb_3m.cif_no)

#import master_id_modelling (filter cif yang mempunyai valid no hp)
master_id=spark.sql("""SELECT cif_no, mobile_phone, cif_latest
FROM db_intensifikasi.master_id_modeling
""") 

#join dengan datamart menggunakan full join agar yang terambil semua nasabah 
datamart3=datamart2.join(master_id,master_id.cif_no==datamart2.cif_no,"right").drop(master_id.cif_no)

#grouping cif dengan master_id (per id level)
from pyspark.sql import functions as f
datamart4=datamart3.groupBy("cif_latest","mobile_phone") \
    .agg(f.sum("freq_trx_out_last3m").alias("freq_trx_out_last3m"), \
         f.sum("amt_trx_out_last3m").alias("amt_trx_out_last3m"), \
         f.sum("freq_trx_in_last3m").alias("freq_trx_in_last3m"), \
         f.sum("amt_trx_in_last3m").alias("amt_trx_in_last3m"), \
         f.sum("freq_trx_nonmb_last3m").alias("freq_trx_nonmb_last3m"), \
         f.sum("amt_trx_nonmb_last3m").alias("amt_trx_nonmb_last3m"))


#membuat spark dataframe frekuensi log mobile - value 3 bulan
f_value3m=spark.sql("""SELECT mobile_phone, count(id) as freq_activity_value_3m
FROM db_intensifikasi.mb_activity_inc
WHERE category_request_type = 'value' and batch_date between '{0}' and '{1}'
GROUP BY mobile_phone
""".format(start_var,end_date))

#membuat spark dataframe frekuensi log mobile - nonvalue 3 bulan
f_nonvalue3m=spark.sql("""SELECT mobile_phone, count(id) as freq_activity_nonvalue_3m
FROM db_intensifikasi.mb_activity_inc
WHERE category_request_type = 'non-value' and batch_date between '{0}' and '{1}'
GROUP BY mobile_phone""".format(start_var,end_date))

#join
datamart5=datamart4.join(f_value3m,datamart4.mobile_phone==f_value3m.mobile_phone,"left").drop(f_value3m.mobile_phone)
datamart6=datamart5.join(f_nonvalue3m,datamart5.mobile_phone==f_nonvalue3m.mobile_phone,"left").drop(f_nonvalue3m.mobile_phone)


#replace missing value dengan nilai 0
datamart7=datamart6.na.fill(0)

#membuat spark dataframe deposit
avg_balance =spark.sql(""" select 
mid.cif_latest cif_latest,
sum(bal.account_avg_bal_30days) account_avg_bal_30days,
sum(bal.account_mtd_avg_bal_60days) account_mtd_avg_bal_60days,
sum(bal.account_avg_bal_90days) account_avg_bal_90days,
count (distinct bal.account_no) total_account,
count (distinct bal.product_desc) total_product
from db_intensifikasi.avg_account_balance_inc bal 
join db_intensifikasi.master_id_modeling  mid on bal.cif_no=mid.cif_no
where batch_date ='{0}'
group by mid.cif_latest
""".format(end_date))

#membuat spark dataframe master_cif (mendapatkan informasi data demografi nasabah)
master_cif=spark.sql(""" select i.cif_latest cif_latest,
c.cust_location cust_location,
c.age age,
c.gender gender,
c.customer_since customer_since,
c.msm_since msm_since
from db_intensifikasi.master_cif c 
join (select distinct cif_latest from db_intensifikasi.master_id_modeling) i on c.cif_no= i.cif_latest
""")
#master_cif.count() #

#join dataframe 
datamart_fogg=datamart6.join(avg_balance,"cif_latest","inner")
datamart_fogg2=datamart_fogg.join(master_cif,"cif_latest","left")
#datamart_fogg2.count()

#menembahkan kolom baru batch_date
predictions_fogg2= predictions_fogg2.withColumn("batch_date", date_format(last_day(add_months(current_date(), -1)), 'yyyy-MM-dd')) #-1 previous month

#export table ke hive
datamart_fogg2.write.mode("overwrite").format("parquet").saveAsTable("db_intensifikasi.datamart_fogg_last")