from configparser import *
from com.vicky.commons.commonutils import *
from com.vicky.hack.allmethods import remspecialchar,converttoupper
from pyspark.sql.functions import to_date,concat,col,current_date,current_timestamp,udf

def readPropertyFile(propfile):
    config = ConfigParser()
    config.read(propfile)
    global appname
    global process
    global jdbclib
    global insinfo1
    global insinfo2
    global output_path
    global custstate
    global url
    global driver
    appname = config.get("CONFIGS",'name')
    process = config.get("CONFIGS",'master')
    jdbclib = config.get("CONFIGS",'jdbclib')
    insinfo1= config.get("FILEPATH",'insinfo1')
    insinfo2= config.get("FILEPATH",'insinfo2')
    output_path= config.get("FILEPATH",'outpath')
    custstate=config.get("FILEPATH",'custstate')
    url=config.get("JDBC",'url')
    driver=config.get("JDBC",'driver')

def main():
    print("Spark Hackathon Starts - Spark core ")
    propfile = "/home/hduser/PycharmProjects/hackathon/conn.prop"
    readPropertyFile(propfile)
    spark = getSparkSession(process,appname,jdbclib,"Y")
    print(spark)
    sc=spark.sparkContext
# define structure type
# 19A
    from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType
    insurancestructtype1 = StructType([StructField("IssuerId",IntegerType(),False),
                                       StructField("IssuerId2",IntegerType(),False),
                                       StructField("BusinessDate",DateType(),False),
                                       StructField("StateCode",StringType(),False),
                                       StructField("SourceName",StringType(),False),
                                       StructField("NetworkName",StringType(),False),
                                       StructField("NetworkURL",StringType(),False),
                                       StructField("custnum",StringType(),False),
                                       StructField("MarketCoverage",StringType(),False),
                                       StructField("DentalOnlyPlan",StringType(),False)])
# 19B
    insurancestructtype2 = StructType([StructField("IssuerId",IntegerType(),False),
                                       StructField("IssuerId2",IntegerType(),False),
                                       StructField("BusinessDate",StringType(),False),
                                       StructField("StateCode",StringType(),False),
                                       StructField("SourceName",StringType(),False),
                                       StructField("NetworkName",StringType(),False),
                                       StructField("NetworkURL",StringType(),False),
                                       StructField("custnum",StringType(),False),
                                       StructField("MarketCoverage",StringType(),False),
                                       StructField("DentalOnlyPlan",StringType(),False)])
# 19C
    insurancestructtype3 = StructType([StructField("IssuerId",IntegerType(),False),
                                       StructField("IssuerId2",IntegerType(),False),
                                       StructField("BusinessDate",StringType(),False),
                                       StructField("StateCode",StringType(),False),
                                       StructField("SourceName",StringType(),False),
                                       StructField("NetworkName",StringType(),False),
                                       StructField("NetworkURL",StringType(),False),
                                       StructField("custnum",StringType(),False),
                                       StructField("MarketCoverage",StringType(),False),
                                       StructField("DentalOnlyPlan",StringType(),False),
                                       StructField("RejectRows",StringType(),False)])

    #insurance1df = spark.read.csv(insinfo1,header=True,sep=",",inferSchema=True)
#20 - create df using schema and drop
    insurance1df = spark.read.csv(insinfo1,schema=insurancestructtype1,header=True,mode='dropmalformed')
    insurance1df.printSchema()
    print(f'Insurance 1 count - {len(insurance1df.collect())}')

    insurance2df = spark.read.csv(insinfo2,schema=insurancestructtype2,header=True,mode='dropmalformed')
    insureahce2df_fmt=insurance2df.withColumn("BusinessDate",to_date("BusinessDate",'MM-dd-yyyy'))
    insureahce2df_fmt.printSchema()
    print(f'Insurance 2 count - {len(insureahce2df_fmt.collect())}')
    print(insureahce2df_fmt.show(3))

    insurance2df_alldata = spark.read.csv(insinfo2,schema=insurancestructtype3,header=True,mode='permissive',
                                          columnNameOfCorruptRecord="RejectRows",
                                          ignoreLeadingWhiteSpace=True,
                                          ignoreTrailingWhiteSpace=True)
    insurance2df_corrupt = insurance2df_alldata.where("RejectRows is not null")
    #print(f'Number of rejected records - {len(insurance2df_corrupt.collect())}')
    print(insureahce2df_fmt.show())

#21(a) - rename column statecode and sourcenameh
    insurance1df_upd = insurance1df.withColumnRenamed("StateCode","stcd").withColumnRenamed("SourceName","srcnm")
    insurance2df_upd = insurance2df.withColumnRenamed("StateCode","stcd").withColumnRenamed("SourceName","srcnm")
    insurance2df_corrupt_upd = insurance2df_corrupt.withColumnRenamed("StateCode","stcd").withColumnRenamed("SourceName","srcnm")

#21(b,c) - concat issuerid's and drop
    insurance1df_upd1 = insurance1df_upd.withColumn("IssuerIdComposite",concat(col("IssuerId").cast("string"),col("IssuerId2").cast("string"))).drop("DentalOnlyPlan")
    insurance2df_upd1 = insurance2df_upd.withColumn("IssuerIdComposite",concat(col("IssuerId").cast("string"),col("IssuerId2").cast("string"))).drop("DentalOnlyPlan")

    print(insurance1df_upd1.show(3))

#21(d)
    insurance1df_upd2 = insurance1df_upd1.withColumn("sysdt",current_date()).withColumn("systs",current_timestamp())
    insurance2df_upd2 = insurance2df_upd1.withColumn("sysdt",current_date()).withColumn("systs",current_timestamp())
    insurancemergedf = insurance1df_upd2.union(insurance2df_upd2)
    print(insurance1df_upd2.show(3))
    print(insurancemergedf.count())
# interesting usecase
    #print the list of column from the datafram
    list_of_columns = insurance1df.columns
    for column_name in list_of_columns:
        print(column_name)
    #print the list of column and its datatypes and extract only the int lumns
    list_of_dt = insurance1df.dtypes
    list_of_int_cols = []
    for column_name_dt_list in list_of_dt:
        print(f'Column Name is {column_name_dt_list[0]} and its data type is {column_name_dt_list[1]}')
        if column_name_dt_list[1] == 'int':
            list_of_int_cols.append(column_name_dt_list[0])
    print(f'list of integer columns - {list_of_int_cols}')

    #show only the int cols using the above list
    print(insurance1df.select(list_of_int_cols).show(10))

    #find the additional cols added in df3
    collist1 = insurance1df.columns
    collist2 = insurance2df_alldata.columns
    diff = set(collist2) - set(collist1)
    list_new_added_cols = list(diff)
    print(f'newly added column in datafram 3 - {list_new_added_cols}')

#22 - remove the rows contains null in anyone of the field nd count the number of rows which contains all col with somevalue
    insurance1df_upd1_cleanup = insurance1df_upd1.na.drop("any")
    print(insurance1df_upd1_cleanup.count())
    insurance2df_upd1_cleanup = insurance2df_upd1.na.drop("any")
    print(insurance2df_upd1_cleanup.count())

#24 - register user created func in spark
    #upperconvert = udf(converttoupper)
    remove_special_chars = udf(remspecialchar)
    #print(insurance1df.withColumn("MarketCoverage",upperconvert("MarketCoverage")).show(5))

#25 - apply the udf in NetworkName
    ins1_final = insurance1df_upd1_cleanup.withColumn("NetworkName",remove_special_chars("NetworkName"))
    ins1_final.show(5)
#26 - save the above one into hdfs ( json)
    writeToFile(ins1_final,"hdfs:///user/hduser/sparkhack2/jsonoutput","json",",","overwrite")
    print("Saved Json file")

#27 - save the above one into hdfs ( json)
    writeToFile(ins1_final,"hdfs:///user/hduser/sparkhack2/csvoutput","csv",",","overwrite")
    print("Saved csv file")

#28 - save the df into hive table
    writeHiveTable(ins1_final,"hack.insurance_hack","append")

#Tale of handling RDDs, DFs and TempViews (20% Completion) – Total 75%
#Loading RDDs, split RDDs, Load DFs, Split DFs, Load Views, Split Views, write UDF,
# register to use in Spark SQL, Transform, Aggregate, store in disk/DB

#29 - load custstates csv file into rdd
    custs_states_rdd = sc.textFile(custstate).map(lambda x:x.split(","))

#30  - split into two rdd cust and states
    custs_rdd = custs_states_rdd.filter(lambda x:len(x)==5)
    states_rdd = custs_states_rdd.filter(lambda x:len(x)==2)
    print("customer rdd")
    print(custs_rdd.take(5))
    print("state rdd")
    print(states_rdd.take(5))

#31 - load custstates into DF
    custs_states_schema = StructType([StructField("Column1",StringType(),False),
                                      StructField("Column2",StringType(),False),
                                      StructField("Column3",StringType(),False),
                                      StructField("Column4",StringType(),False),
                                      StructField("Column5",StringType(),False)])

    custs_states_df = spark.read.csv(custstate,schema=custs_states_schema)
#32 - split into two df cust and states
    custstemp1_df = custs_states_df.where("column3 is not null")
    custstemp2_df=custstemp1_df.withColumnRenamed("Column1","Custid").withColumnRenamed("Column2","fname").withColumnRenamed("Column3","lname")\
        .withColumnRenamed("Column4","age").withColumnRenamed("Column5","profession")
    custs_df=custstemp2_df.withColumn("age",col("age").cast("int"))
    custs_df.printSchema()
    statestemp_df = custs_states_df.where("column3 is null")
    states_df=statestemp_df.withColumnRenamed("Column1","statecode").withColumnRenamed("Column2","statedesc").drop("Column3")\
        .drop("Column4").drop("Column5")
    states_df.printSchema()
    print(f'Cust count : {custs_df.count()} + States count : {states_df.count()} = {custs_states_df.count()} ')

#33 - Register the above DF as temporary view
    custs_df.createOrReplaceTempView("custview")
    states_df.createOrReplaceTempView("statesview")

#34 - register the df created in 21 as tempview
    insurancemergedf.createOrReplaceTempView("insureview")

#35 - register the udf for sparksql
    spark.udf.register("removespecialchar_sql",remspecialchar)

#36 - write sql queries -
    spark.conf.set("spark.sql.shuffle.partitions",4)
#36(a) - use the udf on network name and add a new col in the end
    spark.sql("select *,removespecialchar_sql(NetworkName) as cleanNetworkName from insureview").show(10)
#36(b)(c)(d) add current date, current timestamp, extract month, year from businessdate
    spark.sql("select *,removespecialchar_sql(NetworkName) as cleanNetworkName,"
              "current_date as curdt,"
              "current_timestamp as curts,"
              "year(BusinessDate) as yr,"
              "month(BusinessDate) as mth,"
              "CASE when SPLIT(NetworkURL,':')[0] = 'http' THEN 'http non Secured' "
              "when SPLIT(NetworkURL,':')[0] = 'https' THEN 'http Secured'"
              "ELSE 'No protocol' END as Protocol "
              "from insureview").createOrReplaceTempView("insureview1")

    spark.sql("select * from insureview1").show(10)

#36(e) - join all thre tables
    combined_df = spark.sql("select insureview1.*,statesview.statedesc,custview.age,custview.profession "
              "from insureview1 "
              "inner join statesview "
              "on insureview1.stcd = statesview.statecode "
              "inner join custview "
              "on insureview1.custnum = custview.custid")

    combined_df.show(10,False)
    combined_df.createOrReplaceTempView("insurancedata")
#37 - write as parquet
    combined_df.write.parquet("hdfs:///user/hduser/sparkhack2/parquet",mode='overwrite')
    print("parquest saved")
    #writeToSqlTable(combined_df,url,"insurancedata","overwrite",driver)


#38 -
    finaldf = spark.sql("select seqno,statedesc,protocol,profession,Avgage,count"
              "from (select ROW_NUMBER() over(partition by protocol order by count desc) as seqno, statedesc, protocol, profession,avg(age) as Avgage , count(*) as count "
              "from insurancedata "
              "group by statedesc, protocol, profession) insure "
              "where protocol='http Secured and statedesc='Alaska' and seq=2")

    finaldf.show()

#39 - save to sql
    writeToSqlTable(finaldf,url,"insureaggregated","overwrite",driver)

if __name__ == '__main__':
    main()
