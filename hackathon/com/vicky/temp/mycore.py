from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local[2]").appName("WD30 Spark Core Prog learning").enableHiveSupport().getOrCreate()
sc = spark.sparkContext  # we are not instantiating rather just referring/renaming
sc.setLogLevel("ERROR")

insuredata = sc.textFile("file:///home/hduser/hack/insuranceinfo1.csv")
print(insuredata.collect())