from configparser import *
from com.vicky.commons.commonutils import *

def readPropertyFile(propfile):
    config = ConfigParser()
    config.read(propfile)
    global appname
    global process
    global jdbclib
    global insinfo1
    global insinfo2
    global output_path
    appname = config.get("CONFIGS",'name')
    process = config.get("CONFIGS",'master')
    jdbclib = config.get("CONFIGS",'jdbclib')
    insinfo1= config.get("FILEPATH",'insinfo1')
    insinfo2= config.get("FILEPATH",'insinfo2')
    output_path= config.get("FILEPATH",'outpath')


def main():
    print("Spark Hackathon Starts")
    propfile = "/home/hduser/PycharmProjects/hackathon/conn.prop"
    readPropertyFile(propfile)
    spark = getSparkSession(process,appname,jdbclib,"Y")
    print(spark)
    sc=spark.sparkContext
#1 - Load the file
    insureddatardd = sc.textFile(insinfo1)
    print(insureddatardd.count())
#2 - remove header
    insureddatardd_noheader = removeHeaderInRdd(insureddatardd)
    print(insureddatardd_noheader.count())
#3 - remove trailer
    insureddatardd1 = removeTrailer(insureddatardd_noheader)
    print(insureddatardd1.count())
    insureddatardd2 = insureddatardd1.map(lambda x:x[0])
#5 - remove blank lines
    insureddatardd3 = insureddatardd2.filter(lambda x:len(x.strip())!=0)
    print(insureddatardd3.count())
#6 - split records
    insureddatardd_split=insureddatardd3.map(lambda x:x.split(",",-1))
#7 - filter number of fiedls equal to 10 only
    insureddatardd4 = insureddatardd_split.filter(lambda x:len(x)==10)
    print(insureddatardd4.count())
#8 - original count and removed/rejected count
    originalcount = insureddatardd.count()
    updatedcount = insureddatardd4.count()
    print(f'Rejected/Removed number of records - {originalcount-updatedcount}')
#9 - filter only the rejected data to new rdd of fiedls not equal to 10 only
    rejectdata = insureddatardd_split.filter(lambda x:len(x)!=10).map(lambda x:(len(x),x))
    print(rejectdata.count())
    print(rejectdata.collect())

    print(f'processing of insurance 1 is completed')
#********* Insurance file 2 data ********************************************************
#10 - Lod
    insured2datardd = sc.textFile(insinfo2)
    print(insured2datardd.count())
#11-2 - remove header
    insured2datardd_noheader = removeHeaderInRdd(insured2datardd)
    print(insured2datardd_noheader.count())
#11-3 - remove trailer
    insured2datardd1 = removeTrailer(insured2datardd_noheader)
    print(insured2datardd1.count())
    insured2datardd2 = insured2datardd1.map(lambda x:x[0])
#11-4 - remove blank lines
    insured2datardd3 = insured2datardd2.filter(lambda x:len(x.strip())!=0)
    print(insured2datardd3.count())
#11-6 -  split records
    insured2datardd_split=insured2datardd3.map(lambda x:x.split(",",-1))
#11-7 - filter number of fiedls equal to 10 only
    insured2datardd4 = insured2datardd_split.filter(lambda x:len(x)==10)
    print(insured2datardd4.count())
#11-8 - original count and removed/rejected count
    originalcount = insured2datardd.count()
    updatedcount = insured2datardd4.count()
    print(f'Rejected/Removed number of records - {originalcount-updatedcount}')
#11-9 - filter only the rejected data to new rdd of fiedls not equal to 10 only
    rejectdata = insured2datardd_split.filter(lambda x:len(x)!=10).map(lambda x:(len(x),x))
    print(rejectdata.count())
    print(rejectdata.collect())

#11 - filter null or blank in issuerid, issuerid2
    insured2datafilteredrdd = insured2datardd4.filter(lambda x:x[0]!="" or x[1]!="")
    print(insured2datafilteredrdd.count())
#Data merging , Deduplicationn, Performance tuning, Persistance

#12 - merge both the file RDD ins1 and ins2
    insureddatamerged = insureddatardd4.union(insured2datafilteredrdd)
#13 - persist to memory
    from pyspark import StorageLevel
    #insureddatamerged.cache()
    insureddatamerged.persist(StorageLevel.MEMORY_ONLY)
#14 - rdd count validation
    count_ins1 = insureddatardd4.count()
    count_ins2 = insured2datafilteredrdd.count()
    count_merged = insureddatamerged.count()
    if count_merged == (count_ins1+count_ins2):
        print(f'Merged count is matching - {count_merged} = {count_ins1} and {count_ins2} ')

#15 - increase the partition 8
    insureddatarepart = insureddatamerged.repartition(8)
    print(f' revised partition - {insureddatarepart.getNumPartitions()}')
    print(f'count - {insureddatarepart.count()}')

#16 - split based on business date
    rdd_20191001 = insureddatarepart.filter(lambda x:x[2]=='2019-10-01' or x[2]=='01-10-2019')
    rdd_20191002 = insureddatarepart.filter(lambda x:x[2]=='02-10-2019')

    print(f'10/01 - {rdd_20191001.count()} 10/02 - {rdd_20191002.count()} ')

#TT - date wise count
    datewisecount = insureddatarepart.map(lambda x:(x[2],1)).reduceByKey(lambda x,y:x+y)
    print(datewisecount.collect())
#17 - save in hdfs path
    insureddatarepart.saveAsTextFile(output_path)
    #rdd_20191001.saveAsTextFile(output_path+"/20191001")
    #rdd_20191002.saveAsTextFile(output_path+"/20191002")
#18 - convert to df
    cols=["IssuerId","IssuerId2","BusinessDate","StateCode","SourceName","NetworkName","NetworkURL","custnum","MarketCoverage","DentalOnlyPlan"]
    #insureddatarepartdf=insureddatarepart.map(lambda x:(x[0],x[1],x[2])).toDF(cols)
    insureddatarepartdf = insureddatarepart.toDF(cols)
    insureddatarepartdf.show()





if __name__ == '__main__':
    main()