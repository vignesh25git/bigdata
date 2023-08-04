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
    appname = config.get("CONFIGS",'name')
    process = config.get("CONFIGS",'master')
    jdbclib = config.get("CONFIGS",'jdbclib')
    insinfo1= config.get("FILEPATH",'insinfo1')
    insinfo2= config.get("FILEPATH",'insinfo2')

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






if __name__ == '__main__':
    main()