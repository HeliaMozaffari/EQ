from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
import matplotlib.pyplot as plt
import numpy as np
import pandas

#Creating a session
spark = SparkSession \
    .builder \
    .appName("EQData") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#importing data
dataSample = spark.read.csv(
    "C:/Users/16472/Desktop/DATA/SampleData.csv", header=True)

#data after cleaning duplicates
dS = dataSample.dropDuplicates([' TimeSt','Latitude', 'Longitude'])

#Labeling: I used the difference between  pois longitude and latitude
#measure longitude and latitude and made sure distance is less than mean (because it enters the other range)
#absolute value because it is the length
#ignored P02 as has the same value as P01
dS = dS.withColumn("POIID",when((abs(dS.Latitude - 45.22483) <= 0.1483995) & (abs(dS.Longitude + 63.232729) <= 5.1666475), lit('PO4'))
                          .when((abs(dS.Latitude - 45.521629) <= 4.012269) & (abs(dS.Longitude + 73.566024) <= 19.959855), lit('PO3'))
                          .when((abs(dS.Latitude - 53.546167) <= 4.012269) & (abs(dS.Longitude + 113.485734) <= 19.959855), lit('PO1'))
                          .otherwise('Not sure'))

#Analysis 1)
#calculate the distance for each row and place it in another column
dS4 = dS.filter(dS.POIID == lit('PO4'))
dS4 = dS4.withColumn("Lat_Dist", (abs(dS.Latitude - 45.22483)))
dS4 = dS4.withColumn("Lon_Dist", (abs(dS.Longitude + 63.232729)))
#describe for P04
dS4.describe("Lon_Dist", "Lat_Dist").show()

dS3 = dS.filter(dS.POIID == lit('PO3'))
dS3 = dS3.withColumn("Lat_Dist", (abs(dS.Latitude - 45.521629)))
dS3 = dS3.withColumn("Lon_Dist", (abs(dS.Longitude + 73.566024)))
#describe for P03
dS3.describe("Lon_Dist", "Lat_Dist").show()

dS1 = dS.filter(dS.POIID == lit('PO1'))
dS1 = dS1.withColumn("Lat_Dist", (abs(dS.Latitude - 53.546167)))
dS1 = dS1.withColumn("Lon_Dist", (abs(dS.Longitude + 113.485734)))
#describe for P01
dS1.describe("Lon_Dist", "Lat_Dist").show()
dS2 = dS.filter(dS.POIID == lit('Not sure'))

#Analysis 2)
#display density for each point
#each point has a different color

x1 = dS1.toPandas()['Longitude'].values.tolist()
y1 = dS1.toPandas() ['Latitude'].values.tolist()
plt.scatter(x1,y1, color = 'red', alpha=0.5)

x2 = dS2.toPandas()['Longitude'].values.tolist()
y2 = dS2.toPandas() ['Latitude'].values.tolist()
plt.scatter(x2,y2, color = 'pink', alpha=0.5)

x3 = dS3.toPandas()['Longitude'].values.tolist()
y3 = dS3.toPandas() ['Latitude'].values.tolist()
plt.scatter(x3,y3, color = 'yellow', alpha=0.5)

x4 = dS4.toPandas()['Longitude'].values.tolist()
y4 = dS4.toPandas() ['Latitude'].values.tolist()
plt.scatter(x4,y4, color = 'green', alpha=0.5)
#display POI with their correct radiance

y2 = np.array([45.22483,45.521629, 53.546167])
x2 = np.array([-63.232729, -73.566024, -113.485734])
#sizes calculated from longitude and latitude from labeling part
sizes = np.array([5.169, 20.359, 20.359 ])
plt.scatter(x2,y2, s=sizes, alpha=0.5, color = 'blue')

plt.show()