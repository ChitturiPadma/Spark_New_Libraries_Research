This is an in-memory cluster computing framework designed for large scale spatial data analysis. It consists of 3 
layers - Apache Spark, Spatial RDD layer and Spatial Query processing layer. 

Spatial RDD layer consists of Spatial Resilient distirubuted datasets (SRDDs) which extend regular Spark RDDs. These 
SRDDs provide support for geometrical operations (Overlap & Intersect). 

Spatial Query Processing layer executes spatial query processing algorithms (Spatial Range and JOIN) on SRDDs. GeoSpark on the 
fly decides whether a spatial index needs to be created on SRDD partition to achieve run time performance.

SRDDs offer support for spatial operations(geometric operations that follow Open Geospace Consortium - OGC). SRDD types -
Point RDD and Polygon RDD : provide support for geometrical and distance operations.



<img width="421" alt="screen shot 2018-06-24 at 4 12 16 pm" src="https://user-images.githubusercontent.com/5779462/41818359-cd11d272-77ca-11e8-8c7d-65fe6fb26c65.png">

dcdcd
