This is an in-memory cluster computing framework designed for large scale spatial data analysis. It consists of 3 
layers - Apache Spark, Spatial RDD layer and Spatial Query processing layer. 

Spatial RDD layer consists of Spatial Resilient distirubuted datasets (SRDDs) which extend regular Spark RDDs. These 
SRDDs provide support for geometrical operations (Overlap & Intersect). 

Spatial Query Processing layer executes spatial query processing algorithms (Spatial Range and JOIN) on SRDDs. GeoSpark on the 
fly decides whether a spatial index needs to be created on SRDD partition to achieve run time performance.

SRDDs offer support for spatial operations(geometric operations that follow Open Geospace Consortium - OGC). SRDD types -
Point RDD and Polygon RDD : provide support for geometrical and distance operations.



<img width="421" alt="screen shot 2018-06-24 at 4 12 16 pm" src="https://user-images.githubusercontent.com/5779462/41818359-cd11d272-77ca-11e8-8c7d-65fe6fb26c65.png">

## SRDD Partitioning:
Create a global grid file. Split the spatial space (SRDD) into number of equal geographical size grid cells  which compose a 
global grid file. Then traverse each element of SRDD and assign this element to the grid cell if the element overlaps with the
grid cell. 

/* Step1:Create a global grid file has N grids
*/
Find the minimum geo-boundary for two inputs; Createagridfile; /*Eachgridhasequalgeo-size */
/* Step2:Assign gridID to each element */ foreach spatial object in the SRRDs do
for grid = 0 to N do
if this grid contains / intersects with this Spatial Object then
Assign this grid ID to this Spatial Object;
end
/* Duplicates happen when one spatial object intersects with multiple grids */
end 
end

## Spatial Query Processing
Leverage grid partitioned SRDDs and Spark's in-memory computation model. Spatial indexes based out of Quad-tree/R-tree are
provided. Developers can specify if local spatial index needs to be considered (based on tradeoff between indexing overhead
and number of spatial objects). Sometimes if the indexing is an overhead, GeoSpark executes a full spatial object scan 
or nested loops( for some SRDDs that have few spatial objects).
