# Description

The tool determines for each city address whether or not a public green area is accessible within a distance of 300 meters along the road network. The tool has two components: a QGIS modeler (to identify the access points of the green areas) and a python-based algorithm (to calculate the distance between access points and addresses)


# Input data requirements

* City addresses, vector dataset with point geometry
* Public green areas, vector dataset with polygon geometry
* Known access points, vector dataset with point geometry
* Road network, vector dataset with linestring geometry


# Output data requirements

* Input dataset "City addresses" with the addition of the following fields:
   * "distanza_m": the distance in meters to the closest access point (only if within the desired 300 meters, otherwise it is null)
   * "is_300": the boolean flag, true if the distance in meters to the closest access point along the road network is within 300 metrs


# Objective

The purpose is to support decision makers planning to improve the liveability of cities by identifying the addresses where the criterion is met and the ones where it is not met, and by what amount.


# Use of resource

**First component**
* Extract access points within 10 m from public green areas and flag them as "type A" gates
* Linearize the polygons of the public green areas
* Intersect the elements of the road network with the linearized green areas and flag them as "type B" gates
* Select all the public green areas that are NOT within 10 meters of any ype A or B gate
* Extract one point every 100 meters along the linearization of such public green areas and flag them as "type C" gates
* Merge together type A, B and C gates

**Second component**
* Determine for each address the group of gates that intersect a buffer of radius 300 meters around it
* Determine for each address the distance required to reach the previosly identified gates along the road network 
* The criterion is passed as soon as one of such distances along the road network is less or equal to 300 meters
* If none of the previosly identified gates is reachable within such distance, the criterion is not passed


# Constraints
None: input data are commonly available. 
They can all be derived from OSM almost everywhere. If accurate dataset is locally available, for example from the municipality, the results improve.













