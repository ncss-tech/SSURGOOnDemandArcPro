# SSURGOOnDemandArcPro
SSURGO-OnDemand for ArcGIS Pro

User driven SSURGO properties and interpretations

DISCLAIMER The information returned by the SSURO On-Demand tools are provided "as is". Additionally, there is no expressed or implied accuracy of the data returned. Use at your own risk.

The purpose of these tools are to give users the ability to get Soil Survey Geographic Database (SSURGO) properties and interpretations in an efficient manner. They are very similiar to the United States Department of Agriculture - Natural Resource Conservation Service's Soil Data Viewer (SDV) application, although there are distinct differences. The most important difference is the data collected with the SSURGO On-Demand (SOD) tools are collected in real-time via web requests to Soil Data Access (https://sdmdataaccess.nrcs.usda.gov/). This means that the information collected is the most up-to-date possible.  SOD tools do not require users to have the data found in a traditional SSURGO download from the NRCS's official repository, Web Soil Survey (https://websoilsurvey.sc.egov.usda.gov/App/HomePage.htm). The main intent of both SOD and SDV are to hide the complex relationships of the SSURGO tables and allow the users to focus on asking the question they need to get the information they want. This is accomplished in the user interface of the tools and the subsequent SQL is built and executed for the user. Currently, the tools packaged here are designed to run within the ESRI ArcGIS Pro software and developed under version 2.8.3.

NOTE: The queries in these tools only consider the major components of soil map units.

There are currently 2 tools in this package, 1 for SSURGO properties and the other for SSURGO interpretations.  Both tools require the user to provide a feature layer based upon a WGS84, NAD83, or NAD83(2011) geographic coordinate system.  This feature layer determines the area of interest for which both SSURGO geometry and either property or interpretation are collected.  The feature layer must have a selection.  Even if there is only 1 feature in the layer, it must be selected. The output workspace is required to be a file geodatabase (gdb).  The geometry collected is in WGS84 (4326). Each property or interpretations requested will output an individual table.  Users have the option of updating the spatial attribute table with each property or interpretation requested.

It is very important to consider that Soil Data Access is limited in the number characters it can return.  Due to this, there is an unknown constraint on how large an AOI can be requested because the characters (coordinates/vertices) can reach this threshold fairly quickly. This is locally dependent on polygon (mapping) density and vertex density. When this threshold is exceeded Soil Data Access returns nothing which will cause SSURGO On-Demand tools to exit.   

<h2>Supplemental Information - Aggregation Method</h2>
Aggregation is the process by which a set of component attribute values is reduced to a single value to represent the map unit as a whole. A map unit is typically composed of one or more "components". A component is either some type of soil or some nonsoil entity, e.g., rock outcrop. The components in the map unit name represent the major soils within a map unit delineation. Minor components make up the balance of the map unit. Great differences in soil properties can occur between map unit components and within short distances. Minor components may be very different from the major components. Such differences could significantly affect use and management of the map unit. Minor components may or may not be documented in the database. The results of aggregation do not reflect the presence or absence of limitations of the components which are not listed in the database. An on-site investigation is required to identify the location of individual map unit components. For queries of soil properties, only major components are considered for Dominant Component (numeric) and Weighted Average aggregation methods (see below). Additionally, the aggregation method selected drives the available properties to be queried. For queries of soil interpretations, all components are condisered.
For each of a map unit's components, a corresponding percent composition is recorded. A percent composition of 60 indicates that the corresponding component typically makes up approximately 60% of the map unit. Percent composition is a critical factor in some, but not all, aggregation methods.

For the attribute being aggregated, the first step of the aggregation process is to derive one attribute value for each of a map unit's components. From this set of component attributes, the next step of the aggregation process derives a single value that represents the map unit as a whole. Once a single value for each map unit is derived, a thematic map for soil map units can be generated. Aggregation must be done because, on any soil map, map units are delineated but components are not.

The aggregation method "Dominant Component" returns the attribute value associated with the component with the highest percent composition in the map unit. If more than one component shares the highest percent composition, the value of the first named component is returned.

The aggregation method "Dominant Condition" first groups like attribute values for the components in a map unit. For each group, percent composition is set to the sum of the percent composition of all components participating in that group. These groups now represent "conditions" rather than components. The attribute value associated with the group with the highest cumulative percent composition is returned. If more than one group shares the highest cumulative percent composition, the value of the group having the first named component of the mapunit is returned.

The aggregation method "Weighted Average" computes a weighted average value for all components in the map unit. Percent composition is the weighting factor. The result returned by this aggregation method represents a weighted average value of the corresponding attribute throughout the map unit.

The aggregation method "Minimum or Maximum" returns either the lowest or highest attribute value among all components of the map unit, depending on the corresponding "tie-break" rule. In this case, the "tie-break" rule indicates whether the lowest or highest value among all components should be returned. For this aggregation method, percent composition ties cannot occur. The result may correspond to a map unit component of very minor extent. This aggregation method is appropriate for either numeric attributes or attributes with a ranked or logically ordered domain.
