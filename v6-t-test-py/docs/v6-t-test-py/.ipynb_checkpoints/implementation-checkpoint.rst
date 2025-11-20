Implementation
==============

Overview
--------

Central (``central``)
-----------------
The central part is responsible for the orchestration and aggregation of the algorithm.

The central function is responsible for the creation of partial tasks for each 
organization. Once each organization has completed its assigned task, the central
function collects and aggregates the results to compute the overall *t* value for the 
**Independant-samples t test**.


Partials
--------
Partials are the computations that are executed on each node. The partials have access
to the data that is stored on the node. The partials are executed in parallel on each
node.

``partial``
~~~~~~~~~~~~~~~~

This function computes the mean, count (number of observations) and sample variance for 
numerical columns.

