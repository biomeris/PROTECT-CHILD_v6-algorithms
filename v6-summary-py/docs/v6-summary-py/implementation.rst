Implementation
==============

Overview
--------

This is a two-step algorithm. In the first step, the algorithm computes the basic
statistics on each node. The standard deviation is computed after a second computation
step by all nodes. In this step, they use the global mean to compute the variance.

.. uml::

  !theme superhero-outline

  caption The central part of the algorithm is responsible for the \
          orchestration and aggregation\n of the algorithm. The partial \
          parts are executed on each node.

  |client|
  :request analysis;

  |central|
  :Collect organizations
  in collaboration;
  :Create partial tasks;

  |partial|
  :summary_per_data_station computes
  most of the statistics;

  |partial|
  :Filter results using
  privacy settings;

  |central|
  :Combine basic statistics,
  compute mean per column;
  :Create new partial tasks;

  |partial|
  :variance_per_data_station computes
  the variance using the mean;

  |partial|
  :Filter results using
  privacy settings;

  |central|
  :Compute standard deviation;

  |client|
  :Receive results;


Partials
--------
Partials are the computations that are executed on each node. The partials have access
to the data that is stored on the node. The partials are executed in parallel on each
node.

``summary_per_data_station``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function computes the min, max, count, sum, and the number of missing values for
each numerical column. For categorical columns, the function computes the count, the
number of missing values, and the count of each unique value. Finally, it computes the
number of rows in the dataset that do not have any missing values.

This partial function includes several privacy checks - see the
:ref:`privacy guards <privacy-guards>` section for more information.

``variance_per_data_station``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This function receives the mean of each column over all nodes and uses that to compute
the variance. The function checks first if the node admin allows sharing the variance -
see the :ref:`privacy guards <privacy-guards>` section for more information.

Central (``summary``)
-----------------

The central part of the summary algorithm is responsible for the aggregation of the
results of the partial computations. The central part is responsible for the following
tasks:

1. Collect organizations in collaboration.
2. Create partial tasks for each organization.
3. Combine the basic statistics and compute the mean per column.
4. Create new partial tasks for the variance computation.
5. Compute the standard deviation.
6. Send results back to the server.