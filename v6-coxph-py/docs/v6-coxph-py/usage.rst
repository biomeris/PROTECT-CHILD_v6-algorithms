Usage
=====

Central function: ``coxph``
----------------------------

.. code-block:: python

    client.task.create(
        method="coxph",
        arguments={
            "time_col": "overall_survival_in_days",
            "outcome_col": "event_overall_survival",
            "expl_vars": ["clin_n_1", "index_tumour_location_oropharynx"],
            "organizations_to_include": None,       # defaults to all organizations
            "category_reference_values": None,      # e.g. {"tumour_stage": "I"}
        },
        organizations=[central_organization_id],
        databases=[{"type": "dataframe", "dataframe_id": dataframe_id}],
    )

Arguments
---------

``time_col``
    Column holding the numeric time-to-event (or time-to-censoring).

``outcome_col``
    Column holding the event indicator - boolean (``True``/``False``) or the
    common ``0``/``1`` integer encoding, where ``1`` means the event occurred.

``expl_vars``
    List of explanatory variables (covariates) for the model. A column with
    pandas ``category`` dtype is automatically one-hot encoded (indicator
    columns named ``column[T.level]``, alphabetically-first level dropped as
    the reference by default); everything else must be numeric.

``organizations_to_include``
    Optional list of organization IDs to include; defaults to every
    organization in the collaboration.

``category_reference_values``
    Optional mapping from a categorical column name to the level that should
    be used as the dropped reference category instead of the
    alphabetically-first one.

Return value
------------

A dict with:

- ``included_organizations`` / ``excluded_organizations`` - which
  organizations contributed to the fit, and which were excluded for not
  meeting the minimum local event count.
- ``model`` - the fitted coefficient table (``Coef``, ``Exp(coef)``, ``SE``,
  ``lower_CI``, ``upper_CI``, ``Z``, ``p-value``, indexed by variable name) as
  a JSON string, or ``None`` if the model could not be fit.
- ``overall_p_value`` - Wald test p-value for overall model significance.
- ``aic`` - Akaike Information Criterion.
- ``degrees_of_freedom`` - number of model parameters.
- ``converged`` - whether the Newton-Raphson iteration converged within the
  epoch limit.
- ``iterations`` - number of Newton-Raphson epochs performed.
- ``warnings`` - any data-quality warnings (e.g. suspected perfect
  separation for a covariate).
