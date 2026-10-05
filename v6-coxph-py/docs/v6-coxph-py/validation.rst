Validation
==========

The federated fit was cross-checked against a centralized
``lifelines.CoxPHFitter`` fit (Breslow ties) on the same pooled data
(the HEAD-NECK-RADIOMICS-HN1 clinical dataset, split across 2 mock
organizations, ``time_col="overall_survival_in_days"``,
``outcome_col="event_overall_survival"``,
``expl_vars=["clin_n_1", "index_tumour_location_oropharynx"]``).
Coefficients, standard errors, and AIC matched to 2+ decimal places
(AIC 661.152 federated vs. 661.15 centralized). This case is the
``test_happy_path`` regression test in ``test/test_compute.py``.
