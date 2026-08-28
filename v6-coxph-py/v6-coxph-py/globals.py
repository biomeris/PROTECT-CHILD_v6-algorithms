""" Global variables for the coxph package. """

COXPH_MINIMUM_EVENTS = 10
COXPH_MAX_EPOCHS = 25
COXPH_TOLERANCE = 1e-6
COXPH_MAX_THRESHOLD_RETRIES = 3

# Below this, the *global* (cross-organization) risk set at some unique event
# time is considered small enough that its S0/S1/S2 aggregate could plausibly
# be used to infer an individual subject's covariates. Fits touching a risk
# set this small get an explicit warning (see docs/v6-coxph-py/privacy.rst).
COXPH_MINIMUM_RISK_SET_SIZE = 3
