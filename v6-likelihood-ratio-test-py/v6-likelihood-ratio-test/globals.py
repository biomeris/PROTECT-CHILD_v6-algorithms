# Minimum number of eligible records a node must hold before it will
# contribute at all. "Eligible" means: outcome present, and every covariate in
# the full covariate list present (the same complete-case sample is used for
# the full model and every reduced model, so the likelihood ratio comparison
# is always taken over identical data).
LIKELIHOOD_RATIO_TEST_MINIMUM_NUMBER_OF_RECORDS = 10

# Minimum number of eligible records a node must hold in *each* outcome group
# (event and non-event) before it will contribute. A node whose gradient and
# Fisher information are dominated by a couple of event records would reveal
# more about those specific records than a normal aggregate.
LIKELIHOOD_RATIO_TEST_MINIMUM_GROUP_SIZE = 5

# Safety cap on Newton-Raphson iterations per model fit. Logistic regression's
# log-likelihood is concave, so this typically converges in well under 10
# iterations; this bound exists to fail loudly (rather than loop silently) on
# separation or near-collinearity.
LIKELIHOOD_RATIO_TEST_MAX_ITERATIONS = 25

# Convergence tolerance on the score (gradient of the log-likelihood, in
# standardized covariate units). Iteration stops once every entry is below
# this, or once the Newton step itself is smaller than 1e-8.
LIKELIHOOD_RATIO_TEST_TOLERANCE = 1e-6
