Privacy
=======

Guards
------

This algorithm never reads node data - it has no ``@dataframe`` argument and no
``AlgorithmClient``, so it cannot query a node's database or dispatch a subtask
to one. Consequently it does not need (and does not implement) the kind of
minimum-record-count guard used elsewhere in this repository, e.g.
``FISHER_MINIMUM_NUMBER_OF_RECORDS`` in ``v6-fisher-exact-test-py`` - there is no
record count to threshold against.

The only "guard" this algorithm performs is input validation on the p-values,
``alpha``, and ``num_tests`` arguments (see :doc:`implementation`), which exists
to catch caller mistakes (e.g. an out-of-range p-value, or a ``num_tests`` smaller
than the number of p-values supplied that would understate the correction). This
is a correctness guard, not a privacy guard.

Data sharing
------------

None. The only inputs are p-values, ``alpha``, and ``num_tests``, all supplied
directly by the requester as task arguments - typically copied by hand or by
another script from the output of a prior federated task. No data is requested
from or shared by the nodes when running this algorithm.

Because the task carries no reference to node data, running it does **not**
require any node in the collaboration to execute anything at all - the "central"
step is the entire computation, and it can, in principle, be run by the
requester's own organization without dispatching to any data-holding node.

Vulnerabilities to known attacks
---------------------------------

The standard federated-analysis attack surface (reconstruction, differencing,
model inversion, gradient leakage, etc.) does not apply here, because this
algorithm never touches node-local data or model parameters. The residual
privacy consideration is upstream: if the p-values passed in were themselves
computed from a very small stratum at one site (e.g. a single-site p-value from
a rare subgroup), that disclosure risk was already present in the algorithm that
produced the p-value, not introduced by this correction step.

.. list-table::
    :widths: 25 10 65
    :header-rows: 1

    * - Attack
      - Risk eliminated?
      - Risk analysis
    * - Reconstruction
      - ✔
      - No node data is accessed; nothing to reconstruct.
    * - Differencing
      - ✔
      - No per-node queries are made, so there is no way to difference two task
        results against each other.
    * - Deep Leakage from Gradients (DLG)
      - ✔
      - Not applicable; no model or gradients are involved.
    * - Generative Adversarial Networks (GAN)
      - ✔
      - Not applicable; no model is trained.
    * - Model Inversion
      - ✔
      - Not applicable; no model is involved.
    * - Watermark Attack
      - ✔
      - Not applicable; no model is involved.
