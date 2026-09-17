"""
Run this script to test your compute functions locally using the real CSV files
stored in the test/ directory.

Run as:

    python test/test_compute.py

This script loads the Hospital A and Hospital B CSV files, builds a local cohort
split inside each hospital, and executes the algorithm logic directly.
"""
import sys
import pandas as pd
from pathlib import Path

current_path = Path(__file__).parent
sys.path.insert(0, str(current_path.parent))

from differentialy_methylated_positions_regions.federated import rpc_compute_methylation
from differentialy_methylated_positions_regions.central import aggregate_results

HOSPITALS = [
    {"folder": "Hospital A", "file": "hospital_a_epic_v2.csv"},
    {"folder": "Hospital B", "file": "hospital_b_epic_v2.csv"}
]


def get_local_compute_function():
    """Return the undecorated local compute function for direct testing."""
    return rpc_compute_methylation.__wrapped__.__wrapped__


def split_samples(samples):
    """Split sample IDs into two cohort groups for a single hospital dataset."""
    samples_sorted = sorted(samples)
    midpoint = len(samples_sorted) // 2
    return samples_sorted[:midpoint], samples_sorted[midpoint:]


def main():
    compute_local = get_local_compute_function()

    resultados_simulados = []

    for hospital in HOSPITALS:
        csv_path = current_path / hospital["folder"] / hospital["file"]
        print(f"\n=== Loading real data from {hospital['folder']} ===")
        
        df = pd.read_csv(csv_path)
        sample_ids = sorted(df["sample_id"].unique().tolist())

        ids_cohort_a, ids_cohort_b = split_samples(sample_ids)
        print(f"Hospital: {hospital['folder']}")
        print(f"  Total samples: {len(sample_ids)}")
        print(f"  Cohort A: {ids_cohort_a}")
        print(f"  Cohort B: {ids_cohort_b}")

        result = compute_local(
            df,
            ids_cohort_a=ids_cohort_a,
            ids_cohort_b=ids_cohort_b,
            use_m_values=True,
        )

        if isinstance(result, dict) and "error" in result:
            print(f"  Error: {result['error']}")
            continue

        print(f"  Results for {hospital['folder']}: ")
        # Imprimir DMPs
        if result.get("top_dmps"):
            print("  --- Top DMPs ---")
            df_dmps = pd.DataFrame(result["top_dmps"])
            print(df_dmps.head())
        else:
            print("  No significant DMPs found.")
            
        # Imprimir DMRs
        if result.get("top_dmrs"):
            print("\n  --- Top DMRs ---")
            df_dmrs = pd.DataFrame(result["top_dmrs"])
            print(df_dmrs.head())
        else:
            print("\n  No significant DMRs found.")

        result['node_id'] = hospital['folder']
        resultados_simulados.append(result)

    # 2. This runs outside the loop: call the central function with the complete list.
    print("\n=== STARTING GLOBAL META-ANALYSIS (CENTRAL.PY) ===")
    resultado_global = aggregate_results(resultados_simulados)
    
    if "global_dmps" in resultado_global and resultado_global["global_dmps"]:
        df_global_dmps = pd.DataFrame(resultado_global["global_dmps"])
        print("\n--- TOP GLOBAL DMPs (FISHER'S METHOD) ---")
        print(df_global_dmps.head(15).to_string(index=False))
    else:
        print("No global DMPs found or an aggregation error occurred.")

    if "global_dmrs" in resultado_global and resultado_global["global_dmrs"]:
        df_global_dmrs = pd.DataFrame(resultado_global["global_dmrs"])
        print("\n--- DETECTED METHYLATED REGIONS (DMRs) ---")
        if "cohorte[T.Cohorte_B]_p_value_adjusted" in df_global_dmrs.columns:
            df_global_dmrs = df_global_dmrs.sort_values(
                "cohorte[T.Cohorte_B]_p_value_adjusted",
                ascending=True,
                na_position="last",
            )
        print(df_global_dmrs.head(20).to_string(index=False))

if __name__ == "__main__":
    main()

"""
This is a fake simulation of the central Vantage6 orchestrator, which aggregates results from multiple hospitals.
def run_central_local_federated_test():
    # Simulate the central Vantage6 orchestrator locally.
    compute_local = get_local_compute_function()
    node_results = []

    for hospital in HOSPITALS:
        csv_path = current_path / hospital["folder"] / hospital["file"]
        df = pd.read_csv(csv_path)
        sample_ids = sorted(df["sample_id"].unique().tolist())
        ids_cohort_a, ids_cohort_b = split_samples(sample_ids)

        result = compute_local(
            df,
            ids_cohort_a=ids_cohort_a,
            ids_cohort_b=ids_cohort_b,
            use_m_values=True,
        )

        if isinstance(result, dict) and "error" in result:
            continue

        result = dict(result)
        result["node_id"] = hospital["folder"]
        node_results.append(result)

    class FakeOrganizationClient:
        def list(self):
            return [{"id": 1}, {"id": 2}]

    class FakeTaskClient:
        def __init__(self, results):
            self._results = results

        def create(self, method, organizations, arguments):
            return {"id": "fake-task-123"}

        def wait_for_results(self, task_id):
            return self._results

    class FakeAlgorithmClient:
        def __init__(self, results):
            self.organization = FakeOrganizationClient()
            self.task = FakeTaskClient(results)

    client = FakeAlgorithmClient(node_results)

    return aggregate_results(node_results)


def run_central_local_federated_test():
    # Simulate the central Vantage6 coordinator using hospital folders as node datasets.
    node_results = []

    for hospital in HOSPITALS:
        csv_path = current_path / hospital["folder"] / hospital["file"]
        df = pd.read_csv(csv_path)
        sample_ids = sorted(df["sample_id"].unique().tolist())
        ids_cohort_a, ids_cohort_b = split_samples(sample_ids)

        result = get_local_compute_function()( 
            df,
            ids_cohort_a=ids_cohort_a,
            ids_cohort_b=ids_cohort_b,
            use_m_values=True,
        )

        if isinstance(result, dict) and "error" in result:
            continue

        result = dict(result)
        result["node_id"] = hospital["folder"]
        node_results.append(result)

    class FakeOrganizationClient:
        def list(self):
            return [{"id": 1}, {"id": 2}]

    class FakeTaskClient:
        def __init__(self, results):
            self._results = results

        def create(self, method, organizations, arguments):
            return {"id": "fake-task-123"}

        def wait_for_results(self, task_id):
            return self._results

    class FakeAlgorithmClient:
        def __init__(self, results):
            self.organization = FakeOrganizationClient()
            self.task = FakeTaskClient(results)

    client = FakeAlgorithmClient(node_results)
    fake_ids_cohort_a = ["A001", "A002", "A003", "A004"]
    fake_ids_cohort_b = ["A005", "A006", "A007", "A008", "A009"]

    return central_methylation_analysis(
        client,
        fake_ids_cohort_a,
        fake_ids_cohort_b,
        use_m_values=True,
    )


if __name__ == "__main__":
    main()
    print("\n=== Local central simulation (hospitals as nodes) ===")
    print(run_central_local_federated_test())
    print("\n=== Central aggregation simulation ===")
    print(run_central_local_federated_test())

"""