"""
This file contains all central algorithm functions. It is important to note
that the central method is executed on a node, just like any other method.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled).
"""
from typing import Any, List
import pandas as pd
import numpy as np
from pathlib import Path
import os

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.algorithm_client import algorithm_client
from vantage6.algorithm.decorator.action import central
from vantage6.algorithm.client import AlgorithmClient

# Reference test data paths
TEST_DATA_DIR = Path(__file__).parent.parent / "test"
TEST_DATA_FILES = {
    "test_data_1": TEST_DATA_DIR / "test_data_1.csv",
    "test_data_2": TEST_DATA_DIR / "test_data_2.csv",
    "test_data_3": TEST_DATA_DIR / "test_data_3.csv",
}


@central
@algorithm_client
def central_function(
    client: AlgorithmClient, lista_relojes: List[str],
    df1: pd.DataFrame | None = None,
    # INDIVIDUAL_RESULTS: Add optional parameter to return individual sample ages from all organizations
    return_individual_results: bool = False
) -> dict:
    """
    Central aggregation of epigenetic age predictions from all organizations.
    
    Implements PRIVACY-PRESERVING AGGREGATION: Organizations return only aggregated
    statistics (mean, std, N), never individual patient data. Central node then
    calculates WEIGHTED MEANS accounting for organization size differences.
    
    Parameters:
    -----------
    df1 : pd.DataFrame
        Local beta values matrix (for reference/context)
    lista_relojes : List[str]
        List of clock names to calculate (e.g., ['horvath2013', 'hannum', 'phenoage'])
    # INDIVIDUAL_RESULTS: Document the optional parameter for individual results
    return_individual_results : bool, optional
        If True, also returns individual sample ages from each organization (default: False)
    
    Returns:
    --------
    dict : Global aggregated results with organization-level breakdown
        {
            "global_aggregates": {
                "horvath2013": {"weighted_mean": 45.5, "std_pooled": 8.1, "total_n": 15}
            },
            "organization_breakdown": {
                "0": {
                    "horvath2013": {"mean": 45.2, "std": 8.3, "min": 32.1, "max": 58.9, "N": 5}
                },
                "1": {...}
            },
            # INDIVIDUAL_RESULTS: Show structure when individual results are included
            "individual_results": {
                "0": {
                    "horvath2013": {"Sample_1": 45.2, "Sample_2": 46.1, ...}
                }
            }  # Only included if return_individual_results=True
        }
    """
    
    info("Retrieving organizations in collaboration")
    organizations = client.organization.list()
    org_ids = [org.get("id") for org in organizations]
    info(f"Found {len(org_ids)} organizations")
    
    # Create subtask for partial calculations
    info(f"Creating subtask to calculate clocks: {lista_relojes}")
    arguments = {
        "lista_relojes": lista_relojes,
        # INDIVIDUAL_RESULTS: Pass the individual results flag to partial function
        "return_individual_results": return_individual_results,
    }
    
    task = client.task.create(
        organizations=org_ids,
        method="federated_function",
        name="Epigenetic clock calculation",
        description="Calculate epigenetic age using multiple clocks (aggregated statistics only)",
        arguments=arguments,
        databases=[{"label": "default"}]
    )
    
    # Wait for results
    info(f"Waiting for aggregated results from {len(org_ids)} organizations")
    results = client.wait_for_results(task_id=task.get("id"))
    
    if not results:
        error("No results received from organizations")
        return {}
    
    info(f"Received aggregated results from {len(results)} organizations")
    
    # Calculate weighted aggregates and preserve organization-level results
    global_aggregates = {}
    organization_breakdown = {}
    # INDIVIDUAL_RESULTS: Initialize dictionary to store individual results from all organizations
    all_individual_results = {}
    
    for clk in lista_relojes:
        # Collect all organization statistics for this clock
        org_means = []
        org_stds = []
        org_ns = []
        
        for org_id, org_result in enumerate(results):
            # Store organization-level results
            if org_id not in organization_breakdown:
                organization_breakdown[str(org_id)] = {}
            
            # INDIVIDUAL_RESULTS: Extract aggregated stats from the new structure
            if isinstance(org_result, dict) and "aggregated" in org_result:
                aggregated_data = org_result["aggregated"]
                
                if clk in aggregated_data:
                    org_stats = aggregated_data[clk]
                    organization_breakdown[str(org_id)][clk] = org_stats
                    
                    # Extract values for weighted aggregation
                    if org_stats["N"] > 0 and not np.isnan(org_stats["mean"]):
                        org_means.append(org_stats["mean"])
                        org_stds.append(org_stats["std"])
                        org_ns.append(org_stats["N"])
            
            # INDIVIDUAL_RESULTS: Extract and store individual results if present
            if return_individual_results and isinstance(org_result, dict) and "individual" in org_result:
                # INDIVIDUAL_RESULTS: Initialize organization in individual results dictionary
                if str(org_id) not in all_individual_results:
                    # INDIVIDUAL_RESULTS: Store individual results for this organization
                    all_individual_results[str(org_id)] = org_result["individual"]
                    info(f"Stored individual results for organization {org_id}")
        
        # Calculate weighted mean if we have valid data
        if org_ns:
            total_n = sum(org_ns)
            # Weighted mean: sum(mean_i * N_i) / sum(N_i)
            weighted_mean = np.average(org_means, weights=org_ns)
            
            # Pooled standard deviation for meta-analysis
            # Using formula for combining SDs from multiple groups
            pooled_variance = sum(
                (n - 1) * (s ** 2) for n, s in zip(org_ns, org_stds)
            ) / (total_n - len(org_ns))
            pooled_std = np.sqrt(pooled_variance) if pooled_variance >= 0 else np.nan
            
            global_aggregates[clk] = {
                "weighted_mean": float(weighted_mean),
                "std_pooled": float(pooled_std),
                "total_n": int(total_n),
                "n_organizations": len(org_ns)
            }
            
            info(f"{clk}: weighted_mean={weighted_mean:.2f}, "
                 f"total_n={total_n}, n_orgs={len(org_ns)}")
        else:
            warn(f"{clk}: No valid data to aggregate")
            global_aggregates[clk] = {
                "weighted_mean": np.nan,
                "std_pooled": np.nan,
                "total_n": 0,
                "n_organizations": 0
            }
    
    info("Aggregation complete")
    
    # INDIVIDUAL_RESULTS: Build return dictionary with conditional individual results
    result = {
        "global_aggregates": global_aggregates,
        "organization_breakdown": organization_breakdown
    }
    
    # INDIVIDUAL_RESULTS: Optionally include individual results if collected
    if return_individual_results and all_individual_results:
        # INDIVIDUAL_RESULTS: Add individual sample ages to the response
        result["individual_results"] = all_individual_results
        info("Individual results included in central response")
    
    # Add organization means table
    org_means_table = create_organization_means_table(result)
    if not org_means_table.empty:
        result["organization_means_table"] = org_means_table.to_dict(orient='index')
        info("Organization means table included in central response")
    
    return result


def create_organization_means_table(central_results: dict) -> pd.DataFrame:
    """
    Create a formatted table showing the mean epigenetic ages for all organizations.
    
    Transforms the hierarchical central_results structure into a tabular format where
    each row represents an organization and columns represent different epigenetic clocks.
    
    Parameters:
    -----------
    central_results : dict
        Results dictionary from central_function containing:
        - 'organization_breakdown': Organization-level stats
        - 'global_aggregates': Global weighted statistics
    
    Returns:
    --------
    pd.DataFrame : Table with organizations as rows, clocks as columns, and means as values
        Example:
            Clock                horvath2013  hannum  phenoage
            Organization
            0                       45.2       48.1     42.3
            1                       46.8       49.2     43.1
            2                       44.5       47.9     41.8
            Global (Weighted)       45.5       48.4     42.4
    """
    info("Creating organization means table")
    
    organization_breakdown = central_results.get("organization_breakdown", {})
    global_aggregates = central_results.get("global_aggregates", {})
    
    if not organization_breakdown:
        warn("No organization breakdown data available")
        return pd.DataFrame()
    
    # Build table data: rows = organizations, columns = clocks
    table_data = {}
    all_clocks = set()
    
    # Collect all clock names from global aggregates
    all_clocks.update(global_aggregates.keys())
    
    # Extract means for each organization
    for org_id, org_stats in organization_breakdown.items():
        table_data[f"Organization {org_id}"] = {}
        
        for clk in all_clocks:
            if clk in org_stats:
                mean_val = org_stats[clk].get("mean", np.nan)
                table_data[f"Organization {org_id}"][clk] = round(mean_val, 2) if not np.isnan(mean_val) else np.nan
            else:
                table_data[f"Organization {org_id}"][clk] = np.nan
    
    # Add global aggregates row
    table_data["Global (Weighted)"] = {}
    for clk in all_clocks:
        if clk in global_aggregates:
            weighted_mean = global_aggregates[clk].get("weighted_mean", np.nan)
            table_data["Global (Weighted)"][clk] = round(weighted_mean, 2) if not np.isnan(weighted_mean) else np.nan
        else:
            table_data["Global (Weighted)"][clk] = np.nan
    
    # Convert to DataFrame
    df_table = pd.DataFrame(table_data).T
    df_table.index.name = "Organization"
    
    info("Organization means table created successfully")
    return df_table


def format_organization_summary(central_results: dict) -> str:
    """
    Create a formatted string summary table showing organization means and statistics.
    
    Parameters:
    -----------
    central_results : dict
        Results dictionary from central_function
    
    Returns:
    --------
    str : Formatted table as a string (suitable for logging/display)
    """
    df_table = create_organization_means_table(central_results)
    
    if df_table.empty:
        return "No data available to create summary table"
    
    # Convert to string with nice formatting
    table_str = "\n" + "=" * 80 + "\n"
    table_str += "EPIGENETIC AGE SUMMARY - ORGANIZATION MEANS\n"
    table_str += "=" * 80 + "\n"
    table_str += df_table.to_string()
    table_str += "\n" + "=" * 80 + "\n"
    
    return table_str

# TODO Feel free to add more central functions here.


def load_test_data(test_data_name: str = "test_data_1") -> pd.DataFrame:
    """
    Load reference test data for algorithm validation and testing.
    
    Parameters:
    -----------
    test_data_name : str
        Name of the test data file ('test_data_1', 'test_data_2', 'test_data_3')
    
    Returns:
    --------
    pd.DataFrame : Beta values matrix with CpG IDs as first column
    """
    if test_data_name not in TEST_DATA_FILES:
        error(f"Test data '{test_data_name}' not found. Available: {list(TEST_DATA_FILES.keys())}")
        return pd.DataFrame()
    
    test_file = TEST_DATA_FILES[test_data_name]
    if not test_file.exists():
        error(f"Test data file not found at: {test_file}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(test_file)
        info(f"Loaded {test_data_name}: {df.shape[0]} CpG sites × {df.shape[1]-1} samples")
        return df
    except Exception as e:
        error(f"Failed to load test data: {str(e)}")
        return pd.DataFrame()


def get_test_data_reference_info() -> dict:
    """
    Get information about available test data files.
    
    Returns:
    --------
    dict : Metadata about available test data files
    """
    info_dict = {}
    for name, path in TEST_DATA_FILES.items():
        if path.exists():
            df = pd.read_csv(path)
            info_dict[name] = {
                "path": str(path),
                "cpg_sites": df.shape[0],
                "samples": df.shape[1] - 1,
                "exists": True
            }
        else:
            info_dict[name] = {
                "path": str(path),
                "exists": False
            }
    return info_dict
