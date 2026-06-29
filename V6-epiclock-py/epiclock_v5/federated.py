"""
This file contains all federated algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the federated task
or directly to the user (if they requested federated results).
"""
import pandas as pd
from typing import Any, List
import numpy as np
import pyaging as pya
from pathlib import Path
import os

from vantage6.algorithm.tools.util import info, warn, error
from vantage6.algorithm.decorator.action import federated
from vantage6.algorithm.decorator.data import dataframe

# Reference test data paths
TEST_DATA_DIR = Path(__file__).parent.parent / "test"
TEST_DATA_FILES = {
    "test_data_1": TEST_DATA_DIR / "test_data_1.csv",
    "test_data_2": TEST_DATA_DIR / "test_data_2.csv",
    "test_data_3": TEST_DATA_DIR / "test_data_3.csv",
}


@federated
@dataframe(1)
def federated_function(
    df1: pd.DataFrame, lista_relojes: List[str],
    # INDIVIDUAL_RESULTS: Add optional parameter to return individual sample ages
    return_individual_results: bool = False
) -> dict:
    """
    Calculate epigenetic age using multiple clocks on beta values matrix.
    
    PRIVACY: Returns only aggregated statistics (mean, std, min, max, N) per clock.
    Individual patient ages are NEVER returned by default.
    
    Parameters:
    -----------
    df1 : pd.DataFrame
        Beta values matrix with CpG IDs as index and samples as columns
    lista_relojes : List[str]
        List of clock names to calculate (e.g., ['horvath2013', 'hannum', 'phenoage'])
    # INDIVIDUAL_RESULTS: Document the new parameter for individual results
    return_individual_results : bool, optional
        If True, also returns individual sample ages (default: False for privacy)
    
    Returns:
    --------
    dict : Organization-level aggregated statistics per clock and optionally individual results
        {
            "aggregated": {
                "horvath2013": {"mean": 45.2, "std": 8.3, "min": 32.1, "max": 58.9, "N": 5},
            },
            # INDIVIDUAL_RESULTS: Show structure when individual results are included
            "individual": {
                "horvath2013": {"Sample_1": 45.2, "Sample_2": 46.1, ...}
            }  # Only included if return_individual_results=True
        }
    """
    # Validate inputs
    if df1.empty:
        error("Input dataframe is empty")
        return {}
    
    if not lista_relojes:
        warn("No clocks specified")
        return {}
    
    n_samples = df1.shape[1]
    info(f"Calculating clocks: {lista_relojes} for {n_samples} samples")
    
    try:
        # Set CpG IDs as index (first column contains CpG IDs)
        df_indexed = df1.set_index(df1.columns[0])
        
        # Transpose: pyaging expects samples as rows, CpGs as columns
        df_transposed = df_indexed.T
        
        # Convert to AnnData format
        adata = pya.preprocess.df_to_adata(df_transposed)
        
        # Calculate ages for each clock
        for reloj in lista_relojes:
            try:
                pya.pred.predict_age(adata, clock_names=[reloj])
                info(f"Successfully calculated {reloj}")
            except Exception as e:
                warn(f"Failed to calculate {reloj}: {str(e)}")
                adata.obs[reloj] = np.nan
        
        # Aggregate at organization level (privacy preservation)
        aggregated_stats = {}
        # INDIVIDUAL_RESULTS: Initialize dictionary to store individual ages if requested
        individual_ages = {}
        
        for clk in lista_relojes:
            ages = adata.obs[clk].values
            # Filter out NaN values
            valid_ages = ages[~np.isnan(ages)]
            
            if len(valid_ages) > 0:
                aggregated_stats[clk] = {
                    "mean": float(np.mean(valid_ages)),
                    "std": float(np.std(valid_ages)),
                    "min": float(np.min(valid_ages)),
                    "max": float(np.max(valid_ages)),
                    "N": len(valid_ages)
                }
                info(f"{clk}: mean={aggregated_stats[clk]['mean']:.2f}, N={aggregated_stats[clk]['N']}")
                
                # INDIVIDUAL_RESULTS: Store individual ages if requested
                if return_individual_results:
                    # INDIVIDUAL_RESULTS: Create dictionary with sample names as keys
                    individual_ages[clk] = {
                        # INDIVIDUAL_RESULTS: Map each sample to its calculated age
                        sample: float(age) for sample, age in 
                        zip(adata.obs_names, adata.obs[clk].values)
                    }
            else:
                warn(f"{clk}: No valid ages calculated")
                aggregated_stats[clk] = {
                    "mean": np.nan,
                    "std": np.nan,
                    "min": np.nan,
                    "max": np.nan,
                    "N": 0
                }
        
        info("Clock calculation and aggregation complete")
        
        # INDIVIDUAL_RESULTS: Build result dictionary with conditional individual results
        result = {"aggregated": aggregated_stats}
        # INDIVIDUAL_RESULTS: Only add individual results if explicitly requested
        if return_individual_results:
            # INDIVIDUAL_RESULTS: Include individual sample ages in the response
            result["individual"] = individual_ages
            info("Individual results included in response")
        
        return result
        
    except Exception as e:
        error(f"Error in partial calculation: {str(e)}")
        return {}


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


# TODO Feel free to add more partial functions here.

