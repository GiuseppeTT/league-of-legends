import polars as pl
import scipy.stats as st
from database_handler import create_connection

SIGNIFICANCE = 0.05
ALPHA_PRIOR = 40
BETA_PRIOR = 40


def calculate_mean(success_count, total_count):
    if total_count == 0:
        return None
    mean = success_count / total_count
    return mean


def calculate_lower_bound(success_count, total_count, significance):
    if total_count == 0:
        return None
    if success_count == 0:
        return 0.0
    lower_bound = st.beta.ppf(significance / 2, success_count, total_count - success_count + 1)
    return lower_bound


def calculate_upper_bound(success_count, total_count, significance):
    if total_count == 0:
        return None
    if success_count == total_count:
        return 1.0
    upper_bound = st.beta.ppf(1 - significance / 2, success_count + 1, total_count - success_count)
    return upper_bound


def calculate_bayesian_mean(success_count, total_count, alpha_prior, beta_prior):
    alpha_posterior = alpha_prior + success_count
    beta_posterior = beta_prior + (total_count - success_count)
    if alpha_posterior + beta_posterior == 0:
        return None
    bayesian_mean = alpha_posterior / (alpha_posterior + beta_posterior)
    return bayesian_mean


def calculate_bayesian_credible_lower_bound(
    success_count, total_count, alpha_prior, beta_prior, significance
):
    alpha_posterior = alpha_prior + success_count
    beta_posterior = beta_prior + (total_count - success_count)
    lower_bound = st.beta.ppf(significance / 2, alpha_posterior, beta_posterior)
    return lower_bound


def calculate_bayesian_credible_upper_bound(
    success_count, total_count, alpha_prior, beta_prior, significance
):
    alpha_posterior = alpha_prior + success_count
    beta_posterior = beta_prior + (total_count - success_count)
    upper_bound = st.beta.ppf(1 - significance / 2, alpha_posterior, beta_posterior)
    return upper_bound


database_connection = create_connection("backup_lol.db")
matches = pl.read_database("SELECT * FROM league_unpivoted_matches", database_connection)

(
    matches.remove(pl.col("position") == "")
    .group_by("position", "champion_name")
    .agg(pl.col("win").sum().alias("win_count"), pl.len().alias("total_count"))
    .with_columns(
        # Frequentist
        pl.struct(["win_count", "total_count"])
        .map_elements(
            lambda row: calculate_mean(row["win_count"], row["total_count"]),
            return_dtype=pl.Float64,
        )
        .alias("win_rate"),
        pl.struct(["win_count", "total_count"])
        .map_elements(
            lambda row: calculate_lower_bound(row["win_count"], row["total_count"], SIGNIFICANCE),
            return_dtype=pl.Float64,
        )
        .alias("lower_win_rate"),
        pl.struct(["win_count", "total_count"])
        .map_elements(
            lambda row: calculate_upper_bound(row["win_count"], row["total_count"], SIGNIFICANCE),
            return_dtype=pl.Float64,
        )
        .alias("upper_win_rate"),
        # Bayesian
        pl.struct(["win_count", "total_count"])
        .map_elements(
            lambda row: calculate_bayesian_mean(
                row["win_count"], row["total_count"], ALPHA_PRIOR, BETA_PRIOR
            ),
            return_dtype=pl.Float64,
        )
        .alias("bayesian_win_rate"),
        pl.struct(["win_count", "total_count"])
        .map_elements(
            lambda row: calculate_bayesian_credible_lower_bound(
                row["win_count"], row["total_count"], ALPHA_PRIOR, BETA_PRIOR, SIGNIFICANCE
            ),
            return_dtype=pl.Float64,
        )
        .alias("bayesian_lower_win_rate"),
        pl.struct(["win_count", "total_count"])
        .map_elements(
            lambda row: calculate_bayesian_credible_upper_bound(
                row["win_count"], row["total_count"], ALPHA_PRIOR, BETA_PRIOR, SIGNIFICANCE
            ),
            return_dtype=pl.Float64,
        )
        .alias("bayesian_upper_win_rate"),
    )
    .sort("position", "champion_name")
)
