import os

import polars as pl
import scipy.stats as st
from dotenv import load_dotenv

load_dotenv(override=True)
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_DATABASE = os.environ["POSTGRES_DATABASE"]

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

query = """
SELECT
    *
FROM
    simplified_matches
"""
conn_uri = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
matches = pl.read_database_uri(query=query, uri=conn_uri)
matches = (
    matches.filter(pl.col("position") != "").filter(pl.col("region") == "KR")
)
median_time = (
    matches.get_column("ended_at").median()
)
train_matches = (
    matches.filter(pl.col("ended_at") <= median_time)
)
test_matches = (
    matches.filter(pl.col("ended_at") > median_time)
)

champion_stats = (
    train_matches
    .group_by("position", "champion")
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
    .sort("position", "champion")
)

mean_win_rate = (
    train_matches
    .get_column("win")
    .cast(pl.Float64)
    .mean()
)
print(
    train_matches
    .join(champion_stats, on=["position", "champion"])
    .with_columns(
        (pl.col("win").cast(pl.Float64) - mean_win_rate).alias("baseline_error"),
        (pl.col("win").cast(pl.Float64) - pl.col("win_rate")).alias("frequentist_error"),
        (pl.col("win").cast(pl.Float64) - pl.col("bayesian_win_rate")).alias("bayesian_error")
    )
    .unpivot(
        on=["baseline_error", "frequentist_error", "bayesian_error"],
        variable_name="method",
        value_name="error",
    )
    .group_by(
        "method"
    )
    .agg(
        pl.col("error").pow(2).mean().sqrt().alias("rmse")
    )
)
with pl.Config(tbl_cols=-1):
    print(
        test_matches
        .join(champion_stats, on=["position", "champion"])
    )
print(
    test_matches
    .join(champion_stats, on=["position", "champion"])
    .with_columns(
        (pl.col("win").cast(pl.Float64) - mean_win_rate).alias("baseline_error"),
        (pl.col("win").cast(pl.Float64) - pl.col("win_rate")).alias("frequentist_error"),
        (pl.col("win").cast(pl.Float64) - pl.col("bayesian_win_rate")).alias("bayesian_error")
    )
    .unpivot(
        on=["baseline_error", "frequentist_error", "bayesian_error"],
        variable_name="method",
        value_name="error",
    )
    .group_by(
        "method"
    )
    .agg(
        pl.col("error").pow(2).mean().sqrt().alias("rmse")
    )
)
print(
    test_matches
    .join(champion_stats, on=["position", "champion"])
    .filter(pl.col("total_count") >= 1000)
    .with_columns(
        (pl.col("win").cast(pl.Float64) - mean_win_rate).alias("baseline_error"),
        (pl.col("win").cast(pl.Float64) - pl.col("win_rate")).alias("frequentist_error"),
        (pl.col("win").cast(pl.Float64) - pl.col("bayesian_win_rate")).alias("bayesian_error")
    )
    .unpivot(
        on=["baseline_error", "frequentist_error", "bayesian_error"],
        variable_name="method",
        value_name="error",
    )
    .group_by(
        "method"
    )
    .agg(
        pl.col("error").pow(2).mean().sqrt().alias("rmse")
    )
)