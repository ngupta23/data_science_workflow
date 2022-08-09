from typing import Optional
import pandas as pd
from pycaret.time_series import TSForecastingExperiment


def forecast_single_group(
    data: pd.DataFrame,
    fh: int,
    folds: int,
    sp: Optional[int] = None,
    session_id: int = 42,
) -> pd.DataFrame:
    """Forecasts a single group using PyCaret"""

    group = None
    if "group" in data.columns:
        group = data["group"].unique()
        assert len(group) == 1
        group = group[0]
        print(f"Group: {group} | History Available: {len(data)} periods")
    else:
        print("'group' column not found in data frame.")

    data["Period"] = pd.PeriodIndex(data["Period"], freq="M")
    data = data.sort_values("Period")
    data.set_index("Period", inplace=True)

    # Overrides for groups with minimal data
    if len(data) <= 5 * fh:
        dates = data.index.sort_values()
        future_index = [dates[-1] + i for i in range(1, fh + 1)]
        preds = pd.DataFrame(index=future_index)
        preds["y_pred"] = data["num_passengers"].mean()
    else:
        exp = TSForecastingExperiment()
        exp.setup(
            data=data["num_passengers"],
            seasonal_period=sp,
            fh=fh,
            fold=folds,
            n_jobs=1,
            session_id=session_id,
        )
        include = ["arima", "ets", "exp_smooth", "croston", "lr_cds_dt"]
        best = exp.compare_models(include=include)
        metrics = exp.pull()
        print(exp.check_stats(test="summary"))
        print(
            f"\nMaterial: {group}"
            f"\nBest: {best}"
            f"\nModel Params: {best.get_params()}"
            f"\nCV Metrics\n{metrics}"
        )
        final = exp.finalize_model(best)
        preds = exp.predict_model(final)

    preds["y_pred"] = preds["y_pred"].clip(lower=0).round()

    # When using with ray/spark, material will not be in index since we are not
    # using pandas groupby. Hence, adding explicitly here.
    preds["group"] = group

    # Also reset index so that account period is available in column itself.
    preds.reset_index(inplace=True)
    preds.rename(columns={"index": "Period"}, inplace=True)
    preds["Period"] = preds["Period"].dt.strftime("%Y%m")

    # TODO: Ideally, we would like to return metrics from this function as well.
    # However, Spark can only return 1 value. But Ray can return multiple values.
    # return preds, metrics
    return preds
