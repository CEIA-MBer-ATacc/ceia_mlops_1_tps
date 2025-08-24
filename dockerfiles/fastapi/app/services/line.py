from datetime import date
import json

import pandas as pd
import mlflow

from app.config import settings, s3
from app.schemas.line import DateValue, LineInput, LineOutput


class Line:

    _MODEL = settings.MODEL
    _PREDICTED_DAYS = settings.PREDICTED_DAYS

    @staticmethod
    async def predict_by_line(line: LineInput) -> LineOutput:
        try:
            mlflow.set_tracking_uri('http://mlflow:5000')
            model_name = Line._MODEL.format(line=line.line)

            loaded_model = mlflow.pyfunc.load_model(model_name)

            future_dates = Line._prepare_input_data(
                date_init=line.date_init,
                pred_days=Line._PREDICTED_DAYS
            )
            future_dates["y"] = 0.0   # dummy value
            future_dates["km"] = 0.0  # or the corresponding value if it's a known future regressor

            pred = loaded_model.predict(future_dates)

            return Line._prediction_line_output(pred)

        except Exception as e:
            raise RuntimeError(f"Error processing request: {str(e)}")

    @staticmethod
    def _prediction_line_output(df_pred) -> LineOutput:
        results = [
            DateValue(date=row["ds"].date(), value=int(row["yhat1"]))
            for _, row in df_pred.iterrows()
        ]
        return LineOutput(results=results)

    @staticmethod
    def _prepare_input_data(date_init: date, pred_days: int) -> pd.DataFrame:
        # Create DataFrame with future dates
        return pd.DataFrame({
            "ds": pd.date_range(start=date_init, periods=pred_days, freq="D")
        })

    @staticmethod
    def _get_lines() -> list:
        obj = s3.get_object(Bucket=settings.AWS_BUCKET_NAME, Key=settings.AWS_OBJECT_KEY)
        return json.loads(obj["Body"].read())

    @staticmethod
    def _check_valid_line(line: str) -> None:
        if line not in Line._get_lines():
            raise Exception(f"The line {line} is not valid.")
