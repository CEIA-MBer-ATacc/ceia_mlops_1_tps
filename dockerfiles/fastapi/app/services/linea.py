from datetime import date
import json
import subprocess

import pandas as pd
import mlflow

from app.schemas.linea import FechaValor, LineaInput, LineaOutput
from app.config import settings, s3


class Linea:

    _MODEL = settings.MODEL
    _DIAS_PREDECIDOS = settings.DIAS_PREDECIDOS

    @staticmethod
    async def predecir_por_linea(linea: LineaInput) -> LineaOutput:
        try:
            # Validación de líneas
            Linea._check_valid_line(linea.linea)
            mlflow.set_tracking_uri('http://mlflow:5000')
            # client_mlflow = mlflow.MlflowClient()
            model_name = Linea._MODEL.format(linea=linea.linea)
            # model_data_mlflow = client_mlflow.get_model_version_by_alias(model_name, "champion")
            # pip_path = mlflow.pyfunc.get_model_dependencies(model_name, format='pip')
            # subprocess.check_call(["pip", "install", "-r", pip_path])

            loaded_model = mlflow.pyfunc.load_model(model_name)#model_data_mlflow.source)

            future_dates = Linea._prepare_input_data(
                date_init=linea.date_init,
                dias_pred=Linea._DIAS_PREDECIDOS
            )
            future_dates["y"] = 0.0   # dummy value
            future_dates["km"] = 0.0  # o el valor que corresponda si es regresor futuro conocido

            pred = loaded_model.predict(future_dates)

            return Linea._prediccion_linea_output(pred)

        except Exception as e:
            raise RuntimeError(f"Error processing request: {str(e)}")

    @staticmethod
    def _prediccion_linea_output(df_pred) -> LineaOutput:
        resultados = [
            FechaValor(fecha=row["ds"].date(), valor=int(row["yhat1"]))
            for _, row in df_pred.iterrows()
        ]
        return LineaOutput(resultados=resultados)

    @staticmethod
    def _prepare_input_data(date_init: date, dias_pred: int) -> pd.DataFrame:
        # Crear DataFrame de fechas futuras
        return pd.DataFrame({
            "ds": pd.date_range(start=date_init, periods=dias_pred, freq="D")
        })

    @staticmethod
    def _get_lines() -> list:
        obj = s3.get_object(Bucket=settings.AWS_BUCKET_NAME, Key=settings.AWS_OBJECT_KEY)
        return json.loads(obj["Body"].read())

    @staticmethod
    def _check_valid_line(line: str) -> None:
        if line not in Linea._get_lines():
            raise Exception(f"La línea {line} no es válida.")
