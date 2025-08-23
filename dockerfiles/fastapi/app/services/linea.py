from datetime import date
from app.schemas.linea import FechaValor, LineaInput, LineaOutput
from config import settings
import mlflow.pyfunc
import pandas as pd

class Linea:

    _MODEL = settings.MODEL
    _DIAS_PREDECIDOS = settings.DIAS_PREDECIDOS

    @staticmethod
    async def predecir_por_linea(linea: LineaInput) -> LineaOutput:
        try:

            future_dates = Linea.__prepare_input_data(
                date_init=linea.date_init,
                dias_pred=Linea._DIAS_PREDECIDOS
            )
            
            loaded_model = mlflow.pyfunc.load_model(Linea._MODEL.format(linea=linea.linea))
            pred = loaded_model.predict(future_dates)

            return Linea._prediccion_linea_output(pred)
        
        except Exception as e:
            raise RuntimeError(f"Error processing request: {str(e)}")
        
    @staticmethod
    def _prediccion_linea_output(df_pred) -> LineaOutput:
        resultados = [
            FechaValor(fecha=row["ds"].date(), valor=float(row["yhat1"]))
            for _, row in df_pred.iterrows()
        ]
        return LineaOutput(resultados=resultados)
    
    @staticmethod
    def __prepare_input_data(date_init: date, dias_pred: int) -> pd.DataFrame:
        # Crear DataFrame de fechas futuras
        return  pd.DataFrame({
                    "ds": pd.date_range(start=date_init, periods=dias_pred, freq="D")
                })