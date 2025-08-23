from typing import Any

import mlflow.pyfunc
import pandas as pd


class NeuralProphetWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, model):
        self.model = model

    def predict(self, context: Any, model_input: pd.DataFrame) -> pd.DataFrame:
        # model_input debe tener al menos la columna 'ds' (y las regresoras si aplican)
        return self.model.predict(model_input)
