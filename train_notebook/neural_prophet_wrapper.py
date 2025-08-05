import mlflow.pyfunc

class NeuralProphetWrapper(mlflow.pyfunc.PythonModel):
    def __init__(self, model):
        self.model = model

    def predict(self, context, model_input):
        # model_input debe tener al menos la columna 'ds' (y las regresoras si aplican)
        return self.model.predict(model_input)
