import mlflow 

from neuralprophet import NeuralProphet
from sklearn.metrics import mean_absolute_error


def champion_callback(study, frozen_trial):
    """
    Logging callback that will report when a new trial iteration improves upon existing
    best trial values.
    """

    winner = study.user_attrs.get("winner", None)

    if study.best_value and winner != study.best_value:
        study.set_user_attr("winner", study.best_value)
        if winner:
            improvement_percent = (abs(winner - study.best_value) / study.best_value) * 100
            print(
                f"Trial {frozen_trial.number} achieved value: {frozen_trial.value} with "
                f"{improvement_percent: .4f}% improvement"
            )
        else:
            print(f"Initial trial {frozen_trial.number} achieved value: {frozen_trial.value}")

def objective(trial, df_train, df_test, regresores):
    model = NeuralProphet(
        n_changepoints=trial.suggest_int("n_changepoints", 10, 50),
        trend_reg=trial.suggest_float("trend_reg", 0.1, 10.0),
        learning_rate=trial.suggest_float("learning_rate", 0.001, 0.01, log=True),
        seasonality_mode=trial.suggest_categorical
        ("seasonality_mode", ["additive", "multiplicative"]),
        epochs=100,
        batch_size=128,
    )

    # Agregar regresores externos
    for reg in regresores:
        model.add_future_regressor(reg)

    # Entrenar modelo
    model.fit(df_train, freq='D', metrics=True, early_stopping=True)

    # Predecir sobre validación
    forecast = model.predict(df_test)

    # Calcular MAE
    mae = mean_absolute_error(df_test['y'].values, forecast['yhat1'].values)
    return mae
