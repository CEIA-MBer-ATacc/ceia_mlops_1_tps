# Operaciones de Aprendizaje de Máquina I (CEIA-MLOps1)

[![Python](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-yes-blue.svg)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/airflow-3.0.2-orange.svg)](https://airflow.apache.org/)
[![MLflow](https://img.shields.io/badge/mlflow-3.3.1-lightgrey.svg)](https://mlflow.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-%3E=0.116-green.svg)](https://fastapi.tiangolo.com/)
[![NeuralProphet](https://img.shields.io/badge/NeuralProphet-0.8.0-purple.svg)](https://facebook.github.io/prophet/)

---

## Profesores
**Facundo Adrián Lucianna** – facundolucianna@gmail.com

---

## Integrantes
- **Alejandro Nicolás Tacchella** (a2023) – alejandronicolastacchella@gmail.com  
- **Maximiliano José Bernardo** (a2002) – maxibernardo@gmail.com

---

## Prerrequisitos
- Docker  
- Docker Compose  
- Python 3.12

---

## Tecnologías Utilizadas
- **Airflow 3.0.2:** Orquestación de pipelines ETL y programación de tareas periódicas.  
- **MLflow 3.3.1:** Registro y seguimiento de experimentos, hiperparámetros, métricas y modelos entrenados.  
- **MinIO:** Almacenamiento tipo S3 para datasets y artefactos de modelos.  
- **FastAPI >= 0.116:** API REST para exponer endpoints de predicciones de forma rápida.  
- **Python 3.12:** Lenguaje principal para pipelines, modelos y API.  
- **NeuralProphet 0.8.0:** Modelado de series temporales para predicción de transacciones y kilómetros por línea.

---

## Servicios y Puertos

| Servicio      | URL                                            | Descripción                                    | Credenciales          |
| ------------- | ---------------------------------------------- | ---------------------------------------------- | --------------------- |
| **FastAPI**   | [http://localhost:8800](http://localhost:8800) | API REST y Swagger UI                           | -                     |
| **Airflow**   | [http://localhost:8080](http://localhost:8080) | Orquestador de pipelines de datos              | airflow/airflow           |
| **MLflow**    | [http://localhost:5001](http://localhost:5001) | Tracking de experimentos y métricas           | -                     |
| **MinIO**     | [http://localhost:9001](http://localhost:9001) | Consola de almacenamiento tipo S3             | minio/minio123 |

---

## Ejecución del Proyecto

### 1. Levantar todos los servicios
```bash
docker compose up --profile all -d
```

## Ejecutar el DAG de Airflow

- DAG: process_etl_actrans_data

- Función: ETL de datos de ACTrans, limpieza y carga a MinIO.

## Entrenamiento de modelos

- Notebook: train_notebook/train.ipynb

- Función: Entrenamiento de modelos de series temporales por línea usando NeuralProphet y registro en MLflow.

## Realizar predicciones vía API

Ejemplo para la línea 441:

```
curl -X 'POST' \
  'http://localhost:8800/line/predict' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "line": "441",
  "date_init": "2025-01-01"
}'
```

## Notas

- La API devuelve predicciones de transacciones por línea a partir de la fecha indicada.

- Los experimentos y modelos quedan registrados en MLflow para seguimiento y comparación de métricas.

- Airflow se encarga de orquestar la ingestión, limpieza y preparación de datos automáticamente.

- MinIO se utiliza como almacenamiento tipo S3 para datasets y artefactos de modelos.

## Flujo General del Proyecto

- Airflow extrae y limpia los datos desde los archivos ACTrans.

- Los datos limpios se almacenan en MinIO.

- NeuralProphet entrena modelos por línea y los registra en MLflow.

- FastAPI expone endpoints para predicciones sobre los datos procesados.

## Recursos Adicionales

[Documentación FastAPI](https://fastapi.tiangolo.com/)

[Documentación Airflow](https://airflow.apache.org/docs/)

[Documentación MLflow](https://mlflow.org/docs/latest/index.html)

[Documentación NeuralProphet](https://neuralprophet.com/contents.html)

[MinIO Docs](https://docs.min.io/)
