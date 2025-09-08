from __future__ import annotations

import os
import json
import pendulum
import requests
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

RAW_DIR = "/app/raw_files"
CLEAN_DIR = "/app/clean_data"


def fetch_openweather_data(**_):
    """Tâche (1): récupère météo pour les villes de la Variable Airflow `cities` et écrit un JSON horodaté."""
    # la meilleure pratique serait l'utilisation d'un coffre fort de clé mais hors sujet donc faisons simple
	# variable airflow "owm_api_key" (pas de fourniture de ma clé en dur via le dag de l'exam)
    api_key = Variable.get("owm_api_key")  
    if not api_key:
        raise RuntimeError("OWM_API_KEY manquant (env)")

	# définition de "cities" avec ["perpignan","lyon","marseille"] dans airflow mais on définit un fallback (différent pour verif)
    cities = Variable.get("cities", default_var='["paris","london","washington"]')
    cities = json.loads(cities)

    os.makedirs(RAW_DIR, exist_ok=True)
    now_str = pendulum.now("UTC").format("YYYY-MM-DD HH:mm")
    out_path = os.path.join(RAW_DIR, f"{now_str}.json")

    payloads = []
    for city in cities:
        url = "https://api.openweathermap.org/data/2.5/weather"
        r = requests.get(url, params={"q": city, "appid": api_key})
        r.raise_for_status()
        payloads.append(r.json())

    with open(out_path, "w") as f:
        json.dump(payloads, f)

    print(f"Enregistrement de la requête dans {out_path}")


def transform_data_into_csv(n_files: int | None = None, filename: str = "data.csv"):
    """Tâches (2) & (3): lit /app/raw_files et écrit un CSV agrégé dans /app/clean_data."""
    parent_folder = RAW_DIR
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []
    for f in files:
        with open(os.path.join(parent_folder, f), "r") as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    "temperature": data_city["main"]["temp"],
                    "city": data_city["name"],
                    "pression": data_city["main"]["pressure"],
                    "date": f.split(".")[0],
                }
            )

    df = pd.DataFrame(dfs)
    os.makedirs(CLEAN_DIR, exist_ok=True)
    out_path = os.path.join(CLEAN_DIR, filename)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(df)} rows.")

def prepare_data(path_to_data: str = os.path.join(CLEAN_DIR, "fulldata.csv")):
    """Préparation dataset pour (4) & (5)."""
    df = pd.read_csv(path_to_data)
    df = df.sort_values(["city", "date"], ascending=True)

    dfs = []
    for c in df["city"].unique():
        df_temp = df[df["city"] == c].copy()
        df_temp.loc[:, "target"] = df_temp["temperature"].shift(1)
        for i in range(1, 10):
            df_temp.loc[:, f"temp_m-{i}"] = df_temp["temperature"].shift(-i)
        df_temp = df_temp.dropna()
        dfs.append(df_temp)

    df_final = pd.concat(dfs, axis=0, ignore_index=False)
    df_final = df_final.drop(["date"], axis=1)
    df_final = pd.get_dummies(df_final)

    X = df_final.drop(["target"], axis=1)
    y = df_final["target"]
    return X, y

def compute_model_score(model_cls_name: str, **_):
    """(4') (4'') (4''') : entraîne en CV et retourne le score (XCom)."""
    X, y = prepare_data(os.path.join(CLEAN_DIR, "fulldata.csv"))
    model_map = {
        "lr": LinearRegression,
        "dt": DecisionTreeRegressor,
        "rf": RandomForestRegressor,
    }
    model = model_map[model_cls_name]()
    cv = cross_val_score(model, X, y, cv=3, scoring="neg_mean_squared_error")
    score = cv.mean()
    print(f"{model} CV neg-MSE = {score}")
    return float(score)

def select_and_save_best(ti, **_):
    """(5) : sélectionne le meilleur score, réentraine sur tout X,y et sauvegarde le modèle."""
    scores = {
        "lr": ti.xcom_pull(task_ids="training.score_linear"),
        "dt": ti.xcom_pull(task_ids="training.score_tree"),
        "rf": ti.xcom_pull(task_ids="training.score_forest"),
    }
    best_key = max(scores, key=scores.get)  # neg-MSE: plus grand = meilleur
    print(f"Scores: {scores} → best = {best_key}")

    X, y = prepare_data(os.path.join(CLEAN_DIR, "fulldata.csv"))
    model_map = {
        "lr": LinearRegression,
        "dt": DecisionTreeRegressor,
        "rf": RandomForestRegressor,
    }
    best_model = model_map[best_key]().fit(X, y)
    out_model = os.path.join(CLEAN_DIR, "best_model.pickle")
    dump(best_model, out_model)
    print(f"Saved best model ({best_key}) at {out_model}")

with DAG(
    dag_id="my_weather_dag",
    description="Ingestion OpenWeatherMap → transforms → train → select → save",
    start_date=pendulum.datetime(2025, 9, 8, tz="UTC"),  # date statique préférentiellement
    schedule_interval="* * * * *",  # lancé chaque minute
    catchup=False,  # pas de rattrapage (éviter de bloquer openweather en fllodant tout le rattrapage...)
    default_args={
        # NB : le user "remy" a été créé sur mon serveur airflow avec le profil Op 
		# (seul profil en dehors d'Admin à pouvoir créer les variables)
        "owner": "remy",
    },
    doc_md="""
# Réflexion sur l'architecture générale du DAG pour l'examen

Un FileSensor pour vérifier que le fichier full est bien present avant le groupe de tâches [4]
serait une bonne pratique (mais pas demandé dans l'examen donc pas mis en place ici):

- t_fetch (tache 1)
- t_recent (taches 2)
- t_full (taches 3)
- tg_training (groupe avec tâches 4/4'/4'' en parallèle):
  - t_score_linear (4)
  - t_score_tree (4')
  - t_score_forest (4'')
- t_select_save (tache 5)

	""",
    # max_active_runs=1,
    tags=["examen"],  # pour filtrage facile dans les dag
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_openweather",
        python_callable=fetch_openweather_data,
    )

    t_recent = PythonOperator(
        task_id="to_csv_20_last",
        python_callable=transform_data_into_csv,
        op_kwargs={"n_files": 20, "filename": "data.csv"},
    )

    t_full = PythonOperator(
        task_id="to_csv_full",
        python_callable=transform_data_into_csv,
        op_kwargs={"n_files": None, "filename": "fulldata.csv"},
    )

    with TaskGroup(group_id="training") as tg_training:
        t_score_lr = PythonOperator(
            task_id="score_linear",
            python_callable=compute_model_score,
            op_kwargs={"model_cls_name": "lr"},
        )
        t_score_dt = PythonOperator(
            task_id="score_tree",
            python_callable=compute_model_score,
            op_kwargs={"model_cls_name": "dt"},
        )
        t_score_rf = PythonOperator(
            task_id="score_forest",
            python_callable=compute_model_score,
            op_kwargs={"model_cls_name": "rf"},
        )
        # en parallèle

    t_select_save = PythonOperator(
        task_id="select_and_save_best",
        python_callable=select_and_save_best,
        provide_context=True,
    )

    # Orchestration selon l'architecture demandée
    t_fetch >> t_recent
    t_fetch >> t_full >> tg_training >> t_select_save