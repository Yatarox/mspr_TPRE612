import pytest
from airflow.providers.mysql.hooks.mysql import MySqlHook
from load_script.staging import load_staging_table
from load_script.fact_loader import load_fact_table

@pytest.fixture
def mysql_hook():
    # Configure un hook vers ta base de test
    return MySqlHook(mysql_conn_id="mysql_test")

def test_load_staging_table_integration(mysql_hook, tmp_path):
    # Prépare un CSV de test
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("trip_id,agency_name,route_name,origin_stop_name,destination_stop_name,distance_km\n"
                        "T1,SNCF,R1,Paris,Lyon,400\n")
    # Vide la table cible
    mysql_hook.run("TRUNCATE TABLE stg_trips_summary")
    # Appelle la fonction ETL
    count = load_staging_table(mysql_hook, csv_path, load_id=1, dataset_id=1, origin_max_len=10, dest_max_len=10)
    assert count == 1
    # Vérifie le contenu de la table
    rows = mysql_hook.get_records("SELECT * FROM stg_trips_summary WHERE trip_id='T1'")
    assert len(rows) == 1

def test_load_staging_table_multiple_and_invalid(mysql_hook, tmp_path):
    csv_path = tmp_path / "test_multi.csv"
    csv_path.write_text(
        "trip_id,agency_name,route_name,origin_stop_name,destination_stop_name,distance_km\n"
        "T1,SNCF,R1,Paris,Lyon,400\n"
        ",SNCF,R2,Paris,Marseille,800\n"  # trip_id manquant (doit être ignoré)
        "T2,SNCF,R3,Paris,Nice,notanumber\n"  # distance_km invalide (doit être ignoré)
    )
    mysql_hook.run("TRUNCATE TABLE stg_trips_summary")
    count = load_staging_table(mysql_hook, csv_path, load_id=2, dataset_id=2, origin_max_len=10, dest_max_len=10)
    assert count == 1  # Seule la première ligne est valide
    rows = mysql_hook.get_records("SELECT * FROM stg_trips_summary")
    assert len(rows) == 1
    assert rows[0][0] == "T1"  # trip_id de la ligne valide



def test_staging_to_fact(mysql_hook, tmp_path):
    csv_path = tmp_path / "test_fact.csv"
    csv_path.write_text(
        "trip_id,agency_name,route_name,origin_stop_name,destination_stop_name,distance_km\n"
        "T3,SNCF,R4,Paris,Bordeaux,500\n"
    )
    mysql_hook.run("TRUNCATE TABLE stg_trips_summary")
    mysql_hook.run("TRUNCATE TABLE fact_trips")  # ou le nom de ta table de faits
    count = load_staging_table(mysql_hook, csv_path, load_id=3, dataset_id=3, origin_max_len=10, dest_max_len=10)
    assert count == 1
    loaded = load_fact_table(mysql_hook, load_id=3)
    assert loaded == 1
    rows = mysql_hook.get_records("SELECT * FROM fact_trips WHERE trip_id='T3'")
    assert len(rows) == 1

def test_load_staging_table_empty_file(mysql_hook, tmp_path):
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("trip_id,agency_name,route_name,origin_stop_name,destination_stop_name,distance_km\n")
    mysql_hook.run("TRUNCATE TABLE stg_trips_summary")
    count = load_staging_table(mysql_hook, csv_path, load_id=4, dataset_id=4, origin_max_len=10, dest_max_len=10)
    assert count == 0
    rows = mysql_hook.get_records("SELECT * FROM stg_trips_summary")
    assert len(rows) == 0