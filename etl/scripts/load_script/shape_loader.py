import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

BATCH_SIZE = 5000


def load_shapes_for_dataset(hook, staging_dataset_dir: Path, dataset_id) -> int:
    """
    Lit shapes.txt et trips.txt depuis le dossier GTFS brut d'un dataset.
    Charge dim_shape, dim_shape_point et met à jour dim_trip.shape_sk.
    Retourne le nombre de points chargés.
    """
    shapes_file = staging_dataset_dir / "shapes.txt"
    trips_file  = staging_dataset_dir / "trips.txt"

    if not shapes_file.exists():
        logger.info(f"[shapes] Pas de shapes.txt pour dataset {dataset_id} — ignoré")
        return 0

    if not trips_file.exists():
        logger.warning(f"[shapes] shapes.txt présent mais pas trips.txt pour dataset {dataset_id}")
        return 0

    # -- Lecture shapes.txt --------------------------------------------------
    try:
        shapes_df = pd.read_csv(shapes_file, dtype=str, low_memory=False)
        shapes_df.columns = shapes_df.columns.str.strip()
    except Exception as e:
        logger.error(f"[shapes] Erreur lecture shapes.txt: {e}")
        return 0

    required = {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"}
    missing = required - set(shapes_df.columns)
    if missing:
        logger.warning(f"[shapes] Colonnes manquantes dans shapes.txt: {missing}")
        return 0

    # -- Nettoyage -----------------------------------------------------------
    shapes_df = shapes_df.dropna(subset=list(required))
    shapes_df["shape_pt_lat"]      = pd.to_numeric(shapes_df["shape_pt_lat"],      errors="coerce")
    shapes_df["shape_pt_lon"]      = pd.to_numeric(shapes_df["shape_pt_lon"],      errors="coerce")
    shapes_df["shape_pt_sequence"] = pd.to_numeric(shapes_df["shape_pt_sequence"], errors="coerce")
    shapes_df = shapes_df.dropna(subset=["shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"])

    if shapes_df.empty:
        logger.info(f"[shapes] Aucun point valide pour dataset {dataset_id}")
        return 0

    # -- Lecture trips.txt ---------------------------------------------------
    try:
        trips_df = pd.read_csv(trips_file, dtype=str, low_memory=False)
        trips_df.columns = trips_df.columns.str.strip()
    except Exception as e:
        logger.error(f"[shapes] Erreur lecture trips.txt: {e}")
        return 0

    if "shape_id" not in trips_df.columns or "trip_id" not in trips_df.columns:
        logger.warning("[shapes] trips.txt sans colonnes shape_id/trip_id")
        return 0

    # -- Chargement dim_shape ------------------------------------------------
    unique_shape_ids = shapes_df["shape_id"].unique().tolist()
    logger.info(f"[shapes] {len(unique_shape_ids)} shapes uniques pour dataset {dataset_id}")

    shape_id_to_sk: dict = {}

    for shape_id in unique_shape_ids:
        row = hook.get_first(
            "SELECT shape_sk FROM dim_shape WHERE shape_id = %s",
            parameters=(shape_id,)
        )
        if row:
            shape_id_to_sk[shape_id] = row[0]
            continue

        hook.run(
            "INSERT INTO dim_shape (shape_id) VALUES (%s) "
            "ON DUPLICATE KEY UPDATE shape_id=VALUES(shape_id)",
            parameters=(shape_id,)
        )
        row = hook.get_first(
            "SELECT shape_sk FROM dim_shape WHERE shape_id = %s",
            parameters=(shape_id,)
        )
        if row:
            shape_id_to_sk[shape_id] = row[0]

    # -- Chargement dim_shape_point en batches --------------------------------
    shapes_df["shape_sk"] = shapes_df["shape_id"].map(shape_id_to_sk)
    shapes_df = shapes_df.dropna(subset=["shape_sk"])

    rows = list(zip(
        shapes_df["shape_sk"].astype(int),
        shapes_df["shape_pt_sequence"].astype(int),
        shapes_df["shape_pt_lat"].round(6),
        shapes_df["shape_pt_lon"].round(6),
    ))

    total_points = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        placeholders = ", ".join(["(%s, %s, %s, %s)"] * len(batch))
        flat_params = [v for row in batch for v in row]
        hook.run(
            f"INSERT IGNORE INTO dim_shape_point (shape_sk, pt_sequence, lat, lon) "
            f"VALUES {placeholders}",
            parameters=flat_params
        )
        total_points += len(batch)

    logger.info(f"[shapes] {total_points} points chargés pour dataset {dataset_id}")

    # -- Mise à jour dim_trip.shape_sk ----------------------------------------
    trips_with_shapes = trips_df[["trip_id", "shape_id"]].dropna()
    updated = 0

    for _, row in trips_with_shapes.iterrows():
        shape_sk = shape_id_to_sk.get(row["shape_id"])
        if shape_sk:
            hook.run(
                "UPDATE dim_trip SET shape_sk = %s WHERE trip_id = %s AND shape_sk IS NULL",
                parameters=(shape_sk, row["trip_id"])
            )
            updated += 1

    logger.info(f"[shapes] {updated} trips mis à jour avec shape_sk pour dataset {dataset_id}")
    return total_points
