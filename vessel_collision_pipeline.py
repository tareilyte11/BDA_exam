import os
import shutil
import zipfile
from datetime import datetime, timezone
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit,
    radians, sin, cos, asin, sqrt,
    floor, when, lag, row_number,
    to_timestamp, unix_timestamp,
    broadcast, countDistinct,
    max as spark_max, min as spark_min,
)
from pyspark.sql.window import Window

#Variables:

ZIP_FILE = os.environ.get("ZIP_FILE", "aisdk-2021-12.zip")
TEMP_DIR = "temp_extract"
PARQUET_OUT = "output/filtered_data.parquet"
OUTPUT_DIR = "output"
SPARK_TMP_DIR = os.path.join(os.getcwd(), "spark-tmp")

CENTRAL_LAT = 55.225000
CENTRAL_LON = 14.245000
RADIUS_FROM_CENTER = 50.0
EARTH_RADIUS_NM = 3440.065   # nautical miles
EARTH_RADIUS_M = 6371000  # metres
INVALID_MMSIS = ["000000000", "111111111", "123456789"]
ALLOWED_NAV = ["Under way using engine"]
ALLOWED_MOBILE = ["Class A", "Class B"]
EXCLUDED_SHIP_TYPES = ["Tug", "Dredging", "Law enforcement", "SAR", "Pilot"]
MAX_SPEED_KNOTS = 60.0
MIN_MOVEMENT_DEG = 0.005
COLLISION_THRESHOLD_M =100.0 #in meters
#Grid cell size 
COORDINATE_GRID_DEG = 0.05
TIME_GRID_SEC = 60
# Trajectory window around the collision event
IMAGE_WINDOW  = 600 #in seconds

# Distance Calculations:
def haversine(lat1, lon1, lat2, lon2, radius):
    dphi    = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = (sin(dphi / 2) ** 2
         + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlambda / 2) ** 2)
    return 2 * lit(radius) * asin(sqrt(a))

def haversine_nm(lat1, lon1, lat2, lon2):
    return haversine(lat1, lon1, lat2, lon2, EARTH_RADIUS_NM)

def haversine_m(lat1, lon1, lat2, lon2):
    return haversine(lat1, lon1, lat2, lon2, EARTH_RADIUS_M)

############################################################

def create_spark_session():
    return (SparkSession.builder
            .appName("VesselCollisionDetection")
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.driver.memoryOverhead", "1g")
            .config("spark.driver.maxResultSize", "0")
            .config("spark.driver.extraJavaOptions",
                    "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
            .config("spark.local.dir", SPARK_TMP_DIR)
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())

def read_and_filter(spark, zip_path, parquet_out):

    print(f"Reading of a ZIP started...")
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as z:
        csv_files = sorted(f for f in z.namelist())
        print(f"Found {len(csv_files)} files.")

        for file_name in csv_files:
            print(f"Reading {file_name}")
            z.extract(file_name, TEMP_DIR)
            csv_path = os.path.join(TEMP_DIR, file_name)

            raw = spark.read.csv(csv_path, header=True, inferSchema=False)

            df = raw.select(
                to_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss").alias("Timestamp"),
                col("MMSI"),
                col("Latitude").cast("double"),
                col("Longitude").cast("double"),
                col("Navigational status").alias("nav_status"),
                col("Type of mobile").alias("mobile_type"),
                col("SOG").cast("double"),
                col("Name"),
                col("Ship type").alias("ship_type"),
            )

            df = df.filter(col("Timestamp").isNotNull())
            df = df.withColumn("unix_ts", unix_timestamp("Timestamp"))

            #Checks if coordinates are valid
            df = (df
                  .filter(col("Latitude").isNotNull())
                  .filter(col("Longitude").isNotNull())
                  .filter(col("Latitude").between(-90.0, 90.0))
                  .filter(col("Longitude").between(-180.0, 180.0)))

            #Checks if distance is not more than 50 nm
            df = df.filter(
                haversine_nm(
                    col("Latitude"), col("Longitude"),
                    lit(CENTRAL_LAT), lit(CENTRAL_LON),
                ) <= RADIUS_FROM_CENTER
            )

            #Checks if MMSI is valid
            df = df.filter(~col("MMSI").isin(INVALID_MMSIS))

            #Keeps only vessels under way using engine
            df = df.filter(col("nav_status").isin(ALLOWED_NAV))

            #Keeps only tracked vessel classes
            df = df.filter(col("mobile_type").isin(ALLOWED_MOBILE))

            #Excludes ship types whose type can suflerate that they are working in the same place (dredging) or work alongside others (emergencies, tugs etc. )
            df = df.filter(~col("ship_type").isin(EXCLUDED_SHIP_TYPES))

            #Excludes vessels that are not moving (SOG = 0 or unknown)
            df = df.filter(col("SOG").isNotNull() & (col("SOG") > 0))

            df.write.mode("append").parquet(parquet_out)
            os.remove(csv_path)

    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    print("Reading completed")

def remove_gps_anomalies(df):

    w = Window.partitionBy("MMSI").orderBy("unix_ts")

    df = (df
          .withColumn("prev_lat", lag("Latitude").over(w))
          .withColumn("prev_lon", lag("Longitude").over(w))
          .withColumn("prev_ts",  lag("unix_ts").over(w))
          .withColumn("gap_sec",  col("unix_ts") - col("prev_ts")))

    df = (df
          .withColumn("seg_dist_nm",
                      haversine_nm(col("prev_lat"), col("prev_lon"),
                                   col("Latitude"), col("Longitude")))
          .withColumn("implied_sog",
                      when(col("prev_lat").isNotNull() & (col("gap_sec") > 0),
                           col("seg_dist_nm") / (col("gap_sec") / 3600.0))
                      .otherwise(0.0)))
    
#check if there are no GPS "jumps" that can be treated as annomalies
    df_clean = (df
                .filter(~(col("prev_lat").isNotNull()
                          & (col("implied_sog") > MAX_SPEED_KNOTS)))
                .drop("prev_lat", "prev_lon", "prev_ts", "gap_sec",
                      "seg_dist_nm", "implied_sog"))
    
    return df_clean

def filter_stationary_vessels(df_clean):

    vessel_spread = df_clean.groupBy("MMSI").agg(
        (spark_max("Latitude")  - spark_min("Latitude") ).alias("lat_spread"),
        (spark_max("Longitude") - spark_min("Longitude")).alias("lon_spread"),
    )
#check if all vessels are truly moving in latitude and longitude, remove if movements are not significant
    moving = vessel_spread.filter(
        (col("lat_spread") >= MIN_MOVEMENT_DEG)
        | (col("lon_spread") >= MIN_MOVEMENT_DEG)
    ).select("MMSI")

    return df_clean.join(broadcast(moving), on="MMSI", how="inner")

def detect_collision(spark, df_clean):

    # Round lat/lon to the nearest grid cell and timestamp to the nearest minute
    df_bucketed = (df_clean
                   .withColumn("grid_x",
                                floor(col("Latitude") / COORDINATE_GRID_DEG).cast("int"))
                   .withColumn("grid_y",
                                floor(col("Longitude") / COORDINATE_GRID_DEG).cast("int"))
                   .withColumn("time_bucket",
                                floor(col("unix_ts") / TIME_GRID_SEC).cast("long")))

    w_dedup = Window.partitionBy("MMSI", "time_bucket").orderBy("unix_ts")
    df_dedup = (df_bucketed
                .withColumn("rn", row_number().over(w_dedup))
                .filter(col("rn") == 1)
                .drop("rn"))

    df_dedup.cache()

    neighbor_offsets = [
        (x, y, t)
        for x in (-1, 0, 1)
        for y in (-1, 0, 1)
        for t in (0, 1)
    ]
    neighbor_offsets_df = spark.createDataFrame(neighbor_offsets, ["x", "y", "t"])

    df_right = (df_dedup
                .crossJoin(broadcast(neighbor_offsets_df))
                .withColumn("adj_x", col("grid_x") + col("x"))
                .withColumn("adj_y", col("grid_y") + col("y"))
                .withColumn("adj_t", col("time_bucket") + col("t"))
                .drop("x", "y", "t"))
    
    L = df_dedup.alias("L")
    R = df_right.alias("R")

    candidates = (L.join(R,
        (col("L.grid_x")      == col("R.adj_x"))
        & (col("L.grid_y")    == col("R.adj_y"))
        & (col("L.time_bucket") == col("R.adj_t"))
        & (col("L.MMSI")      <  col("R.MMSI")),
        how="inner",
    ).select(
        col("L.MMSI").alias("mmsi_a"),
        col("L.Name").alias("name_a"),
        col("L.Latitude").alias("lat_a"),
        col("L.Longitude").alias("lon_a"),
        col("L.unix_ts").alias("ts_a"),
        col("R.MMSI").alias("mmsi_b"),
        col("R.Name").alias("name_b"),
        col("R.Latitude").alias("lat_b"),
        col("R.Longitude").alias("lon_b"),
        col("R.unix_ts").alias("ts_b"),
    ))

    candidates = (candidates
                  .withColumn("dist_m",
                               haversine_m(col("lat_a"), col("lon_a"),
                                           col("lat_b"), col("lon_b")))
                  .filter(col("dist_m") <= COLLISION_THRESHOLD_M)
                  .dropDuplicates(["mmsi_a", "mmsi_b", "ts_a", "ts_b"]))

    # Pick the minimum-distance encounter per vessel pair
    w_pair = Window.partitionBy("mmsi_a", "mmsi_b").orderBy("dist_m")
    collision_rows = (candidates
                      .withColumn("pair_rn", row_number().over(w_pair))
                      .filter(col("pair_rn") == 1)
                      .drop("pair_rn")
                      .orderBy("dist_m")
                      .collect())

    df_dedup.unpersist()
    return collision_rows

def verify_collisions(spark, collision_rows, df_moving):

    if not collision_rows:
        return collision_rows

    vc_data = []
    for i, row in enumerate(collision_rows):
        collision_ts = max(int(row["ts_a"]), int(row["ts_b"]))
        vc_data.append((i, int(row["mmsi_a"]), collision_ts))
        vc_data.append((i, int(row["mmsi_b"]), collision_ts))

    vc_df = spark.createDataFrame(vc_data, ["collision_idx", "MMSI", "collision_ts"])

    # Per collision, count how many of the two vessels have at least one post-collision ping with SOG > 0
    # Pairs where both vessels confirmed moving is removed
    both_moving = (df_moving
                   .join(broadcast(vc_df), on="MMSI", how="inner")
                   .filter(col("unix_ts") > col("collision_ts"))
                   .filter(col("SOG") > 0)
                   .groupBy("collision_idx")
                   .agg(countDistinct("MMSI").alias("moving_count"))
                   .filter(col("moving_count") == 2)
                   .select("collision_idx"))

    survived_set = {r["collision_idx"] for r in both_moving.collect()}

    verified = [row for i, row in enumerate(collision_rows) if i not in survived_set]
    removed  = len(collision_rows) - len(verified)

    return verified

########Trajectories and image creation#########
def extract_trajectories(df_clean, mmsi_a, mmsi_b, collision_ts):

    t_start = collision_ts - IMAGE_WINDOW
    t_end   = collision_ts + IMAGE_WINDOW

    def get_traj(mmsi):
        return (df_clean
                .filter(
                    (col("MMSI") == mmsi)
                    & col("unix_ts").between(t_start, t_end)
                )
                .orderBy("unix_ts")
                .select("unix_ts", "Latitude", "Longitude", "Timestamp")
                .toPandas())

    return get_traj(mmsi_a), get_traj(mmsi_b)


def save_results(collision_rows, output_dir):
    
#Print and save all collision candidates to a text file, ordered by distance

    all_lines = [
        "=" * 62,
        f"  VESSEL COLLISION DETECTION — {len(collision_rows)} CANDIDATE(S)",
        "=" * 62,
    ]

    for i, row in enumerate(collision_rows, start=1):
        ts_a   = datetime.fromtimestamp(int(row["ts_a"]), tz=timezone.utc)
        ts_b   = datetime.fromtimestamp(int(row["ts_b"]), tz=timezone.utc)
        dist_m = row["dist_m"]
        name_a = row["name_a"] or str(row["mmsi_a"])
        name_b = row["name_b"] or str(row["mmsi_b"])

        all_lines += [
            "",
            f"  [{i}] Distance : {dist_m:.1f} m  ({dist_m / 1852:.4f} nm)",
            f"  Vessel A  MMSI : {row['mmsi_a']}",
            f"  Vessel A  Name : {name_a}",
            f"  Vessel A  Time : {ts_a.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"  Vessel A  Pos  : {row['lat_a']:.6f}°N  {row['lon_a']:.6f}°E",
            f"  Vessel B  MMSI : {row['mmsi_b']}",
            f"  Vessel B  Name : {name_b}",
            f"  Vessel B  Time : {ts_b.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"  Vessel B  Pos  : {row['lat_b']:.6f}°N  {row['lon_b']:.6f}°E",
        ]

    all_lines.append("=" * 62)
    text = "\n".join(all_lines)
    print(text)

    path = os.path.join(output_dir, "collision_result.txt")
    with open(path, "w") as f:
        f.write(text + "\n")
    print(f"Results saved in {path}")


def visualise(traj_a, traj_b, collision_row, name_a, name_b, output_dir):
    dist_m = collision_row["dist_m"]
    lat_c  = (collision_row["lat_a"] + collision_row["lat_b"]) / 2
    lon_c  = (collision_row["lon_a"] + collision_row["lon_b"]) / 2
    ts_str = datetime.fromtimestamp(
        int(collision_row["ts_a"]), tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M UTC")


    fig, ax = plt.subplots(figsize=(10, 8))

    if not traj_a.empty:
        ax.plot(traj_a["Longitude"], traj_a["Latitude"],
                color="steelblue", marker="o", linestyle="-",
                markersize=4, linewidth=1.5, label=f"{name_a} (A)")

    if not traj_b.empty:
        ax.plot(traj_b["Longitude"], traj_b["Latitude"],
                color="hotpink", marker="o", linestyle="-",
                markersize=4, linewidth=1.5, label=f"{name_b} (B)")

    ax.plot(lon_c, lat_c, marker="o", color="black", markersize=12,
            label=f"Closest approach ({dist_m:.0f} m)", zorder=5)

    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")
    ax.set_title(
        f"Vessel paths before and after the event"
    )
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.5)

    png_path = os.path.join(output_dir, "trajectory.png")
    plt.tight_layout()
    plt.savefig(png_path, dpi=150)
    plt.close()
    print(f"PNG saved in {png_path}")


############ Main pipeline ####################
def main():
    shutil.rmtree(SPARK_TMP_DIR, ignore_errors=True)
    os.makedirs(SPARK_TMP_DIR, exist_ok=True)
    shutil.rmtree(PARQUET_OUT, ignore_errors=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = create_spark_session()

    read_and_filter(spark, ZIP_FILE, PARQUET_OUT)
    df = spark.read.parquet(PARQUET_OUT)

    df_clean = remove_gps_anomalies(df)

    df_moving = filter_stationary_vessels(df_clean)

    collision_rows = detect_collision(spark, df_moving)
    df_moving.cache()

    #remove pairs where both vessels had SOG > 0 after the event (meaning they both were moving forward)
    collision_rows = verify_collisions(spark, collision_rows, df_moving)

    if not collision_rows:
        print(f"No collisions found.")
        df_moving.unpersist()
        spark.stop()
        return

    save_results(collision_rows, OUTPUT_DIR)

    best = collision_rows[0]
    name_a = best["name_a"] or str(best["mmsi_a"])
    name_b = best["name_b"] or str(best["mmsi_b"])

    traj_a, traj_b = extract_trajectories(
        df_moving,
        best["mmsi_a"],
        best["mmsi_b"],
        int(best["ts_a"]),
    )

    df_moving.unpersist()

    visualise(traj_a, traj_b, best, name_a, name_b, OUTPUT_DIR)

    spark.stop()
    print(f"Pipeline complete.")


if __name__ == "__main__":
    main()
