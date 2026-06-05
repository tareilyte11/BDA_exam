# Vessel Collision Detection

Detects vessel collisions in Danish AIS data (December 2021) using Apache PySpark.

## Requirements

- Docker and Docker Compose
- `aisdk-2021-12.zip` placed inside the `Zip_data_ais/` folder in the project root

## How to build and run

```bash
# Clone the repository
git clone <repo-url>
cd BDA_EXAM

# Build the Docker image and run the pipeline
docker compose up --build
```

The pipeline will print the collision result to the console. Output files are saved to `./output/` on the host:

| File | Description |
|------|-------------|
| `output/collision_result.txt` | Vessel names, MMSI, timestamp, coordinates, distance |
| `output/trajectory.png` | Trajectory plot for both vessels around the collision |
| `output/filtered_data.parquet/` | Cached checkpoint — reused on subsequent runs |

## Re-running

On the second run the pipeline skips the data ingest (checkpoint already exists) and goes straight to collision detection — this is significantly faster.

To force a full re-run from scratch:

```bash
rm -rf output/filtered_data.parquet
docker compose up
```

## Running locally (without Docker)

Java 11 or 17 must be installed and available on `PATH`.

```bash
python -m venv myenv
source myenv/bin/activate
pip install -r requirements.txt
python vessel_collision_pipeline.py
```
