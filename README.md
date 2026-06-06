# Vessel Collision Detection

Detects vessel collisions in Danish AIS data (December 2021) using Apache PySpark.

## Requirements

- Docker and Docker Compose
- Docker Desktop configured with **at least 8 GB of RAM** (Settings → Resources → Memory)
- `aisdk-2021-12.zip` placed in the project root (next to `docker-compose.yml`)

## How to run with Docker

```bash
# Clone the repository
git clone https://github.com/tareilyte11/BDA_exam.git
cd BDA_exam

# Pull the pre-built image from Docker Hub
docker pull zivile11/vessel-collision-image:latest

# Run the pipeline
docker compose up
```

The pipeline prints the collision result to the console. Output files are saved to `./output/` on the host:

| File | Description |
|------|-------------|
| `output/collision_result.txt` | Vessel names, MMSI, timestamp, coordinates, distance |
| `output/trajectory.png` | Trajectory plot for both vessels around the collision |

> **Note:** Each run cleans up temp files and re-extracts the data from scratch to avoid stale state from previous runs.

## Re-running

Simply run again — the pipeline cleans up its own temp files and parquet on startup:

```bash
docker compose up
```

To manually clear leftover files before running (e.g. after a crashed run):

```bash
rm -rf spark-tmp/ output/filtered_data.parquet
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

