# Vessel Collision Detection

This report describes the methodology used to detect a vessel collision event.
The data consisted of a raw zipped dataset (`aisdk-2021-12.zip`) containing 31 CSV files, 
one for each day of December 2021. The search area was limited to a 50 nautical mile radius around the point (55.225°N, 14.245°E).

## Data Cleaning

Data cleaning was performed in two stages.
First, the CSV files were read and filtered to retain only the relevant fields:
timestamp, MMSI, latitude, longitude, speed over ground (SOG), navigational status, 
vessel name, ship type, type of mobile

Before writing the data into Parquet files, several validation checks were applied to remove noisy or invalid records.

### Data Cleaning(Part 1)

The following checks were performed before writing to Parquet files:
1. Invalid timestamps were removed if they could not be parsed or were structurally corrupted.
2. Coordinate ranges were validated:
  2.1. Latitude must be within [-90, 90]
  2.2. Longitude must be within [-180, 180]
  2.3. Neither coordinate may be NULL
3. Only records within 50 nautical miles of the study centre were retained.
4. Invalid MMSI values were removed. Examples such as `000000000`, `111111111`, and `123456789` were excluded.
5. Only records with navigational status `"Under way using engine"` were retained, since the analysis focused on moving vessels.
6. Only `Class A` and `Class B` AIS transponders were retained. Other categories (e.g. base stations and AtoN devices) represent infrastructure rather than vessels.
7. Certain ship types were excluded, including tugs, dredgers, law enforcement vessels, search and rescue (SAR) vessels, pilot vessels
These vessel types routinely operate in close proximity to other ships as part of their normal duties,
so close encounters involving them are not reliable indicators of collisions.
8. A stationarity check was applied. Any vessel reporting zero speed over ground was treated as stationary at the time of the record,
regardless of its navigational status.

### Data Cleaning — Part 2

The second cleaning stage was performed on the Parquet files and required grouping by MMSI, since vessel-level behaviour rather than individual records was analysed.
1.GPS anomaly detection:
  For each MMSI, consecutive records were compared. The implied speed between positions was computed using the Haversine formula.
  If the implied speed exceeded 60 knots, the movement was considered physically implausible. Such records were treated as GPS jumps and removed from further analysis.

2. Stationary vessel filtering
  A vessel may report `"Under way using engine"` while effectively drifting, anchored, or otherwise not moving. 
  Since navigational status is self-reported and not always reliable, an additional movement-based filter was applied.
  The coordinate spread across all records was computed per MMSI.
   A vessel whose spread in both axes was below 0.005° was considered stationary and removed.

## Collision Detection Strategy
The pipeline uses a spatial-temporal grid bucketing approach.
1. Each record was assigned to:
  1.1. a spatial grid cell (0.05° × 0.05°)
  1.2. a 1-minute time bucket
Only one record per MMSI per time bucket was retained, using `row_number()` over a window ordered by timestamp.
This reduced the dataset to at most one record per vessel per minute without significantly losing positional information.

2.The right-hand side of the join was expanded using 27 offset tuples representing all combinations of:
x, y ad t that all can get values of -1,0,1. By doing this, each row was shifted into neighbouring spatial and temporal cells. 

3. The original grid records were joined with the shifted grid records by matching spatial and time bucket coordinates.
The condition `L.MMSI < R.MMSI` was added to avoid self-matches and duplicate vessel pairs.
As a result, only vessels that were close in both space and time were compared, which reduced the number of candidate pairs.

4. The Haversine distance (in metres) was computed for each candidate pair.
Pairs farther than 100 m apart were discarded.
For each remaining vessel pair, only the closest encounter was retained using `row_number()` over a window partitioned by vessel pair and ordered by minimum distance.

## Additional Verification of Collision Events
A near-miss between two vessels does not necessarily constitute a collision. Vessels may pass very close to each other and continue sailing normally.
This behaviour was particularly common among pleasure craft and tourist vessels.
To reduce false positives, a candidate pair was removed if both vessels had at least one subsequent AIS record with SOG greater than 0 after the encounter timestamp.
This indicated that both vessels continued moving normally after the event, suggesting a close passage rather than an actual collision.

## Computational Strategy
1. Intermediate and final results were written to Parquet files for efficient storage and retrieval.
2. Spatial and temporal grid bucketing significantly reduced the search space by limiting comparisons to nearby vessels.
3. Unix timestamps were used for efficient temporal processing.
4. `df_moving` was cached to avoid repeated recomputation during later stages of the pipeline.

## Results

The pipeline detected a collision between:

|               | Vessel A              | Vessel B              |
| ------------- | --------------------- | --------------------- |
| **Name**      | KARIN HØEJ            | MV SCOT CARRIER       |
| **MMSI**      | 219021240             | 232018267             |
| **Ship type** | Other                 | Cargo                 |
| **Date/Time** | 2021-12-13 ~01:27 UTC | 2021-12-13 ~01:27 UTC |
| **Position**  | 55.223°N, 14.244°E    | 55.223°N, 14.245°E    |

The two vessels were detected within the 100 m proximity threshold at the same timestamp, which is consistent with a physical collision.
The trajectory plot (`output/trajectory.png`) shows the ±10-minute paths of both vessels converging at the collision point.
