import os
import re
from pathlib import Path
import pandas as pd

RAW_DIR = Path("RAW")
OUTPUT_DIR = Path("MERGED")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

all_sessions = []

def parse_folder_name(folder_name: str):
    """
    Expected style:
    best_jumping_01_20260306_101500
    tracy_walking_03_timestamp
    
    We only strictly need:
    member_activity_trial
    """
    parts = folder_name.split("_")
    if len(parts) < 3:
        raise ValueError(f"Folder name not in expected format: {folder_name}")
    
    member = parts[0]
    activity = parts[1]
    trial = parts[2]
    return member, activity, trial

for session_folder in RAW_DIR.iterdir():
    if not session_folder.is_dir():
        continue

    acc_file = session_folder / "Accelerometer.csv"
    gyro_file = session_folder / "Gyroscope.csv"

    if not acc_file.exists() or not gyro_file.exists():
        print(f"Skipping {session_folder.name}: missing CSV")
        continue

    try:
        member, activity, trial = parse_folder_name(session_folder.name)

        # Load files
        acc = pd.read_csv(acc_file)
        gyro = pd.read_csv(gyro_file)

        # Clean column names
        acc.columns = [c.strip().lower() for c in acc.columns]
        gyro.columns = [c.strip().lower() for c in gyro.columns]

        # Rename sensor axes
        acc = acc.rename(columns={
            "x": "acc_x",
            "y": "acc_y",
            "z": "acc_z"
        })

        gyro = gyro.rename(columns={
            "x": "gyro_x",
            "y": "gyro_y",
            "z": "gyro_z"
        })

        # Keep only needed columns
        acc_needed = ["time", "seconds_elapsed", "acc_x", "acc_y", "acc_z"]
        gyro_needed = ["time", "seconds_elapsed", "gyro_x", "gyro_y", "gyro_z"]

        missing_acc = [c for c in acc_needed if c not in acc.columns]
        missing_gyro = [c for c in gyro_needed if c not in gyro.columns]

        if missing_acc:
            raise ValueError(f"{session_folder.name} missing acc columns: {missing_acc}")
        if missing_gyro:
            raise ValueError(f"{session_folder.name} missing gyro columns: {missing_gyro}")

        acc = acc[acc_needed].sort_values("time").reset_index(drop=True)
        gyro = gyro[gyro_needed].sort_values("time").reset_index(drop=True)

        # Merge on nearest timestamp
        merged = pd.merge_asof(
            acc,
            gyro,
            on="time",
            direction="nearest"
        )

        # Add metadata
        merged["member"] = member
        merged["activity"] = activity
        merged["trial"] = trial
        merged["session_id"] = session_folder.name

        # Save session-level merged file
        merged.to_csv(OUTPUT_DIR / f"{session_folder.name}_merged.csv", index=False)

        all_sessions.append(merged)
        print(f"Merged: {session_folder.name}")

    except Exception as e:
        print(f"Error in {session_folder.name}: {e}")

# Combine all sessions into one dataset
if all_sessions:
    final_df = pd.concat(all_sessions, ignore_index=True)
    final_df.to_csv(OUTPUT_DIR / "all_merged_sessions.csv", index=False)
    print("\nSaved combined dataset to MERGED/all_merged_sessions.csv")
    print(final_df.head())
else:
    print("No sessions were merged.")