import subprocess
import os

# List of your environment names
environments = [
    "emb-diachronic",
    "gnn-cen",
    "gnn-cenet",
    "gnn-evokg",
    "gnn-hismatch",
    "gnn-retemp",
    "rnn-renet"
]

# Optional: Create export directory
export_dir = "conda_env_exports"
os.makedirs(export_dir, exist_ok=True)

for env_name in environments:
    temp_file = os.path.join(export_dir, f"{env_name}_temp.yaml")
    final_file = os.path.join(export_dir, f"{env_name}_environment.yaml")

    print(f"Exporting {env_name} to {final_file}...")

    # Step 1: Export to a temporary file first
    with open(temp_file, "w", encoding="utf-8") as f:
        subprocess.run([
            "conda", "env", "export",
            "-n", env_name,
            "--no-builds"
        ], stdout=f)

    # Step 2: Clean the 'prefix:' line
    with open(temp_file, "r", encoding="utf-8") as f_in, open(final_file, "w", encoding="utf-8") as f_out:
        for line in f_in:
            if not line.startswith("prefix:"):
                f_out.write(line)

    # Step 3: Remove temp file
    os.remove(temp_file)

print("\n✅ All environments exported without prefix. Files are inside:", export_dir)
