# Running Code on Databricks from VS Code

This guide explains how to run your PySpark code on Databricks from VS Code.

## Setup Complete ✓

Your VS Code is now configured to connect to:
- **Workspace**: https://adb-955988584387839.19.azuredatabricks.net
- **User**: mvangapalli@nationsbenefits.com
- **Profile**: dev

The `dev` entry in `databricks.yml` is the bundle target, while `dev` in
`~/.databrickscfg` is the authentication profile. The Databricks extension
needs the bundle project and target selected before you configure its
authentication profile.

## Prerequisites

1. **Install Databricks Extension**
   - Open VS Code Extensions (Ctrl+Shift+X)
   - Search for "Databricks"
   - Install the official Databricks extension

2. **Open and select the bundle project**
   - Open the repository root folder (the folder containing `databricks.yml`).
   - Open the Databricks sidebar and select the `data-platform` bundle project.
   - In the extension's Configuration view, select the `dev` target.

3. **Configure authentication**
   - In the Configuration view, click **Auth Type**, then the sign-in gear.
   - Select the `dev` authentication profile from the Command Palette.
   - If `dev` is not listed, configure it in `~/.databrickscfg` first.

4. **Verify Connection**
   - Run `test_databricks_connection.py` (Debug → Test Databricks Connection)
   - This confirms your SDK connection is working

## Methods to Run Code on Databricks

### Method 1: Using Databricks Extension (Recommended)

The Databricks extension syncs your local files to the workspace and runs them on clusters.

**Steps:**
1. Open the Databricks sidebar and select the `data-platform` bundle project.
2. Select the `dev` target and confirm the workspace host.
3. In the Configuration view, use **Auth Type** and the sign-in gear to select the `dev` profile.
4. Open a Python file (e.g., `src/ingestion/sql_server_reader.py`).
5. Right-click → "Databricks: Upload and Run File".
6. Select a cluster from the dropdown.

**File Sync:**
- Files are synced to: `/Users/mvangapalli@nationsbenefits.com/data-flex-platform`
- Auto-sync on save (configure in settings)

### Method 2: Using Databricks Notebooks

**Create/Edit Notebooks:**
1. Open Command Palette
2. "Databricks: Open Notebook from Workspace"
3. Browse and select existing notebook OR
4. "Databricks: Create New Notebook"

**Run Notebooks:**
- Click "Run All" or run cells individually
- Notebooks execute on the attached cluster
- Results show inline in VS Code

### Method 3: Using Databricks CLI & Bundles

**Deploy and Run:**
```bash
# Validate bundle
databricks bundle validate -t dev

# Deploy to workspace
databricks bundle deploy -t dev

# Run a job
databricks bundle run -t dev member_load
```

### Method 4: Direct SDK Usage

For administrative tasks or testing:

```python
from databricks.sdk import WorkspaceClient

# Connect using profile
w = WorkspaceClient(profile="dev")

# List clusters
clusters = list(w.clusters.list())

# Execute commands
w.workspace.upload(...)
```

## Running PySpark Code

### Option A: Upload and Execute on Cluster

1. **For Python Files:**
   ```bash
   # Upload file to workspace
   databricks workspace import src/ingestion/sql_server_reader.py \
     /Users/mvangapalli@nationsbenefits.com/code/sql_server_reader.py \
     --profile dev

   # Run using job or notebook
   ```

2. **Using VS Code Extension:**
   - Right-click file → "Upload to Databricks"
   - Create a notebook that imports your module
   - Run the notebook on a cluster

### Option B: Convert to Notebook

Create `test_sql_reader.py` as a notebook:

```python
# Databricks notebook source

from ingestion.sql_server_reader import SQLServerReader
from pyspark.sql import SparkSession

# Get Spark session (available in Databricks)
spark = SparkSession.builder.getOrCreate()

# Initialize reader
reader = SQLServerReader(
    spark=spark,
    jdbc_url="jdbc:sqlserver://your-server:1433;database=YourDB",
    username="your_username",
    password="your_password"
)

# Read data
df = reader.read_table("YourTable", schema="dbo")
df.show()
```

### Option C: Interactive Development

1. **Open Databricks Web UI**:
   - https://adb-955988584387839.19.azuredatabricks.net
   
2. **Create Interactive Notebook**:
   - Import your modules
   - Test queries interactively
   - Attach to running cluster

3. **Sync with VS Code**:
   - Use Databricks extension to edit notebooks locally
   - Changes sync back to workspace

## Available Clusters

Currently running clusters:
- Chilukuri Reddy's Personal Compute (Spark 18.0.x)
- Podduturi Sai Sharath's Personal Compute (Spark 19.x)

View all clusters:
```bash
databricks clusters list --profile dev
```

## Debug Configuration

VS Code launch configurations available (F5):
1. **Test Databricks Connection** - Verify SDK connection
2. **Run on Databricks Cluster** - Run current file with Databricks env vars
3. **Databricks: Debug Notebook** - Attach debugger to running notebook

## Environment Variables

Your `.env` file contains:
- `DATABRICKS_HOST` - Workspace URL
- `DATABRICKS_TOKEN` - Authentication token
- Other service credentials

Load in Python:
```python
from dotenv import load_dotenv
load_dotenv()
```

## Best Practices

1. **Local Development:**
   - Write code in `src/`
   - Test with unit tests (`pytest`)
   - Use mock Spark for unit tests

2. **Integration Testing:**
   - Upload to Databricks workspace
   - Run on dev cluster
   - Use small sample datasets

3. **Production Deployment:**
   - Use Databricks Asset Bundles
   - Deploy via `databricks bundle deploy -t prod`
   - Run scheduled jobs

## Troubleshooting

**Connection Issues:**
```bash
# Test CLI connection
databricks workspace list / --profile dev

# Check current user
databricks current-user me --profile dev
```

If the extension reports `Can't set configuration 'authProfile' without selecting a target`,
open the repository root as a Databricks bundle project and select the `dev`
target in the extension's Configuration view before selecting the `dev`
authentication profile. These are separate selections: the target is defined
in `databricks.yml`, while the profile is defined in `~/.databrickscfg`.

**Import Errors:**
- Ensure `PYTHONPATH` includes `src/`
- Upload all required modules to workspace
- Use absolute imports from package root

**Cluster Access:**
- Verify cluster is running
- Check cluster permissions
- Ensure cluster has required libraries

## Next Steps

1. ✓ Connection configured
2. Install Databricks VS Code extension
3. Upload your code to workspace
4. Create test notebooks
5. Run on dev cluster
6. Set up CI/CD with bundles

## Resources

- [Databricks VS Code Extension Docs](https://docs.databricks.com/dev-tools/vscode-ext.html)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)
- [Asset Bundles Guide](https://docs.databricks.com/dev-tools/bundles/)
