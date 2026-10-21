# Debugging Guide for Databricks in VS Code

## Quick Start - Set a Breakpoint Now!

1. Open any `.py` file in VS Code
2. Click left of line number to set a red breakpoint dot
3. Press `F5` → Select "Python: Current File"
4. Code pauses at breakpoint - inspect variables in left panel!

---

## Debugging Methods

### Method 1: Local Debugging (Fastest)
**When to use:** Testing logic before deploying to Databricks

```python
# test_databricks_connection.py
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="dev")  # ← Set breakpoint here (F9)
user = w.current_user.me()          # ← Step over (F10) to debug
print(user.user_name)
```

**Shortcuts:**
- `F9` - Toggle breakpoint
- `F5` - Start debugging
- `F10` - Step over (next line)
- `F11` - Step into (enter function)
- `Shift+F11` - Step out
- `F5` - Continue

### Method 2: Remote Debugging on Cluster
**When to use:** Debugging PySpark code that must run on cluster

**Setup (one-time):**
```bash
# Install on cluster - already installed locally
pip install debugpy
```

**In your Databricks notebook/script:**
```python
# Add at the top of your code
import debugpy
debugpy.listen(("0.0.0.0", 5678))
print("⏸️  Waiting for VS Code debugger...")
debugpy.wait_for_client()
print("✅ Debugger attached!")

# Your code here - breakpoints will work!
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.table("my_table")  # ← Set breakpoint in VS Code
df.show()
```

**In VS Code:**
1. Set breakpoints in your local file
2. Press `F5` → Select **"Databricks: Debug Notebook"**
3. Run the notebook on Databricks
4. VS Code connects automatically!

### Method 3: Print/Logging (Most Reliable)
**When to use:** Always! Works everywhere.

```python
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def my_function(data):
    print(f"DEBUG: Input data type: {type(data)}")
    print(f"DEBUG: Data shape: {data.shape if hasattr(data, 'shape') else len(data)}")

    logger.debug(f"Processing {len(data)} records")

    # Your logic here
    result = process(data)

    print(f"DEBUG: Result: {result}")
    return result
```

**Pro tips:**
- Use `display(df)` in Databricks notebooks to see DataFrames
- Add `print(df.explain())` to see Spark execution plan
- Use `df.printSchema()` to debug schema issues

### Method 4: Interactive Cell Debugging
**When to use:** Exploring data and testing queries

Use `# COMMAND ----------` to split code into cells:

```python
# COMMAND ----------
# Cell 1: Setup
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Cell 2: Load data - run this cell independently
df = spark.read.table("my_table")
display(df.head(10))  # See results immediately

# COMMAND ----------
# Cell 3: Debug transformation
filtered_df = df.filter(df.age > 25)
print(f"Rows before: {df.count()}")
print(f"Rows after: {filtered_df.count()}")
display(filtered_df)
```

---

## Your Current Debug Configurations

Run these from the Debug panel (Ctrl+Shift+D):

1. **Python: Current File** - Debug any Python file locally
2. **Debug Tests** - Debug pytest files
3. **Test Databricks Connection** - Debug connection script
4. **Databricks: Debug Notebook** - Remote debugging on cluster
5. **Run on Databricks Cluster** - Execute on cluster with env vars

---

## Common Debugging Scenarios

### Scenario 1: "Why is my DataFrame empty?"
```python
df = spark.read.table("my_table")

# Debug steps:
print(f"DEBUG: Row count: {df.count()}")  # Is data there?
print(f"DEBUG: Schema: {df.printSchema()}")  # Correct columns?
df.show(5, truncate=False)  # See first 5 rows
print(f"DEBUG: Explain plan: {df.explain()}")  # How is Spark reading it?
```

### Scenario 2: "Why did my job fail?"
```python
try:
    result = my_complex_operation(data)
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()  # Full stack trace
    
    # Add more context
    print(f"DEBUG: Data state: {data}")
    print(f"DEBUG: Variables: {locals()}")
    raise
```

### Scenario 3: "Is my cluster configuration correct?"
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="dev")
cluster = w.clusters.get("1013-103948-caejxbqa")

print(f"DEBUG: Cluster state: {cluster.state.value}")
print(f"DEBUG: Spark version: {cluster.spark_version}")
print(f"DEBUG: Node type: {cluster.node_type_id}")
print(f"DEBUG: Workers: {cluster.num_workers}")
```

---

## Debugging Checklist

Before asking "why isn't this working?":
- [ ] Set breakpoints and step through code
- [ ] Print variable types and values
- [ ] Check DataFrame counts and schemas
- [ ] Verify cluster is running (`test_databricks_connection.py`)
- [ ] Check environment variables (`echo $DATABRICKS_CONFIG_PROFILE`)
- [ ] Look at full error stack trace
- [ ] Test with smaller sample data first

---

## Resources

- VS Code Python Debugging: https://code.visualstudio.com/docs/python/debugging
- Databricks Debugging: https://docs.databricks.com/dev-tools/debugging.html
- PySpark Debugging: https://spark.apache.org/docs/latest/api/python/user_guide/debugging.html
