# Databricks notebook source
# MAGIC %md
# MAGIC # QDR Data Quality Validation Pipeline
# MAGIC
# MAGIC This notebook implements a data quality validation framework for **QDR** (Quality Data Repository).
# MAGIC
# MAGIC ## Pipeline Flow:
# MAGIC 1. **Setup & Configuration** - Load libraries, config, and initialize DQX engine
# MAGIC 2. **Load Validation Rules** - Read YAML files containing validation rules and SQL queries
# MAGIC 3. **Execute Validations** - Run SQL queries and apply DQX validation rules
# MAGIC 4. **Process Failures** - Transform quarantined data and write to QDR validation table
# MAGIC
# MAGIC ## Key Features:
# MAGIC * **Template-based SQL** - YAML queries use `{catalog_name}` placeholders
# MAGIC * **Environment-agnostic** - Same YAML works across DEV/UAT/PROD
# MAGIC * **Traceable** - Full query and failed values captured for each validation failure

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import FileChecksStorageConfig
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition
import os, re, sys, json
import logging

# COMMAND ----------

# Setting up logging
logging.basicConfig(
    level=logging.ERROR,           # log only errors (or INFO for more details)
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# COMMAND ----------

# MAGIC %run ./Config_Reader

# COMMAND ----------

config.sections()

# COMMAND ----------

env = os.getenv("env_name")
print(f"Environment: {env}\n")

# Load configuration for the specified environment
if env in config.sections():
    print(f"All items defined under {env}:")
    for key, value in config[env].items():
        globals()[key] = value
        print(f"{key}: {value}")
else:
    print(f"{env} section not found in config")

# COMMAND ----------

# Initialize workspace client and DQX engine
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# COMMAND ----------

def find_repo_root(marker_file="marker.txt"):
    """Find the repository root by looking for a marker file."""
    current_dir = os.getcwd()
    while current_dir != '/':  # stop at root
        if marker_file in os.listdir(current_dir):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    raise FileNotFoundError(f"{marker_file} not found in any parent directories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Validation Rules from YAML Files
# MAGIC
# MAGIC This section loads data quality validation rules from the QDR YAML file:
# MAGIC * **DQX_QDR_Checks.yml** - QDR product validation rules
# MAGIC
# MAGIC The YAML file contains:
# MAGIC * **Validation rules** - SQL expressions to check data quality
# MAGIC * **SQL queries** - Base queries to retrieve data (with `{catalog_name}` placeholders)
# MAGIC * **Metadata** - Table names, unique keys, validation keys
# MAGIC
# MAGIC The queries use template placeholders that are replaced at runtime with environment-specific values from the config.

# COMMAND ----------

# Create the storage config for QDR YAML file
qdr_config = FileChecksStorageConfig(
    os.path.join(find_repo_root(), 'DQX-Quality-Insights', 'rules', 'qdr', 'DQX_QDR_Checks.yml')
)

# COMMAND ----------

# Import the validation checks from the QDR YAML file
try:
    qdr_checks = dq_engine.load_checks(qdr_config)
    print(f"✓ Successfully loaded QDR checks")
    print(f"Number of QDR test cases: {len(qdr_checks)}")
except Exception as e:
    logging.error(f"Error while loading the check files: {e}")
    raise Exception(e)

# COMMAND ----------

# Store checks for processing
checks = qdr_checks
print("QDR Checks loaded:")
for key in checks.keys():
    print(f"  - {key}")

# COMMAND ----------

# MAGIC %run ./dqx_custom_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Validation Column Values
# MAGIC
# MAGIC This function extracts the actual values from validation columns for failed records.
# MAGIC
# MAGIC **Process:**
# MAGIC 1. Executes the base SQL query from YAML to get the source dataset
# MAGIC 2. Joins the quarantined data with the base dataset using the unique key
# MAGIC 3. Extracts the validation column value(s) that caused the failure
# MAGIC 4. Supports multiple validation columns (concatenated with `|` separator)
# MAGIC
# MAGIC **Purpose:** Allows users to see the exact values that failed validation in the output table.

# COMMAND ----------

def get_validation_value(df_flattened, unique_key_column_name, validation_key_column_name):
    """
    Extract validation column values for failed records.
    
    Args:
        df_flattened: Flattened quarantine dataframe
        unique_key_column_name: Name of the unique identifier column
        validation_key_column_name: Name of the validation column(s) - comma-separated if multiple
    
    Returns:
        DataFrame with validation_key_value column added
    """
    base_query = df_flattened.select("sql_query").first()[0]

    # Run the query to get the result df
    base_df = spark.sql(base_query)

    # If multiple columns, split by comma
    validation_columns = [col.strip() for col in validation_key_column_name.split(",")]
    concat_cols_expr = F.concat_ws(
        "|",  # separator between columns
        *[F.col(c).cast("string") for c in validation_columns]
    ).alias("base_df_validation_value")

    # Join with base data to get the target value
    df_with_results = (
        df_flattened
        .join(
            base_df.select(
                F.col(unique_key_column_name).alias("base_df_unique_value"), 
                concat_cols_expr
            ),
            df_flattened["unique_key_value"] == F.col("base_df_unique_value"),
            "left"
        )
        .withColumnRenamed("base_df_validation_value", "validation_key_value")
    )
    return df_with_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Quarantined Data for Output
# MAGIC
# MAGIC This function processes validation failures and prepares them for the DQX_Validation_Details table.
# MAGIC
# MAGIC **Transformation Steps:**
# MAGIC 1. **Explode errors/warnings** - Flattens `_errors` and `_warnings` arrays into individual rows
# MAGIC 2. **Extract metadata** - Pulls validation rule details (name, message, criticality)
# MAGIC 3. **Extract user metadata** - Gets table name, unique key, validation key from YAML
# MAGIC 4. **Capture unique key values** - Extracts the actual unique key value from failed records
# MAGIC 5. **Get validation values** - Retrieves the actual column values that failed validation
# MAGIC 6. **Build validation query** - Creates a query to retrieve the specific failed record
# MAGIC 7. **Format output** - Structures data with standardized column names
# MAGIC
# MAGIC **Output Columns:**
# MAGIC * `product`, `rule_id`, `table_name`, `unique_key_column`, `unique_key_value`
# MAGIC * `validation_rule_name`, `validation_message`, `validation_type`
# MAGIC * `validation_key_column`, `validation_key_value`, `validation_query`
# MAGIC * `run_time`, `_errors`, `_warnings`

# COMMAND ----------

def write_quarantine(df, index, product, full_parameterized_query):
    """
    Transform quarantined dataframe into structured format for DQX_Validation_Details table.
    
    Args:
        df: The quarantined dataframe with validation failures
        index: The test case identifier (e.g., 'qdr_tc_1')
        product: Product type ('QDR')
        full_parameterized_query: The full SQL query with catalog/schema parameterized
    
    Returns:
        Transformed dataframe ready for writing to validation details table
    """

    # Step 1: Explode errors/warnings into individual rows
    new_quarantined_df = df.withColumn(
        "exploded_struct", 
        F.explode_outer(F.coalesce("_errors", "_warnings"))
    )

    # Step 2: Flatten the struct into individual columns
    top_level_cols = ["name", "message", "columns", "filter", "function", "run_time", "user_metadata"]

    for col_name in top_level_cols:
        new_quarantined_df = new_quarantined_df.withColumn(
            col_name, 
            F.col(f"exploded_struct.{col_name}")
        )

    # Step 3: Flatten nested user_metadata
    new_quarantined_df = new_quarantined_df.withColumn(
        "table_name", F.col("exploded_struct.user_metadata.table_name")
    )
    new_quarantined_df = new_quarantined_df.withColumn(
        "unique_key_column", F.col("exploded_struct.user_metadata.unique_key")
    )
    new_quarantined_df = new_quarantined_df.withColumn(
        "validation_key_column", F.col("exploded_struct.user_metadata.validation_key")
    )

    # Capture the unique_key and validation_key column names
    unique_key_column_name = new_quarantined_df.select("unique_key_column").first()[0]
    validation_key_column_name = new_quarantined_df.select("validation_key_column").first()[0]

    # Create unique_key_value column
    new_quarantined_df = new_quarantined_df.withColumn(
        "unique_key_value", 
        F.col(unique_key_column_name)
    )

    # Step 4: Drop the exploded_struct column
    df_flattened = new_quarantined_df.drop("exploded_struct")

    # Create additional metadata columns
    df_flattened = df_flattened.withColumn("rule_id", F.lit(index))
    df_flattened = df_flattened.withColumn("product", F.lit(product))

    # Rename columns to meaningful names
    df_flattened = df_flattened.withColumnRenamed("name", "validation_rule_name")
    df_flattened = df_flattened.withColumnRenamed("function", "validation_type")
    df_flattened = df_flattened.withColumnRenamed("message", "validation_message")

    # Get validation column values
    if df_flattened.select("validation_type").first()[0] == 'sql_expression':
        # For sql_expression type, use the full parameterized query
        df_flattened = df_flattened.withColumn("sql_query", F.lit(full_parameterized_query))
        df_flattened = get_validation_value(df_flattened, unique_key_column_name, validation_key_column_name)
    else:
        # For other validation types, extract values directly
        validation_columns = [col.strip() for col in validation_key_column_name.split(",")]
        concat_cols_expr = F.concat_ws(
            "|",  # separator between columns
            *[F.col(c).cast("string") for c in validation_columns]
        ).alias("validation_key_value")
        df_flattened = df_flattened.withColumn("validation_key_value", concat_cols_expr)

    # Create validation_query for easy record lookup
    df_flattened = df_flattened.withColumn(
        "validation_query",
        F.concat(
            F.lit("SELECT * FROM ("),
            F.lit(full_parameterized_query),
            F.lit(") WHERE "),
            F.col("unique_key_column"),
            F.lit(" = '"),
            F.col("unique_key_value"),
            F.lit("'")
        )
    )
    
    # Format run_time as string
    df_flattened = df_flattened.withColumn(
        "run_time", 
        F.date_format(F.col("run_time"), "yyyy-MM-dd HH:mm:ss.SSSXXX")
    )

    # Select final columns in desired order
    cols = [
        'product', 'rule_id', 'table_name', 'unique_key_column', 'unique_key_value',
        'validation_rule_name', 'validation_message', 'validation_type',
        'validation_key_column', 'validation_key_value', 'validation_query',
        'run_time', '_errors', '_warnings'
    ]
    result_df = df_flattened.select(*cols)
    
    return result_df

# COMMAND ----------

def write_dqx_validation_details(quarantined_df, key, product, full_parameterized_query):
    """
    Write quarantined data to QDR DQX_Validation_Details table.
    
    Args:
        quarantined_df: The quarantined dataframe with validation failures
        key: The test case identifier (e.g., 'qdr_tc_1')
        product: Product type ('QDR')
        full_parameterized_query: The full SQL query with catalog/schema parameterized
    
    Returns:
        None
    
    Raises:
        Exception: If there's an error writing to the table
    """
    try:
        # Transform quarantined data into structured format
        df = write_quarantine(quarantined_df, key, product, full_parameterized_query)
        
        # Get target table configuration for QDR
        catalog_name = config[env]['DQX_catalog_name']
        schema_name = config[env]['dqx_qdr_schema_name']
        table_name = config[env]['dqx_qdr_table_name']
        
        target_table = f"{catalog_name}.{schema_name}.{table_name}"
        
        # Write to QDR DQX_Validation_Details table
        df.write.mode("append").saveAsTable(target_table)
        
        print(f"✓ DQX_Validation_Details written for {key} to {target_table}")
        
    except Exception as e:
        raise Exception(f"Error while writing quarantined df for test case {key}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Query Processing Helper Functions
# MAGIC
# MAGIC These utility functions process SQL queries from YAML files before execution.
# MAGIC
# MAGIC ### 1. Extract Inner SELECT from CTE
# MAGIC **Purpose:** Extracts the base SELECT query from CTE wrapper
# MAGIC
# MAGIC **Input (from YAML):**
# MAGIC ```sql
# MAGIC with cte as(
# MAGIC   select ... from {catalog_name}.{schema_name}.table ...
# MAGIC )
# MAGIC select * from cte where ...
# MAGIC ```
# MAGIC
# MAGIC **Output (for execution):**
# MAGIC ```sql
# MAGIC select ... from {catalog_name}.{schema_name}.table ...
# MAGIC ```
# MAGIC
# MAGIC **Why:** The WHERE clause logic is handled by DQX validation rules, so we only need the base data retrieval query.
# MAGIC
# MAGIC ### 2. Parameterize SQL Query
# MAGIC **Purpose:** Replaces template placeholders with environment-specific config values
# MAGIC
# MAGIC **Placeholders:**
# MAGIC * `{catalog_name}` → Replaced with catalog from config
# MAGIC
# MAGIC **Why:** Makes YAML queries environment-agnostic - same YAML works in DEV/UAT/PROD.

# COMMAND ----------

def extract_inner_select_from_cte(sql_query):
    """
    Extract the inner SELECT query from a CTE pattern.
    Pattern: with cte as( SELECT ... ) select * from cte where ...
    Returns: Just the SELECT part inside the CTE
    """
    if not sql_query:
        return None
    
    # Pattern to match: with cte as( ... )
    cte_pattern = r'with\s+cte\s+as\s*\(\s*(.+?)\s*\)\s*select\s+\*\s+from\s+cte'
    match = re.search(cte_pattern, sql_query, re.IGNORECASE | re.DOTALL)
    
    if match:
        # Return the inner SELECT query
        return match.group(1).strip()
    else:
        # If no CTE pattern found, return the original query
        return sql_query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute QDR Validation Pipeline
# MAGIC
# MAGIC This section processes all QDR validation rules defined in the YAML file.
# MAGIC
# MAGIC **For each test case:**
# MAGIC 1. Extract and parameterize SQL query from YAML
# MAGIC 2. Execute query to get input dataframe
# MAGIC 3. Apply DQX validation checks
# MAGIC 4. Split results into valid and quarantined records
# MAGIC 5. Write quarantined records to DQX_Validation_Details table
# MAGIC
# MAGIC **Output:** Validation results for all QDR test cases

# COMMAND ----------

print("=" * 80)
print("Starting QDR Validation Pipeline")
print("=" * 80)

for i, key in enumerate(checks.keys()):
    try:
        # Get the check configuration
        check_list = checks[key]
        
        # Skip if check list is empty
        if not check_list:
            print(f"⚠️  Skipping {key} - no checks defined")
            continue
        
        print(f"\n{'='*80}")
        print(f"Processing: {key}")
        print(f"{'='*80}")
        
        # Extract sql_query from user_metadata
        full_sql_query = check_list[0].get('user_metadata', {}).get('sql_query')
        
        if not full_sql_query:
            print(f"⚠️  Skipping {key} - no sql_query defined in YAML")
            continue
        
        # Replace all placeholders in the format {variable.name} with global variable values
        full_parameterized_query = full_sql_query
        placeholders = re.findall(r'\{([^}]+)\}', full_sql_query)
        for placeholder in placeholders:
            if placeholder in globals():
                full_parameterized_query = full_parameterized_query.replace(
                    f'{{{placeholder}}}', 
                    globals()[placeholder]
                )
        
        # Extract only the inner SELECT from the parameterized query (for execution)
        sql_query = extract_inner_select_from_cte(full_parameterized_query)
        
        try:
            # Execute the inner SELECT query to get input dataframe
            print(f"📊 Executing query...")
            input_df = spark.sql(sql_query)
            print(f"   Total records: {input_df.count()}")
        except Exception as e:
            raise Exception(f"Error executing SQL query for test case {key}. The error is: \n {e}")
        
        # Apply validation checks and split into valid/quarantined
        print(f"✓  Applying validation checks...")
        valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks[key])
        
        valid_count = valid_df.count()
        invalid_count = quarantined_df.count()
        
        print(f"   ✓ Valid records: {valid_count}")
        print(f"   ✗ Invalid records: {invalid_count}")
        
        if invalid_count > 0:
            print(f"📝 Writing quarantined records to DQX_Validation_Details...")
            write_dqx_validation_details(quarantined_df, key, "QDR", full_parameterized_query)
        else:
            print(f"✓  No quarantined records - all data passed validation!")
        
    except Exception as e:
        logging.error(f"Error in iteration {i} and test case {key}: {str(e)}")
        print(f"✗ Iteration {i} for test case {key} failed. Error logged. Continuing to next test case.")
        continue

print(f"\n{'='*80}")
print("QDR Validation Pipeline Completed")
print(f"{'='*80}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Quarantined Records (Optional)
# MAGIC
# MAGIC Uncomment the cell below to display the most recent quarantined records

# COMMAND ----------

# Display sample of quarantined records
# display(quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Validation Results
# MAGIC
# MAGIC Use the query below to review validation results from the DQX_Validation_Details table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query QDR validation results
# MAGIC -- Uncomment and update table name as needed
# MAGIC 
# MAGIC -- SELECT 
# MAGIC --   product,
# MAGIC --   rule_id,
# MAGIC --   table_name,
# MAGIC --   validation_rule_name,
# MAGIC --   COUNT(*) as failure_count,
# MAGIC --   MAX(run_time) as latest_run
# MAGIC -- FROM {catalog_name}.{schema_name}.{table_name}
# MAGIC -- WHERE product = 'QDR'
# MAGIC -- GROUP BY product, rule_id, table_name, validation_rule_name
# MAGIC -- ORDER BY latest_run DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pipeline Summary
# MAGIC
# MAGIC **✓ Pipeline Completed Successfully**
# MAGIC
# MAGIC **What was done:**
# MAGIC 1. ✓ Loaded QDR validation rules from YAML
# MAGIC 2. ✓ Executed validation checks for all test cases
# MAGIC 3. ✓ Identified and quarantined invalid records
# MAGIC 4. ✓ Wrote validation details to target table
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Review quarantined records in the DQX_Validation_Details table
# MAGIC 2. Analyze validation failures and identify patterns
# MAGIC 3. Work with data owners to resolve data quality issues
# MAGIC 4. Re-run pipeline after fixes to validate improvements
