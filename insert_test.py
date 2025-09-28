import logging
from pyspark.sql import SparkSession
import dbutils

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Test logging messages
logging.info("This is an info message")
logging.warning("This is a warning message")
logging.error("This is an error message")

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# -------------------------------
# Configuration
# -------------------------------
DBFS_TMP_PATH = "dbfs:/tmp/smatracker"      # DBFS path
LOCAL_PATH = "/dbfs/tmp/smatracker"         # Local path for git commands
REPO_URL = "https://github.com/dominikgebhardt/smatracker.git"  # Replace with your repo
FOLDERS_TO_CLONE = ["databricks"]   # List of folders for sparse checkout
SECRET_SCOPE = "smatracker-prod"
SECRET_KEY = "smatracker-github"

# -------------------------------
# 1. Ensure directory exists
# -------------------------------
logging.info(f"Creating DBFS directory: {DBFS_TMP_PATH}")
dbutils.fs.mkdirs(DBFS_TMP_PATH)
os.makedirs(LOCAL_PATH, exist_ok=True)

# -------------------------------
# 2. Get GitHub token from Databricks Secrets
# -------------------------------
logging.info("Retrieving GitHub token from Databricks Secrets")
GITHUB_TOKEN = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)

# -------------------------------
# 3. Sparse clone using subprocess
# -------------------------------
def run_cmd(cmd, cwd=None):
    """Run shell command and log output"""
    logging.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)
    if result.stdout:
        logging.info(result.stdout)
    if result.stderr:
        logging.warning(result.stderr)

# Insert one row into your test table
try:
    logging.info("Initializing git repository")
    run_cmd(["git", "init", LOCAL_PATH])

    os.chdir(LOCAL_PATH)

    repo_with_token = REPO_URL.replace("https://", f"https://{GITHUB_TOKEN}@")
    logging.info(f"Adding remote: {REPO_URL}")
    run_cmd(["git", "remote", "add", "origin", repo_with_token])

    logging.info("Enabling sparse checkout")
    run_cmd(["git", "sparse-checkout", "init", "--cone"])

    logging.info(f"Setting sparse checkout folders: {FOLDERS_TO_CLONE}")
    run_cmd(["git", "sparse-checkout", "set"] + FOLDERS_TO_CLONE)

    logging.info("Pulling content from GitHub")
    run_cmd(["git", "pull", "origin", "main"])  # change branch if needed

    files = dbutils.fs.ls("dbfs:/tmp/my_repo")
    for f in files:
        logging.info(f"Name: {f.name}, Path: {f.path}, Size: {f.size} bytes")

    
    spark.sql("""
    INSERT INTO smatracker_prod.bronze.test (person)
    VALUES ('Harry Hohl')
    """)
    logging.info("Row inserted successfully into smatracker_prod.bronze.test")
    logging.info("Try to clone git repo")
    dbutils.fs.mkdirs(DBFS_TMP_PATH)  # ensures parent exists
    
except Exception as e:
    logging.error(f"Failed to insert row: {e}")
