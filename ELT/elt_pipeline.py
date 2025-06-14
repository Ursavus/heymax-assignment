import time
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from env import DB_URI
import uuid
import subprocess

class DBConnection:
    def __init__(self, uri):
        self.engine = create_engine(
            uri,
            pool_size=3,
            max_overflow=0,
            pool_timeout=10,
            pool_recycle=1800)

    def execute(self, query, params=None):
        with self.engine.connect() as conn:
            return conn.execute(text(query), params or {})

    def execute_transaction(self, query, params=None):
        with self.engine.begin() as conn:
            return conn.execute(text(query), params or {})

    def read_sql(self, query, params=None):
        return pd.read_sql(query, self.engine, params=params or {})

class IngestionPipeline:
    def __init__(self, pipeline_id: str, db_uri: str):
        self.pipeline_id = pipeline_id

        # Temporary connection to metadata store
        self.metadata_conn = DBConnection(db_uri)

        self._init_tracking_tables()
        self._load_pipeline_definition()

        self.source = DBConnection(db_uri)
        self.target = DBConnection(db_uri)

    def _init_tracking_tables(self):
        self.metadata_conn.execute("""
            CREATE TABLE IF NOT EXISTS orchestrator.pipeline_definitions (
                pipeline_id TEXT PRIMARY KEY,
                source_uri TEXT NOT NULL,
                target_uri TEXT NOT NULL,
                table_name TEXT NOT NULL,
                timestamp_column TEXT NOT NULL,
                max_hours INTEGER DEFAULT 24
            );
        """)
        self.metadata_conn.execute("""
            CREATE TABLE IF NOT EXISTS orchestrator.pipeline_runs (
                run_id UUID PRIMARY KEY,
                pipeline_id TEXT,
                run_start_time TIMESTAMP,
                batch_start_time TIMESTAMP,
                batch_latest_time TIMESTAMP,
                elapsed_ms INTEGER,
                status TEXT
            );
        """)
        self.metadata_conn.execute("""
            CREATE TABLE IF NOT EXISTS orchestrator.pipeline_logs (
                run_id UUID,
                log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                message TEXT
            );
        """)

    def _load_pipeline_definition(self):
        result = self.metadata_conn.execute("""
            SELECT source_uri, target_uri, table_name, timestamp_column, max_hours
            FROM orchestrator.pipeline_definitions
            WHERE pipeline_id = :pipeline_id
        """, {"pipeline_id": self.pipeline_id}).fetchone()

        if not result:
            raise ValueError(f"Pipeline definition for '{self.pipeline_id}' not found.")

        self.source_uri, self.target_uri, self.table, self.timestamp_col, self.max_hours = result

    def log(self, message):
        ts = datetime.now().isoformat()
        self.insert_logs(message)
        print(f"[{ts}] {message}")

    def get_last_run_time(self):
        result = self.metadata_conn.execute(f"""
            SELECT batch_latest_time FROM orchestrator.pipeline_runs
            WHERE pipeline_id = :pipeline_id AND status = 'COMPLETED'
            ORDER BY batch_latest_time DESC LIMIT 1
        """, {"pipeline_id": self.pipeline_id}).fetchone()
        return result[0] if result else pd.Timestamp("2025-03-07")

    def insert_run_record(self, start_time, latest_time, elapsed_ms, status):
        if status == 'COMPLETED':
            result = self.metadata_conn.execute_transaction("""
                INSERT INTO orchestrator.pipeline_runs (run_id, pipeline_id, run_start_time, batch_start_time, batch_latest_time, elapsed_ms, status)
                VALUES (:run_id, :pipeline_id, to_timestamp(:run_start_time), :batch_start_time, :batch_latest_time, :elapsed_ms, :status)
            """, {
                "pipeline_id": self.pipeline_id,
                "run_id": self.run_id,
                "run_start_time": self.t0,
                "batch_start_time": start_time,
                "batch_latest_time": latest_time,
                "elapsed_ms": elapsed_ms,
                "status": status
            })
        else: 
            result = self.metadata_conn.execute_transaction("""
                INSERT INTO orchestrator.pipeline_runs (run_id, pipeline_id, batch_start_time, batch_latest_time, elapsed_ms, status)
                VALUES (:run_id, :pipeline_id, to_timestamp(:run_start_time), :batch_start_time, :batch_latest_time, :elapsed_ms, :status)
            """, {
                "pipeline_id": self.pipeline_id,
                "run_id": self.run_id,
                "run_start_time": self.t0,
                "batch_start_time": None,
                "batch_latest_time": None,
                "elapsed_ms": None,
                "status": 'FAILED'
            })

    def insert_logs(self, message):
        self.metadata_conn.execute_transaction("""
            INSERT INTO orchestrator.pipeline_logs (run_id, message)
            VALUES (:run_id, :message)
        """, {"run_id": self.run_id, "message": message})

    def run(self):
        start_time = self.get_last_run_time()
        self.run_id = uuid.uuid4()
        self.log(f"Starting batch ingestion for {self.table} from {self.timestamp_col} = {start_time}")
        self.t0 = time.time()
        self.log(f"Run starting at {self.t0}")

        query = f"""
            SELECT * FROM {self.table}
            WHERE {self.timestamp_col} > %(start_time)s
            and {self.timestamp_col} < %(start_time)s + interval '%(max_hours)s hours'
        """
        df = self.source.read_sql(query, {'start_time': start_time, 'max_hours': self.max_hours})

        if df.empty:
            self.log("No new data to ingest.")
            self.insert_run_record(0, 0, 0, 'FAILED')

            return

        df.to_sql(self.table.replace('.', '_'), self.target.engine, 'warehouse', if_exists="append", index=False, method="multi")

        latest_time = df[self.timestamp_col].max()
        elapsed_ms = int((time.time() - self.t0) * 1000)

        self.log(f"Ingested {len(df)} rows.")
        self.log(f"Latest event time: {latest_time}, Elapsed: {elapsed_ms} ms")

        self.insert_run_record(start_time, latest_time, elapsed_ms, 'COMPLETED')

def run_dbt():
    result = subprocess.run(
        ["dbt", "run"],
        cwd="dbt_transform/",
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("dbt run failed!")

if __name__ == "__main__":
    while True:
        pipeline = IngestionPipeline('events_ingest', DB_URI)
        pipeline.run()
        run_dbt()
        time.sleep(2)