from dataclasses import dataclass

ALLOWED_ENVS = {"dev", "prod"}


@dataclass(frozen=True)
class PipelineConfig:
    env: str

    def __post_init__(self):
        normalized_env = self.env.strip().lower()

        if normalized_env not in ALLOWED_ENVS:
            raise ValueError(
                f"Invalid environment: {self.env}. "
                f"Allowed environments: {sorted(ALLOWED_ENVS)}"
            )

        object.__setattr__(self, "env", normalized_env)

    # ---------------------------------------------------------
    # Environment
    # ---------------------------------------------------------

    @property
    def catalog(self):
        return f"aviation_{self.env}"

    # ---------------------------------------------------------
    # Storage
    # ---------------------------------------------------------

    @property
    def raw_volume(self):
        return f"/Volumes/{self.catalog}/raw/landing"

    @property
    def telemetry_source_path(self):
        return f"{self.raw_volume}/telemetry"

    @property
    def telemetry_schema_path(self):
        return f"{self.raw_volume}/_schemas/telemetry"

    @property
    def telemetry_checkpoint_path(self):
        return f"{self.raw_volume}/_checkpoints/bronze_telemetry"

    # ---------------------------------------------------------
    # Bronze
    # ---------------------------------------------------------

    @property
    def bronze_telemetry_table(self):
        return f"{self.catalog}.bronze.telemetry"

    # ---------------------------------------------------------
    # Silver
    # ---------------------------------------------------------

    @property
    def silver_telemetry_table(self):
        return f"{self.catalog}.silver.telemetry"

    @property
    def silver_aircraft_table(self):
        return f"{self.catalog}.silver.aircraft"

    # ---------------------------------------------------------
    # Quarantine
    # ---------------------------------------------------------

    @property
    def quarantine_telemetry_table(self):
        return f"{self.catalog}.quarantine.telemetry"

    # ---------------------------------------------------------
    # Gold
    # ---------------------------------------------------------

    @property
    def gold_hourly_table(self):
        return f"{self.catalog}.gold.fact_telemetry_hourly"

    @property
    def gold_daily_table(self):
        return f"{self.catalog}.gold.fact_telemetry_daily"

    @property
    def dim_aircraft_table(self):
        return f"{self.catalog}.gold.dim_aircraft"

    # ---------------------------------------------------------
    # Operations
    # ---------------------------------------------------------

    @property
    def pipeline_audit_table(self):
        return f"{self.catalog}.ops.pipeline_audit"

    @property
    def pipeline_control_table(self):
        return f"{self.catalog}.ops.pipeline_control"
