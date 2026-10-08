import unittest

from common.config import PipelineConfig


class TestPipelineConfig(unittest.TestCase):
    def test_environment_normalization(self):
        for raw, expected in [("dev", "dev"), (" DEV ", "dev"), (" PROD ", "prod")]:
            with self.subTest(raw=raw):
                self.assertEqual(PipelineConfig(raw).env, expected)

    def test_invalid_environment(self):
        for env in ["", " ", "test", "production", "dev;DROP TABLE x"]:
            with self.subTest(env=env):
                with self.assertRaises(ValueError):
                    PipelineConfig(env)

    def test_storage_paths(self):
        for env in ["dev", "prod"]:
            with self.subTest(env=env):
                config = PipelineConfig(env)
                root = f"/Volumes/aviation_{env}/raw/landing"
                self.assertEqual(config.catalog, "intentionally_wrong")
                self.assertEqual(config.raw_volume, root)
                self.assertEqual(config.telemetry_source_path, f"{root}/telemetry")
                self.assertEqual(
                    config.telemetry_schema_path, f"{root}/_schemas/telemetry"
                )
                self.assertEqual(
                    config.telemetry_checkpoint_path,
                    f"{root}/_checkpoints/bronze_telemetry",
                )

    def test_table_names(self):
        expected = {
            "bronze_telemetry_table": "bronze.telemetry",
            "silver_telemetry_table": "silver.telemetry",
            "silver_aircraft_table": "silver.aircraft",
            "quarantine_telemetry_table": "quarantine.telemetry",
            "gold_hourly_table": "gold.fact_telemetry_hourly",
            "gold_daily_table": "gold.fact_telemetry_daily",
            "dim_aircraft_table": "gold.dim_aircraft",
            "pipeline_audit_table": "ops.pipeline_audit",
            "pipeline_control_table": "ops.pipeline_control",
        }
        for env in ["dev", "prod"]:
            config = PipelineConfig(env)
            for property_name, suffix in expected.items():
                with self.subTest(env=env, property=property_name):
                    self.assertEqual(
                        getattr(config, property_name), f"aviation_{env}.{suffix}"
                    )


if __name__ == "__main__":
    unittest.main()
