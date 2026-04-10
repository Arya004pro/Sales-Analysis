select
  current_timestamp as transformed_at,
  '{{ target.name }}' as dbt_target,
  '{{ env_var("DUCKDB_PATH", "motia/data/analytics.duckdb") }}' as duckdb_path
