select
    cast('{{ invocation_id }}' as {{ dbt.type_string() }}) as invocation_id
  , cast('p11_tuva' as {{ dbt.type_string() }}) as package_name
  , cast('{{ p11_tuva.get_tuva_package_version() }}' as {{ dbt.type_string() }}) as package_version
  , cast('{{ project_name }}' as {{ dbt.type_string() }}) as dbt_project_name
  , cast('{{ target.name }}' as {{ dbt.type_string() }}) as dbt_target_name
  , cast('{{ target.type }}' as {{ dbt.type_string() }}) as dbt_target_type
  , cast('{{ flags.WHICH }}' as {{ dbt.type_string() }}) as dbt_command
  , cast('{{ run_started_at }}' as {{ dbt.type_timestamp() }}) as run_started_at
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
