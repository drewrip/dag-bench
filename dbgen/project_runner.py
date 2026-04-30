from importlib import import_module


def run_project(project_name: str) -> None:
    import_module(f"dbgen.{project_name}").main()
