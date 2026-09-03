"""
the_conservator: restores archived tables through a Databricks job, on paper.

A stand-in for a customer's archived-data restore flow whose parameter
schema is reproduced here field for field. Users of that flow report that
the new run form obscures default parameter values, and this schema holds
every shape that could be involved:

- six required parameters with no default, one of them untyped
  (``credentials_block_name``) and one a nested object
  (``job_configuration``);
- a nested Pydantic model whose fields carry their own defaults, most of
  them nullable (``anyOf: [type, null]``) with a non-null default such as
  ``"default_job_key"``, ``"Standard_F4s_v2"`` or ``1440``;
- five optional parameters with no type annotation and a string default
  (``kafka_sasl_user`` and friends), which come out of the schema with a
  ``default`` but no ``type``.

The flow does no real work. It logs the parameters it received, reports
which optional ones arrived at their schema default and which were
overridden, and returns the resolved parameters so the run's result shows
exactly what the form submitted.
"""

import json
from typing import Any, Optional

from prefect import flow, get_run_logger
from prefect.utilities.callables import parameter_schema
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Nested parameter model
# ---------------------------------------------------------------------------

class DatabricksJobConfiguration(BaseModel):
    """A configuration class that holds all the job task details necessary to execute in Databricks"""

    databricks_url: str
    cluster_policy: str
    custom_tags: dict[str, Any]
    git_url: Optional[str] = None
    git_branch: Optional[str] = None
    job_key: Optional[str] = "default_job_key"
    job_name: Optional[str] = None
    job_tasks: Optional[list[dict[str, Any]]] = None
    config_env: Optional[str] = None
    config_path: Optional[str] = None
    view_group: Optional[str] = "users"
    manage_group: Optional[str] = "admins"
    file_prefix: Optional[str] = None
    init_scripts: Optional[list[dict[str, Any]]] = None
    node_type_id: Optional[str] = "Standard_F4s_v2"
    driver_node_type_id: Optional[str] = "Standard_DS3_v2"
    req_file_path: Optional[str] = None
    spark_version: Optional[str] = "14.3.x-scala2.12"
    spark_env_vars: Optional[dict[str, Any]] = None
    workflow_tags: Optional[dict[str, Any]] = None
    instance_pool_id: Optional[str] = None
    driver_instance_pool_id: Optional[str] = None
    idempotency_token: Optional[bool] = True
    min_workers_count: int = 1
    max_workers_count: Optional[int] = None
    data_security_mode: Optional[str] = "SINGLE_USER"
    use_databricks_pat: bool = False
    catalog_environment: str = "npe"
    databricks_dr_enabled: bool = False
    databricks_retry_timeout: Optional[int] = 1440
    parallel_tasks_execution: Optional[bool] = False
    apply_policy_default_values: Optional[bool] = True
    prefect_flow_max_wait_seconds: Optional[int] = 86400
    databricks_job_max_wait_seconds: Optional[int] = 86400


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(
    name="the-conservator",
    description=(
        "Pretends to restore a list of archived tables through a Databricks "
        "job. Its parameter schema mirrors a customer's flow: required "
        "untyped and nested-object parameters next to untyped optional "
        "parameters with string defaults. Logs and returns what it received."
    ),
)
def the_conservator(
    credentials_block_name,
    job_configuration: DatabricksJobConfiguration,
    request_id: str,
    requestor_email: str,
    compute_policy: str,
    tables: list[dict[str, Any]],
    kafka_sasl_user="EEH_UDAP_CAPS_DEV",
    kafka_bootstrap_server="dev-brokers-messaging-az.npe.cdf.humana.com:9094",
    vault_address="https://preprod.vault.humana.net",
    restore_status_topic="udapcapabilites_archiveddatarestore_restorestatus",
    environment="dev",
) -> dict[str, Any]:
    """
    Restore archived tables. Nothing is restored; the parameters are the point.

    Args:
        credentials_block_name: Name of the credentials block used for authentication.
        job_configuration: Databricks job configuration.
        request_id: Unique identifier for the restore request.
        requestor_email: Email address of the requestor.
        compute_policy: Compute policy to be applied to the Databricks job.
        tables: List of tables to be restored.
        kafka_sasl_user: Kafka SASL user for authentication.
        kafka_bootstrap_server: Kafka bootstrap server address.
        vault_address: Vault address used to retrieve secrets.
        restore_status_topic: Kafka topic for publishing restore status updates.
        environment: Environment in which the flow is executed.
    """
    logger = get_run_logger()

    received: dict[str, Any] = {
        "credentials_block_name": credentials_block_name,
        "job_configuration": job_configuration.model_dump(),
        "request_id": request_id,
        "requestor_email": requestor_email,
        "compute_policy": compute_policy,
        "tables": tables,
        "kafka_sasl_user": kafka_sasl_user,
        "kafka_bootstrap_server": kafka_bootstrap_server,
        "vault_address": vault_address,
        "restore_status_topic": restore_status_topic,
        "environment": environment,
    }

    logger.info("Received parameters:\n%s", json.dumps(received, indent=2, default=str))

    # Compare every defaulted field, top level and nested, against the schema
    # default so the log says which values the form left alone.
    schema = parameter_schema(the_conservator).model_dump()
    at_default: list[str] = []
    overridden: list[str] = []

    for name, prop in schema["properties"].items():
        if "default" in prop:
            (at_default if received[name] == prop["default"] else overridden).append(name)

    nested = schema["definitions"]["DatabricksJobConfiguration"]["properties"]
    for name, prop in nested.items():
        if "default" in prop:
            path = f"job_configuration.{name}"
            value = received["job_configuration"][name]
            (at_default if value == prop["default"] else overridden).append(path)

    logger.info(
        "%d defaulted parameters arrived at their schema default: %s",
        len(at_default),
        ", ".join(at_default) or "(none)",
    )
    logger.info(
        "%d defaulted parameters were overridden: %s",
        len(overridden),
        ", ".join(overridden) or "(none)",
    )

    for index, table in enumerate(tables, start=1):
        logger.info("Restoring table %d of %d: %s (pretend)", index, len(tables), table)

    logger.info(
        "Restore request %s for %s filed to %s on behalf of %s.",
        request_id,
        environment,
        restore_status_topic,
        requestor_email,
    )
    return received


# ---------------------------------------------------------------------------
# Entrypoint (local dev without a deployment)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    the_conservator(
        credentials_block_name="databricks-dev",
        job_configuration=DatabricksJobConfiguration(
            databricks_url="https://adb-0000000000000000.0.azuredatabricks.net",
            cluster_policy="restore-policy",
            custom_tags={"team": "udap", "purpose": "restore"},
        ),
        request_id="req-0001",
        requestor_email="someone@example.com",
        compute_policy="standard",
        tables=[{"name": "claims_2019", "catalog": "archive"}],
    )
