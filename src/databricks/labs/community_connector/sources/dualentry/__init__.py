"""DualEntry source connector."""

from databricks.labs.community_connector.sources.dualentry.dualentry import (
    DualEntryLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class DualEntryDataSource(LakeflowSource):
    _lakeflow_connect_cls = DualEntryLakeflowConnect
    # Override the Spark format name with the source name once this no longer
    # relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "dualentry"


__all__ = [
    "DualEntryLakeflowConnect",
    "DualEntryDataSource",
]
