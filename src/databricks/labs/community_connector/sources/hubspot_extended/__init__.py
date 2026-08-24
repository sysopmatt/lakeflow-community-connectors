"""HubSpot Extended source connector."""

from databricks.labs.community_connector.sources.hubspot_extended.hubspot_extended import (
    HubspotExtendedLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class HubspotExtendedDataSource(LakeflowSource):
    _lakeflow_connect_cls = HubspotExtendedLakeflowConnect


__all__ = ["HubspotExtendedDataSource", "HubspotExtendedLakeflowConnect"]
