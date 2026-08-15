from databricks.labs.community_connector.sources.hubspot_extended.hubspot_extended import (
    HubspotExtendedLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestHubspotExtendedConnector(LakeflowConnectTests):
    connector_class = HubspotExtendedLakeflowConnect
    simulator_source = "hubspot_extended"
    replay_config = {"access_token": "simulator-fake-access-token"}
