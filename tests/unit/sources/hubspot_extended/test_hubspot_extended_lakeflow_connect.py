from databricks.labs.community_connector.sources.hubspot_extended.hubspot_extended import (
    HubspotExtendedLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestHubspotExtendedConnector(LakeflowConnectTests):
    connector_class = HubspotExtendedLakeflowConnect
    simulator_source = "hubspot_extended"
    replay_config = {"access_token": "simulator-fake-access-token"}

    def test_v3_cursor_incremental_client_filter_defends_against_ignored_since_param(self):
        records, offset = self.connector.read_table(
            "blog_posts", {"updated": "2024-02-20T10:00:00Z"}, {}
        )

        ids = {record["id"] for record in records}

        assert ids == {"5001", "5002"}
        assert offset == {"updated": "2024-02-28T10:00:00Z"}

    def test_conversation_messages_fanout_is_thread_scoped(self):
        records, offset = self.connector.read_table("conversation_messages", {}, {})
        messages = list(records)

        assert {(record["thread_id"], record["id"]) for record in messages} == {
            ("thread-1", "msg-1"),
            ("thread-1", "msg-2"),
            ("thread-2", "msg-3"),
        }
        assert offset == {"createdAt": "2024-03-02T09:12:00Z"}
