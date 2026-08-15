"""Schemas and table metadata for the HubSpot Extended connector."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _properties(*names: str) -> StructType:
    return StructType([StructField(name, StringType(), True) for name in names])


def _crm_schema(properties: StructType) -> StructType:
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("createdAt", TimestampType(), True),
            StructField("updatedAt", TimestampType(), True),
            StructField("archived", BooleanType(), True),
            StructField("properties", properties, True),
        ]
    )


CONTACTS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "lastmodifieddate",
    "firstname",
    "lastname",
    "email",
    "phone",
    "company",
    "jobtitle",
    "lifecyclestage",
    "hubspot_owner_id",
)

COMPANIES_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "name",
    "domain",
    "website",
    "industry",
    "city",
    "state",
    "country",
    "numberofemployees",
    "annualrevenue",
    "hubspot_owner_id",
)

DEALS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "dealname",
    "dealstage",
    "pipeline",
    "dealtype",
    "amount",
    "closedate",
    "hs_is_closed",
    "hs_is_closed_won",
    "hubspot_owner_id",
)

TICKETS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "subject",
    "content",
    "hs_pipeline",
    "hs_pipeline_stage",
    "hs_ticket_priority",
    "hs_ticket_category",
    "source_type",
    "closed_date",
    "hubspot_owner_id",
)

LINE_ITEMS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "name",
    "description",
    "hs_sku",
    "hs_product_id",
    "price",
    "amount",
    "quantity",
    "discount",
    "hs_line_item_currency_code",
)

PRODUCTS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "name",
    "description",
    "hs_sku",
    "price",
    "hs_cost_of_goods_sold",
    "hs_product_type",
    "hs_url",
    "recurringbillingfrequency",
    "tax",
)

_ENGAGEMENT_BASE = (
    "hs_object_id",
    "hs_createdate",
    "hs_lastmodifieddate",
    "hs_timestamp",
    "hubspot_owner_id",
)

CALLS_PROPERTIES = _properties(
    *_ENGAGEMENT_BASE,
    "hs_call_title",
    "hs_call_body",
    "hs_call_direction",
    "hs_call_disposition",
    "hs_call_duration",
    "hs_call_from_number",
    "hs_call_to_number",
    "hs_call_status",
)

EMAILS_PROPERTIES = _properties(
    *_ENGAGEMENT_BASE,
    "hs_email_subject",
    "hs_email_text",
    "hs_email_direction",
    "hs_email_status",
    "hs_email_from_email",
    "hs_email_to_email",
    "hs_email_message_id",
    "hs_email_thread_id",
    "hs_email_headers",
)

MEETINGS_PROPERTIES = _properties(
    *_ENGAGEMENT_BASE,
    "hs_meeting_title",
    "hs_meeting_body",
    "hs_meeting_location",
    "hs_meeting_start_time",
    "hs_meeting_end_time",
    "hs_meeting_outcome",
)

TASKS_PROPERTIES = _properties(
    *_ENGAGEMENT_BASE,
    "hs_task_subject",
    "hs_task_body",
    "hs_task_status",
    "hs_task_priority",
    "hs_task_type",
    "hs_task_is_completed",
    "hs_task_completion_date",
)

NOTES_PROPERTIES = _properties(
    *_ENGAGEMENT_BASE,
    "hs_note_body",
    "hs_attachment_ids",
)

OWNER_TEAM_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("primary", BooleanType(), True),
    ]
)

OWNERS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("type", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("userId", LongType(), True),
        StructField("userIdIncludingInactive", LongType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("archived", BooleanType(), True),
        StructField("teams", ArrayType(OWNER_TEAM_SCHEMA), True),
    ]
)

PIPELINE_STAGE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("label", StringType(), True),
        StructField("displayOrder", LongType(), True),
        StructField("metadata", StringType(), True),
        StructField("archived", BooleanType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
    ]
)

PIPELINES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("label", StringType(), True),
        StructField("displayOrder", LongType(), True),
        StructField("archived", BooleanType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("stages", ArrayType(PIPELINE_STAGE_SCHEMA), True),
    ]
)

TABLE_SCHEMAS: dict[str, StructType] = {
    "contacts": _crm_schema(CONTACTS_PROPERTIES),
    "companies": _crm_schema(COMPANIES_PROPERTIES),
    "deals": _crm_schema(DEALS_PROPERTIES),
    "tickets": _crm_schema(TICKETS_PROPERTIES),
    "calls": _crm_schema(CALLS_PROPERTIES),
    "emails": _crm_schema(EMAILS_PROPERTIES),
    "meetings": _crm_schema(MEETINGS_PROPERTIES),
    "tasks": _crm_schema(TASKS_PROPERTIES),
    "notes": _crm_schema(NOTES_PROPERTIES),
    "owners": OWNERS_SCHEMA,
    "pipelines": PIPELINES_SCHEMA,
    "line_items": _crm_schema(LINE_ITEMS_PROPERTIES),
    "products": _crm_schema(PRODUCTS_PROPERTIES),
}

_CDC_METADATA = {
    "primary_keys": ["id"],
    "cursor_field": "updatedAt",
    "ingestion_type": "cdc",
}

TABLE_METADATA: dict[str, dict] = {
    "contacts": dict(_CDC_METADATA),
    "companies": dict(_CDC_METADATA),
    "deals": dict(_CDC_METADATA),
    "tickets": dict(_CDC_METADATA),
    "calls": dict(_CDC_METADATA),
    "emails": dict(_CDC_METADATA),
    "meetings": dict(_CDC_METADATA),
    "tasks": dict(_CDC_METADATA),
    "notes": dict(_CDC_METADATA),
    "line_items": dict(_CDC_METADATA),
    "products": dict(_CDC_METADATA),
    "owners": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "pipelines": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
}

SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS)

CRM_OBJECTS: set[str] = {
    "contacts",
    "companies",
    "deals",
    "tickets",
    "calls",
    "emails",
    "meetings",
    "tasks",
    "notes",
    "line_items",
    "products",
}

SEARCH_CURSOR_PROPERTIES: dict[str, str] = {
    "contacts": "lastmodifieddate",
    "companies": "hs_lastmodifieddate",
    "deals": "hs_lastmodifieddate",
    "tickets": "hs_lastmodifieddate",
    "calls": "hs_lastmodifieddate",
    "emails": "hs_lastmodifieddate",
    "meetings": "hs_lastmodifieddate",
    "tasks": "hs_lastmodifieddate",
    "notes": "hs_lastmodifieddate",
    "line_items": "hs_lastmodifieddate",
    "products": "hs_lastmodifieddate",
}


def crm_request_properties(table_name: str) -> list[str]:
    properties = TABLE_SCHEMAS[table_name]["properties"].dataType
    assert isinstance(properties, StructType)
    return [field.name for field in properties.fields]
