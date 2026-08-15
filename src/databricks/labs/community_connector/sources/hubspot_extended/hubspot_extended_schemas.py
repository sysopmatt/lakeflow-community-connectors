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


ASSOCIATION_RESULT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
    ]
)

ASSOCIATION_COLLECTION_SCHEMA = StructType(
    [StructField("results", ArrayType(ASSOCIATION_RESULT_SCHEMA), True)]
)

CRM_ASSOCIATIONS_SCHEMA = StructType(
    [
        StructField("contacts", ASSOCIATION_COLLECTION_SCHEMA, True),
        StructField("companies", ASSOCIATION_COLLECTION_SCHEMA, True),
        StructField("deals", ASSOCIATION_COLLECTION_SCHEMA, True),
        StructField("tickets", ASSOCIATION_COLLECTION_SCHEMA, True),
        StructField("line_items", ASSOCIATION_COLLECTION_SCHEMA, True),
        StructField("products", ASSOCIATION_COLLECTION_SCHEMA, True),
        StructField("quotes", ASSOCIATION_COLLECTION_SCHEMA, True),
    ]
)


def _crm_schema_with_associations(properties: StructType) -> StructType:
    return StructType(
        _crm_schema(properties).fields
        + [StructField("associations", CRM_ASSOCIATIONS_SCHEMA, True)]
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

LEADS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_lead_name",
    "hs_lead_label",
    "hs_lead_type",
    "hs_lead_stage",
    "hs_pipeline",
    "hs_pipeline_stage",
    "hs_lead_disqualification_reason",
    "hubspot_owner_id",
)

QUOTES_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_title",
    "hs_status",
    "hs_expiration_date",
    "hs_quote_amount",
    "hs_currency",
    "hs_language",
    "hs_terms",
    "hs_public_url_key",
    "hs_pdf_download_link",
    "hubspot_owner_id",
)

FEEDBACK_SUBMISSIONS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_submission_name",
    "hs_survey_type",
    "hs_survey_id",
    "hs_value",
    "hs_response_group",
    "hs_sentiment",
    "hs_content",
    "hs_contact_id",
)

APPOINTMENTS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_appointment_name",
    "hs_appointment_start",
    "hs_appointment_end",
    "hs_appointment_location",
    "hs_appointment_status",
    "hs_pipeline",
    "hs_pipeline_stage",
    "hubspot_owner_id",
)

LISTINGS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_listing_name",
    "hs_listing_type",
    "hs_listing_status",
    "hs_listing_url",
    "hs_price",
    "hs_currency",
    "hs_address",
    "hubspot_owner_id",
)

ORDERS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_order_name",
    "hs_order_status",
    "hs_order_amount",
    "hs_currency_code",
    "hs_source_store",
    "hs_fulfillment_status",
    "hs_payment_status",
    "hs_external_order_id",
)

CARTS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_cart_name",
    "hs_cart_status",
    "hs_cart_amount",
    "hs_currency_code",
    "hs_source_store",
    "hs_abandoned_cart_url",
    "hs_external_cart_id",
)

COMMERCE_PAYMENTS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_payment_name",
    "hs_payment_status",
    "hs_payment_amount",
    "hs_currency_code",
    "hs_payment_method",
    "hs_processor",
    "hs_external_payment_id",
)

SUBSCRIPTIONS_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_subscription_name",
    "hs_subscription_status",
    "hs_recurring_billing_period",
    "hs_next_billing_date",
    "hs_start_date",
    "hs_end_date",
    "hs_currency_code",
    "hs_subscription_amount",
)

INVOICES_PROPERTIES = _properties(
    "hs_object_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_invoice_number",
    "hs_invoice_status",
    "hs_invoice_amount",
    "hs_balance_due",
    "hs_due_date",
    "hs_currency_code",
    "hs_external_invoice_id",
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

COMMUNICATIONS_PROPERTIES = _properties(
    *_ENGAGEMENT_BASE,
    "hs_communication_body",
    "hs_communication_channel_type",
    "hs_communication_logged_from",
    "hs_communication_global_id",
    "hs_body_preview",
)

POSTAL_MAIL_PROPERTIES = _properties(
    *_ENGAGEMENT_BASE,
    "hs_postal_mail_body",
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

MARKETING_EMAILS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("state", StringType(), True),
        StructField("type", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("publishedAt", TimestampType(), True),
        StructField("archived", BooleanType(), True),
        StructField("authorName", StringType(), True),
        StructField("campaign", StringType(), True),
    ]
)

EMAIL_EVENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("created", LongType(), True),
        StructField("recipient", StringType(), True),
        StructField("portalId", LongType(), True),
        StructField("appId", LongType(), True),
        StructField("sentBy", StringType(), True),
        StructField("emailCampaignId", LongType(), True),
        StructField("smtpId", StringType(), True),
        StructField("url", StringType(), True),
        StructField("response", StringType(), True),
    ]
)

FORM_FIELD_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("label", StringType(), True),
        StructField("type", StringType(), True),
        StructField("fieldType", StringType(), True),
        StructField("required", BooleanType(), True),
    ]
)

FORMS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("guid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("formType", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("archived", BooleanType(), True),
        StructField("publishedAt", TimestampType(), True),
        StructField("fields", ArrayType(FORM_FIELD_SCHEMA), True),
    ]
)

FORM_SUBMISSION_VALUE_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("value", StringType(), True),
        StructField("objectTypeId", StringType(), True),
    ]
)

FORM_SUBMISSION_PAGE_SCHEMA = StructType(
    [
        StructField("pageUrl", StringType(), True),
        StructField("pageName", StringType(), True),
    ]
)

FORM_SUBMISSIONS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("form_id", StringType(), True),
        StructField("submittedAt", TimestampType(), True),
        StructField("contact_id", StringType(), True),
        StructField("conversionId", StringType(), True),
        StructField("page", FORM_SUBMISSION_PAGE_SCHEMA, True),
        StructField("values", ArrayType(FORM_SUBMISSION_VALUE_SCHEMA), True),
    ]
)

MARKETING_EVENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("externalEventId", StringType(), True),
        StructField("externalAccountId", StringType(), True),
        StructField("eventName", StringType(), True),
        StructField("eventType", StringType(), True),
        StructField("startDateTime", TimestampType(), True),
        StructField("endDateTime", TimestampType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("eventOrganizer", StringType(), True),
        StructField("eventUrl", StringType(), True),
    ]
)

CAMPAIGNS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("campaignCode", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("archived", BooleanType(), True),
        StructField("color", StringType(), True),
        StructField("notes", StringType(), True),
    ]
)

BLOG_POSTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("created", TimestampType(), True),
        StructField("updated", TimestampType(), True),
        StructField("publishDate", TimestampType(), True),
        StructField("authorName", StringType(), True),
        StructField("htmlTitle", StringType(), True),
        StructField("metaDescription", StringType(), True),
        StructField("tagIds", ArrayType(StringType()), True),
    ]
)

BLOG_TAGS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("language", StringType(), True),
        StructField("created", TimestampType(), True),
        StructField("updated", TimestampType(), True),
    ]
)

BLOG_AUTHORS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("email", StringType(), True),
        StructField("bio", StringType(), True),
        StructField("created", TimestampType(), True),
        StructField("updated", TimestampType(), True),
    ]
)

CMS_PAGE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("publishedAt", TimestampType(), True),
        StructField("htmlTitle", StringType(), True),
        StructField("metaDescription", StringType(), True),
    ]
)

HUBDB_TABLES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("label", StringType(), True),
        StructField("published", BooleanType(), True),
        StructField("rowCount", LongType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
    ]
)

URL_REDIRECTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("routePrefix", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("redirectStyle", LongType(), True),
        StructField("isOnlyAfterNotFound", BooleanType(), True),
        StructField("isMatchFullUrl", BooleanType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
    ]
)

LISTS_SCHEMA = StructType(
    [
        StructField("listId", StringType(), True),
        StructField("name", StringType(), True),
        StructField("objectTypeId", StringType(), True),
        StructField("listType", StringType(), True),
        StructField("processingType", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("createdById", StringType(), True),
    ]
)

SUBSCRIPTION_DEFINITIONS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("purpose", StringType(), True),
        StructField("active", BooleanType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
    ]
)

BEHAVIORAL_EVENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("eventType", StringType(), True),
        StructField("objectId", StringType(), True),
        StructField("occurredAt", TimestampType(), True),
        StructField("email", StringType(), True),
        StructField("utk", StringType(), True),
        StructField("properties", StructType([StructField("source", StringType(), True)]), True),
    ]
)

CONVERSATION_INBOXES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("channelTypes", ArrayType(StringType()), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("archived", BooleanType(), True),
    ]
)

CONVERSATION_THREADS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("inboxId", StringType(), True),
        StructField("status", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("latestMessageTimestamp", TimestampType(), True),
        StructField("archived", BooleanType(), True),
    ]
)

MESSAGE_SENDER_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("email", StringType(), True),
    ]
)

CONVERSATION_MESSAGES_SCHEMA = StructType(
    [
        StructField("thread_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("type", StringType(), True),
        StructField("text", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("sender", MESSAGE_SENDER_SCHEMA, True),
    ]
)

PROPERTY_OPTION_SCHEMA = StructType(
    [
        StructField("label", StringType(), True),
        StructField("value", StringType(), True),
        StructField("description", StringType(), True),
        StructField("displayOrder", LongType(), True),
        StructField("hidden", BooleanType(), True),
    ]
)

PROPERTY_DEFINITION_SCHEMA = StructType(
    [
        StructField("objectType", StringType(), True),
        StructField("name", StringType(), True),
        StructField("label", StringType(), True),
        StructField("description", StringType(), True),
        StructField("groupName", StringType(), True),
        StructField("type", StringType(), True),
        StructField("fieldType", StringType(), True),
        StructField("options", ArrayType(PROPERTY_OPTION_SCHEMA), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("archived", BooleanType(), True),
    ]
)

SCHEMA_LABELS_SCHEMA = StructType(
    [
        StructField("singular", StringType(), True),
        StructField("plural", StringType(), True),
    ]
)

CRM_SCHEMAS_SCHEMA = StructType(
    [
        StructField("objectTypeId", StringType(), True),
        StructField("name", StringType(), True),
        StructField("fullyQualifiedName", StringType(), True),
        StructField("labels", SCHEMA_LABELS_SCHEMA, True),
        StructField("primaryDisplayProperty", StringType(), True),
        StructField("requiredProperties", ArrayType(StringType()), True),
        StructField("searchableProperties", ArrayType(StringType()), True),
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
    ]
)

ANALYTICS_VIEWS_SCHEMA = StructType(
    [
        StructField("breakdown", StringType(), True),
        StructField("period", StringType(), True),
        StructField("visits", LongType(), True),
        StructField("pageviews", LongType(), True),
        StructField("sessions", LongType(), True),
        StructField("contacts", LongType(), True),
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
    "leads": _crm_schema_with_associations(LEADS_PROPERTIES),
    "quotes": _crm_schema_with_associations(QUOTES_PROPERTIES),
    "feedback_submissions": _crm_schema_with_associations(FEEDBACK_SUBMISSIONS_PROPERTIES),
    "appointments": _crm_schema_with_associations(APPOINTMENTS_PROPERTIES),
    "listings": _crm_schema_with_associations(LISTINGS_PROPERTIES),
    "orders": _crm_schema_with_associations(ORDERS_PROPERTIES),
    "carts": _crm_schema_with_associations(CARTS_PROPERTIES),
    "commerce_payments": _crm_schema_with_associations(COMMERCE_PAYMENTS_PROPERTIES),
    "subscriptions": _crm_schema_with_associations(SUBSCRIPTIONS_PROPERTIES),
    "invoices": _crm_schema_with_associations(INVOICES_PROPERTIES),
    "communications": _crm_schema_with_associations(COMMUNICATIONS_PROPERTIES),
    "postal_mail": _crm_schema_with_associations(POSTAL_MAIL_PROPERTIES),
    "marketing_emails": MARKETING_EMAILS_SCHEMA,
    "email_events": EMAIL_EVENTS_SCHEMA,
    "forms": FORMS_SCHEMA,
    "form_submissions": FORM_SUBMISSIONS_SCHEMA,
    "marketing_events": MARKETING_EVENTS_SCHEMA,
    "campaigns": CAMPAIGNS_SCHEMA,
    "blog_posts": BLOG_POSTS_SCHEMA,
    "blog_tags": BLOG_TAGS_SCHEMA,
    "blog_authors": BLOG_AUTHORS_SCHEMA,
    "landing_pages": CMS_PAGE_SCHEMA,
    "site_pages": CMS_PAGE_SCHEMA,
    "hubdb_tables": HUBDB_TABLES_SCHEMA,
    "url_redirects": URL_REDIRECTS_SCHEMA,
    "lists": LISTS_SCHEMA,
    "subscription_definitions": SUBSCRIPTION_DEFINITIONS_SCHEMA,
    "behavioral_events": BEHAVIORAL_EVENTS_SCHEMA,
    "conversation_inboxes": CONVERSATION_INBOXES_SCHEMA,
    "conversation_threads": CONVERSATION_THREADS_SCHEMA,
    "conversation_messages": CONVERSATION_MESSAGES_SCHEMA,
    "properties": PROPERTY_DEFINITION_SCHEMA,
    "crm_schemas": CRM_SCHEMAS_SCHEMA,
    "analytics_views": ANALYTICS_VIEWS_SCHEMA,
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
    "leads": dict(_CDC_METADATA),
    "quotes": dict(_CDC_METADATA),
    "feedback_submissions": dict(_CDC_METADATA),
    "appointments": dict(_CDC_METADATA),
    "listings": dict(_CDC_METADATA),
    "orders": dict(_CDC_METADATA),
    "carts": dict(_CDC_METADATA),
    "commerce_payments": dict(_CDC_METADATA),
    "subscriptions": dict(_CDC_METADATA),
    "invoices": dict(_CDC_METADATA),
    "communications": dict(_CDC_METADATA),
    "postal_mail": dict(_CDC_METADATA),
    "marketing_emails": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
    "email_events": {
        "primary_keys": ["id"],
        "cursor_field": "created",
        "ingestion_type": "cdc",
    },
    "forms": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
    "form_submissions": {
        "primary_keys": ["form_id", "submittedAt", "contact_id"],
        "cursor_field": "submittedAt",
        "ingestion_type": "cdc",
    },
    "marketing_events": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
    "campaigns": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
    "blog_posts": {
        "primary_keys": ["id"],
        "cursor_field": "updated",
        "ingestion_type": "cdc",
    },
    "blog_tags": {
        "primary_keys": ["id"],
        "cursor_field": "updated",
        "ingestion_type": "cdc",
    },
    "blog_authors": {
        "primary_keys": ["id"],
        "cursor_field": "updated",
        "ingestion_type": "cdc",
    },
    "landing_pages": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
    "site_pages": {
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
        "ingestion_type": "cdc",
    },
    "owners": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "pipelines": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "hubdb_tables": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "url_redirects": {"primary_keys": ["id"], "ingestion_type": "snapshot"},
    "lists": {"primary_keys": ["listId"], "ingestion_type": "snapshot"},
    "subscription_definitions": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "behavioral_events": {
        "primary_keys": ["id"],
        "cursor_field": "occurredAt",
        "ingestion_type": "cdc",
    },
    "conversation_inboxes": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "conversation_threads": {
        "primary_keys": ["id"],
        "cursor_field": "latestMessageTimestamp",
        "ingestion_type": "cdc",
    },
    "conversation_messages": {
        "primary_keys": ["thread_id", "id"],
        "cursor_field": "createdAt",
        "ingestion_type": "cdc",
    },
    "properties": {
        "primary_keys": ["objectType", "name"],
        "ingestion_type": "snapshot",
    },
    "crm_schemas": {
        "primary_keys": ["objectTypeId"],
        "ingestion_type": "snapshot",
    },
    "analytics_views": {
        "primary_keys": ["breakdown", "period"],
        "ingestion_type": "snapshot",
    },
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
    "leads",
    "quotes",
    "feedback_submissions",
    "appointments",
    "listings",
    "orders",
    "carts",
    "commerce_payments",
    "subscriptions",
    "invoices",
    "communications",
    "postal_mail",
}

V3_CURSOR_TABLE_PATHS: dict[str, str] = {
    "marketing_emails": "/marketing/v3/emails/",
    "forms": "/marketing/v3/forms/",
    "marketing_events": "/marketing/v3/marketing-events/",
    "campaigns": "/marketing/v3/campaigns/",
    "blog_posts": "/cms/v3/blogs/posts",
    "blog_tags": "/cms/v3/blogs/tags",
    "blog_authors": "/cms/v3/blogs/authors",
    "landing_pages": "/cms/v3/pages/landing-pages",
    "site_pages": "/cms/v3/pages/site-pages",
    "behavioral_events": "/events/v3/events",
    "conversation_threads": "/conversations/v3/conversations/threads",
}

LEGACY_OFFSET_TABLE_PATHS: dict[str, str] = {
    "email_events": "/email/public/v1/events",
}

SNAPSHOT_TABLE_PATHS: dict[str, str] = {
    "hubdb_tables": "/cms/v3/hubdb/tables",
    "url_redirects": "/cms/v3/url-redirects",
    "lists": "/crm/v3/lists/",
    "subscription_definitions": "/communication-preferences/v3/definitions",
    "conversation_inboxes": "/conversations/v3/conversations/inboxes",
    "crm_schemas": "/crm/v3/schemas",
    "analytics_views": "/analytics/v2/reports/sources/total",
}

PROPERTY_OBJECT_TYPES: tuple[str, ...] = (
    "contacts",
    "companies",
    "deals",
    "tickets",
    "products",
    "line_items",
)

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
    "leads": "hs_lastmodifieddate",
    "quotes": "hs_lastmodifieddate",
    "feedback_submissions": "hs_lastmodifieddate",
    "appointments": "hs_lastmodifieddate",
    "listings": "hs_lastmodifieddate",
    "orders": "hs_lastmodifieddate",
    "carts": "hs_lastmodifieddate",
    "commerce_payments": "hs_lastmodifieddate",
    "subscriptions": "hs_lastmodifieddate",
    "invoices": "hs_lastmodifieddate",
    "communications": "hs_lastmodifieddate",
    "postal_mail": "hs_lastmodifieddate",
}


def crm_request_properties(table_name: str) -> list[str]:
    properties = TABLE_SCHEMAS[table_name]["properties"].dataType
    assert isinstance(properties, StructType)
    return [field.name for field in properties.fields]
