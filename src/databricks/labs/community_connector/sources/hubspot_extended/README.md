# HubSpot Extended

HubSpot Extended is a Lakeflow Community Connector for HubSpot CRM v3 objects.

This scaffold supports contacts, companies, deals, tickets, calls, emails,
meetings, tasks, notes, owners, pipelines, line items, products, leads, quotes,
feedback submissions, appointments, listings, orders, carts, commerce payments,
subscriptions, invoices, communications, and postal mail. CRM object tables use
HubSpot cursor pagination and incremental search on the object updated
timestamp. Snapshot tables are listed from their HubSpot CRM v3 endpoints.

Marketing streams include marketing emails, email events, forms, form
submissions, marketing events, and campaigns. Email events use the legacy
offset/hasMore feed so recipient-level send/open/click/bounce rows are
preserved. Form submissions fan out by first listing forms and then reading each
form's submissions endpoint.

CMS and content streams include blog posts, blog tags, blog authors, landing
pages, site pages, HubDB tables, URL redirects, CRM lists, and communication
subscription definitions. Blog resources use the `updated` cursor field; CMS
pages use `updatedAt`; HubDB tables, URL redirects, lists, and subscription
definitions are snapshot tables.

Final metadata and activity streams include behavioral events, conversation
inboxes, conversation threads, conversation messages, CRM property definitions,
CRM schemas, and analytics views. Conversation messages fan out by first
listing conversation threads and then reading each thread's messages endpoint.
Property definitions fan out over contacts, companies, deals, tickets,
products, and line items. Analytics views are aggregate report snapshots keyed
by breakdown and period; HubSpot does not expose row-level traffic analytics
through these report endpoints, so these tables are snapshots rather than true
CDC streams.
