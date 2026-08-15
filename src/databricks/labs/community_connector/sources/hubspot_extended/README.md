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
