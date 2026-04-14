-- Shared billing app schema outline
-- This is a starting point for the multi-user migration.
-- It is intentionally explicit and audit-friendly rather than minimal.

create extension if not exists pgcrypto;

create type app_role as enum ('admin', 'billing_ops', 'finance_approver', 'readonly');
create type invoice_status as enum ('draft', 'approved', 'finalized', 'void');
create type import_status as enum ('uploaded', 'parsing', 'normalized', 'failed', 'complete');
create type contract_status as enum ('uploaded', 'parsed', 'verified', 'archived');
create type event_kind as enum ('transaction', 'reversal', 'virtual_account', 'rev_share_summary', 'fx_partner_payout');

create table organizations (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  slug text not null unique,
  created_at timestamptz not null default now()
);

create table app_users (
  id uuid primary key,
  organization_id uuid not null references organizations(id) on delete cascade,
  email text not null,
  full_name text,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  unique (organization_id, email)
);

create table user_roles (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  user_id uuid not null references app_users(id) on delete cascade,
  role app_role not null,
  created_at timestamptz not null default now(),
  unique (organization_id, user_id, role)
);

create table partners (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  name text not null,
  legal_name text,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  unique (organization_id, name)
);

create table partner_aliases (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  partner_id uuid not null references partners(id) on delete cascade,
  alias text not null,
  source text,
  unique (organization_id, alias)
);

create table partner_config (
  partner_id uuid primary key references partners(id) on delete cascade,
  pricing_mode text not null default 'flat',
  notes text,
  updated_by uuid references app_users(id),
  updated_at timestamptz not null default now()
);

create table offline_rates (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  txn_type text not null,
  speed_flag text not null default '',
  min_amt numeric(18,2) not null default 0,
  max_amt numeric(18,2) not null default 0,
  payer_funding text not null default '',
  payee_funding text not null default '',
  fee numeric(18,6) not null,
  payer_ccy text not null default '',
  payee_ccy text not null default '',
  payer_country text not null default '',
  payee_country text not null default '',
  processing_method text not null default '',
  start_date date not null,
  end_date date,
  created_at timestamptz not null default now()
);

create table volume_rates (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  txn_type text not null default '',
  speed_flag text not null default '',
  rate numeric(18,8) not null,
  payer_funding text not null default '',
  payee_funding text not null default '',
  payee_card_type text not null default '',
  ccy_group text not null default '',
  min_vol numeric(18,2) not null default 0,
  max_vol numeric(18,2) not null default 0,
  note text not null default '',
  start_date date not null,
  end_date date,
  created_at timestamptz not null default now()
);

create table fx_rates (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  payer_corridor text not null default '',
  payer_ccy text not null default '',
  payee_corridor text not null default '',
  payee_ccy text not null default '',
  min_txn_size numeric(18,2) not null default 0,
  max_txn_size numeric(18,2) not null default 0,
  min_vol numeric(18,2) not null default 0,
  max_vol numeric(18,2) not null default 0,
  rate numeric(18,8) not null,
  note text not null default '',
  start_date date not null,
  end_date date,
  created_at timestamptz not null default now()
);

create table fee_caps (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  product_type text not null,
  cap_type text not null,
  amount numeric(18,2) not null,
  created_at timestamptz not null default now()
);

create table rev_share_terms (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  txn_type text not null default '',
  speed_flag text not null default '',
  rev_share_pct numeric(18,8) not null,
  start_date date not null,
  end_date date,
  note text not null default '',
  created_at timestamptz not null default now()
);

create table monthly_minimum_terms (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  min_amount numeric(18,2) not null,
  min_vol numeric(18,2) not null default 0,
  max_vol numeric(18,2) not null default 0,
  impl_fee_offset boolean not null default false,
  start_date date not null,
  end_date date,
  created_at timestamptz not null default now()
);

create table platform_fees (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  monthly_fee numeric(18,2) not null,
  start_date date not null,
  end_date date,
  created_at timestamptz not null default now()
);

create table reversal_fees (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  payer_funding text not null default '',
  fee_per_reversal numeric(18,2) not null,
  start_date date not null,
  end_date date,
  created_at timestamptz not null default now()
);

create table implementation_fees (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  fee_type text not null,
  fee_amount numeric(18,2) not null,
  go_live_date date not null,
  apply_against_min boolean not null default false,
  note text not null default '',
  created_at timestamptz not null default now()
);

create table virtual_account_fees (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  fee_type text not null,
  min_accounts integer not null,
  max_accounts integer not null,
  discount numeric(18,8) not null default 0,
  fee_per_account numeric(18,2) not null,
  note text not null default '',
  created_at timestamptz not null default now()
);

create table surcharges (
  id uuid primary key default gen_random_uuid(),
  partner_id uuid not null references partners(id) on delete cascade,
  surcharge_type text not null,
  rate numeric(18,8) not null,
  min_vol numeric(18,2) not null,
  max_vol numeric(18,2) not null,
  note text not null default '',
  start_date date not null,
  end_date date,
  created_at timestamptz not null default now()
);

create table provider_costs (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  provider text not null,
  direction text not null,
  txn_name text not null,
  corridor_type text not null,
  worldlink boolean not null default false,
  min_amt numeric(18,2) not null default 0,
  max_amt numeric(18,2) not null default 0,
  var_fixed text not null,
  fee numeric(18,8) not null,
  payment_or_chargeback text not null,
  created_at timestamptz not null default now()
);

create table workbook_snapshots (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null unique references organizations(id) on delete cascade,
  snapshot_json jsonb not null default '{}'::jsonb,
  source_version integer not null default 1,
  updated_by uuid references app_users(id),
  updated_at timestamptz not null default now()
);

create table import_runs (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  period text,
  status import_status not null default 'uploaded',
  source_label text not null,
  started_by uuid references app_users(id),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  notes text
);

create table import_files (
  id uuid primary key default gen_random_uuid(),
  import_run_id uuid not null references import_runs(id) on delete cascade,
  file_kind text not null,
  original_filename text not null,
  storage_path text not null,
  checksum_sha256 text,
  uploaded_at timestamptz not null default now()
);

create table staging_source_rows (
  id uuid primary key default gen_random_uuid(),
  import_file_id uuid not null references import_files(id) on delete cascade,
  row_number integer not null,
  partner_name_raw text,
  payment_id text,
  account_id text,
  event_date date,
  raw_payload jsonb not null,
  parse_warnings jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);

create table billing_transactions (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  partner_id uuid not null references partners(id) on delete cascade,
  import_run_id uuid not null references import_runs(id) on delete cascade,
  source_family text not null,
  payment_id text not null,
  account_id text,
  credit_complete_date date not null,
  billing_period text not null,
  txn_type text not null,
  speed_flag text not null default '',
  processing_method text not null default '',
  payer_funding text not null default '',
  payee_funding text not null default '',
  payer_ccy text not null default '',
  payee_ccy text not null default '',
  payer_country text not null default '',
  payee_country text not null default '',
  txn_count integer not null default 1,
  total_volume numeric(18,2) not null default 0,
  customer_revenue numeric(18,8) not null default 0,
  revenue_basis text not null default 'gross',
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (organization_id, source_family, payment_id)
);

create table billing_reversals (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  partner_id uuid not null references partners(id) on delete cascade,
  import_run_id uuid not null references import_runs(id) on delete cascade,
  source_family text not null,
  payment_id text not null,
  account_id text,
  refund_completed_date date not null,
  billing_period text not null,
  payer_funding text not null default '',
  reversal_count integer not null default 1,
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (organization_id, source_family, payment_id)
);

create table virtual_account_usage (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  partner_id uuid not null references partners(id) on delete cascade,
  import_run_id uuid not null references import_runs(id) on delete cascade,
  billing_period text not null,
  new_accounts_opened integer not null default 0,
  total_active_accounts integer not null default 0,
  dormant_accounts integer not null default 0,
  closed_accounts integer not null default 0,
  new_business_setups integer not null default 0,
  settlement_count integer not null default 0,
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (organization_id, partner_id, billing_period, import_run_id)
);

create table revenue_share_summaries (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  partner_id uuid not null references partners(id) on delete cascade,
  import_run_id uuid not null references import_runs(id) on delete cascade,
  billing_period text not null,
  net_revenue numeric(18,8) not null default 0,
  partner_revenue_share numeric(18,8) not null default 0,
  revenue_owed numeric(18,8) not null default 0,
  monthly_minimum_revenue numeric(18,8) not null default 0,
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (organization_id, partner_id, billing_period, import_run_id)
);

create table fx_partner_payouts (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  partner_id uuid not null references partners(id) on delete cascade,
  import_run_id uuid not null references import_runs(id) on delete cascade,
  billing_period text not null,
  share_txn_count integer not null default 0,
  reversal_txn_count integer not null default 0,
  share_amount numeric(18,8) not null default 0,
  reversal_amount numeric(18,8) not null default 0,
  partner_payout numeric(18,8) not null default 0,
  note text not null default '',
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (organization_id, partner_id, billing_period, import_run_id)
);

create table contracts (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  partner_id uuid references partners(id) on delete set null,
  status contract_status not null default 'uploaded',
  filename text not null,
  storage_path text not null,
  uploaded_by uuid references app_users(id),
  uploaded_at timestamptz not null default now()
);

create table contract_extractions (
  id uuid primary key default gen_random_uuid(),
  contract_id uuid not null references contracts(id) on delete cascade,
  extracted_json jsonb not null,
  extracted_by uuid references app_users(id),
  extracted_at timestamptz not null default now()
);

create table contract_verifications (
  id uuid primary key default gen_random_uuid(),
  contract_id uuid not null references contracts(id) on delete cascade,
  partner_id uuid not null references partners(id) on delete cascade,
  result_json jsonb not null,
  verified_by uuid references app_users(id),
  verified_at timestamptz not null default now()
);

create table invoices (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  partner_id uuid not null references partners(id) on delete cascade,
  billing_period text not null,
  status invoice_status not null default 'draft',
  import_run_id uuid references import_runs(id),
  snapshot_json jsonb not null default '{}'::jsonb,
  total_charge numeric(18,2) not null default 0,
  total_pay numeric(18,2) not null default 0,
  net_amount numeric(18,2) not null default 0,
  generated_by uuid references app_users(id),
  generated_at timestamptz not null default now(),
  finalized_by uuid references app_users(id),
  finalized_at timestamptz,
  pdf_storage_path text,
  unique (organization_id, partner_id, billing_period, status)
);

create table invoice_lines (
  id uuid primary key default gen_random_uuid(),
  invoice_id uuid not null references invoices(id) on delete cascade,
  line_order integer not null,
  category text not null,
  group_label text not null default '',
  description text not null,
  direction text not null,
  amount numeric(18,2) not null,
  is_active boolean not null default true,
  inactive_reason text,
  minimum_eligible boolean not null default false,
  metadata jsonb not null default '{}'::jsonb
);

create table invoice_line_activity_links (
  id uuid primary key default gen_random_uuid(),
  invoice_line_id uuid not null references invoice_lines(id) on delete cascade,
  activity_kind event_kind not null,
  activity_id uuid not null,
  created_at timestamptz not null default now()
);

create table audit_log (
  id uuid primary key default gen_random_uuid(),
  organization_id uuid not null references organizations(id) on delete cascade,
  actor_user_id uuid references app_users(id),
  entity_type text not null,
  entity_id uuid,
  action text not null,
  before_json jsonb,
  after_json jsonb,
  created_at timestamptz not null default now()
);

create index idx_billing_transactions_partner_period on billing_transactions (partner_id, billing_period);
create index idx_billing_reversals_partner_period on billing_reversals (partner_id, billing_period);
create index idx_invoices_partner_period on invoices (partner_id, billing_period);
create index idx_staging_rows_payment_id on staging_source_rows (payment_id);

-- RLS should be enabled in implementation.
-- Policies should scope all org-owned rows by organization_id and user role.
