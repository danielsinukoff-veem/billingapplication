create or replace function public.current_organization_id()
returns uuid
language sql
stable
as $$
  select nullif(auth.jwt() ->> 'organization_id', '')::uuid
$$;

create or replace function public.has_app_role(required_role app_role)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from public.user_roles ur
    where ur.user_id = auth.uid()
      and ur.organization_id = public.current_organization_id()
      and ur.role = required_role
  );
$$;

alter table organizations enable row level security;
alter table app_users enable row level security;
alter table user_roles enable row level security;
alter table partners enable row level security;
alter table partner_aliases enable row level security;
alter table partner_config enable row level security;
alter table offline_rates enable row level security;
alter table volume_rates enable row level security;
alter table fx_rates enable row level security;
alter table fee_caps enable row level security;
alter table rev_share_terms enable row level security;
alter table monthly_minimum_terms enable row level security;
alter table platform_fees enable row level security;
alter table reversal_fees enable row level security;
alter table implementation_fees enable row level security;
alter table virtual_account_fees enable row level security;
alter table surcharges enable row level security;
alter table provider_costs enable row level security;
alter table workbook_snapshots enable row level security;
alter table import_runs enable row level security;
alter table import_files enable row level security;
alter table staging_source_rows enable row level security;
alter table billing_transactions enable row level security;
alter table billing_reversals enable row level security;
alter table virtual_account_usage enable row level security;
alter table revenue_share_summaries enable row level security;
alter table fx_partner_payouts enable row level security;
alter table contracts enable row level security;
alter table contract_extractions enable row level security;
alter table contract_verifications enable row level security;
alter table invoices enable row level security;
alter table invoice_lines enable row level security;
alter table invoice_line_activity_links enable row level security;
alter table audit_log enable row level security;

create policy organization_read_partners on partners
for select using (organization_id = public.current_organization_id());

create policy organization_manage_partners on partners
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
);

create policy organization_read_provider_costs on provider_costs
for select using (organization_id = public.current_organization_id());

create policy organization_manage_provider_costs on provider_costs
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
);

create policy organization_read_workbook_snapshots on workbook_snapshots
for select using (organization_id = public.current_organization_id());

create policy organization_manage_workbook_snapshots on workbook_snapshots
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
);

create policy organization_read_import_runs on import_runs
for select using (organization_id = public.current_organization_id());

create policy organization_manage_import_runs on import_runs
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
);

create policy organization_read_billing_transactions on billing_transactions
for select using (organization_id = public.current_organization_id());

create policy organization_manage_billing_transactions on billing_transactions
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
);

create policy organization_read_billing_reversals on billing_reversals
for select using (organization_id = public.current_organization_id());

create policy organization_manage_billing_reversals on billing_reversals
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
);

create policy organization_read_virtual_account_usage on virtual_account_usage
for select using (organization_id = public.current_organization_id());

create policy organization_manage_virtual_account_usage on virtual_account_usage
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
);

create policy organization_read_revenue_share_summaries on revenue_share_summaries
for select using (organization_id = public.current_organization_id());

create policy organization_manage_revenue_share_summaries on revenue_share_summaries
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
);

create policy organization_read_fx_partner_payouts on fx_partner_payouts
for select using (organization_id = public.current_organization_id());

create policy organization_manage_fx_partner_payouts on fx_partner_payouts
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops'))
);

create policy organization_read_invoices on invoices
for select using (organization_id = public.current_organization_id());

create policy organization_manage_invoices on invoices
for all using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops') or public.has_app_role('finance_approver'))
)
with check (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('billing_ops') or public.has_app_role('finance_approver'))
);

create policy organization_read_audit_log on audit_log
for select using (
  organization_id = public.current_organization_id()
  and (public.has_app_role('admin') or public.has_app_role('finance_approver'))
);
