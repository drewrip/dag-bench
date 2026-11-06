"""Source data for projects/p12_stripe.

dbt_stripe ships only ~260 rows of integration-test seeds, so the Fivetran
Stripe connector schema is synthesized here instead, in DuckDB SQL. Every
random choice is a hash of (tag, row key), so the output is deterministic.

The business: one Stripe account selling subscriptions (monthly, yearly and
weekly prices over 50 products) and one-off charges, 2023-01-01 .. 2025-12-31
(the window pinned by dagbench_start_date / dagbench_as_of_date in
p12_stripe/dbt_project.yml). Keys are consistent across tables: invoices and
line items follow each subscription's billing periods, charges pay invoices,
balance transactions/fees/refunds/disputes hang off charges, and one automatic
payout per business day sweeps the funds that became available. Volume grows
over the window with weekly and daily shape; ids are Stripe-style random
strings; ~3% of rows in livemode-filtered tables are test-mode objects.

Each table carries every column dbt_stripe's get_<table>_columns() macro lists
(parsed from p12_stripe/macros/staging/), NULL unless populated below, so the
raw tables are as wide as the connector's. See SPEC.md for row counts.

Output: <project>/data/warehouse.duckdb, cached as dbgen/.cache/p12_sf<sf>.duckdb
like dbgen/project_runner.py does.
"""
from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = REPO_ROOT / "dbgen" / ".cache"
OUTPUT_PATH = Path("data/warehouse.duckdb")

START = "2023-01-01"
AS_OF = "2025-12-31 23:59:59"
SYNCED = "2026-01-01 00:00:00"

# Rows per unit of sf: sf=1 is a ~1 GB warehouse.duckdb (~12M source rows, ~1.9M charges).
CUSTOMERS_PER_SF = 50_000
SUBSCRIPTION_SHARE = 0.6          # share of customers with a subscription
ONE_OFF_CHARGES_PER_CUSTOMER = 30
TRANSFERS_PER_CUSTOMER = 0.2

# Volume grows over the window: event density ∝ exp(GROWTH · fraction of window),
# so the last day sees e^GROWTH ≈ 4.5x the first day's signups and charges.
GROWTH = 1.5
# Constant cancellation hazards: per month for monthly/weekly plans, per renewal for yearly.
MONTHLY_CHURN = 0.04
YEARLY_CHURN = 0.25
# Share of rows duplicated as test-mode (livemode = false) objects, in the tables
# whose staging models filter on livemode (LIVEMODE_TABLES).
TEST_MODE_SHARE = 0.03
LIVEMODE_TABLES = {
    "charge", "coupon", "credit_note", "credit_note_line_item", "customer", "dispute", "invoice",
    "invoice_line_item", "payment_intent", "payment_method", "payout", "price", "product",
    "subscription", "transfer",
}
# Columns holding object ids (besides `id` and `*_id`). Generated ids are readable
# structured keys ("ch_in_<sub>_<period>_<attempt>"); the final write maps each to
# <prefix>_<24 base62 chars> from its hash, consistently across tables.
EXTRA_ID_COLUMNS = {
    "source", "balance_transaction", "latest_invoice_id", "invoice_settings_default_payment_method",
    "type_id", "destination", "destination_payment", "unique_id",
}
BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

N_PRODUCTS = 50
N_COUPONS = 100
FX = {"usd": 1.0, "eur": 1.08, "gbp": 1.27}
# Local list prices relative to usd.
LIST_PRICE = {"usd": 1.0, "eur": 0.92, "gbp": 0.79}

# Source table -> get_<x>_columns macro.
TABLES = {
    "account": "account", "balance_transaction": "balance_transaction", "card": "card",
    "charge": "charge", "coupon": "coupon", "credit_note": "credit_note",
    "credit_note_line_item": "credit_note_line_item", "customer": "customer",
    "discount": "discount", "dispute": "dispute", "fee": "fee", "invoice": "invoice",
    "invoice_line_item": "invoice_line_item", "payment_intent": "payment_intent",
    "payment_method": "payment_method", "payment_method_card": "payment_method_card",
    "payout": "payout", "payout_balance_transaction": "payout_balance_transaction",
    "price": "price_plan", "product": "product", "refund": "refund",
    "subscription": "subscription", "subscription_discount": "subscription_discount",
    "subscription_item": "subscription_item", "transfer": "transfer",
}
DUCKDB_TYPES = {"string": "VARCHAR", "int": "BIGINT", "timestamp": "TIMESTAMP", "boolean": "BOOLEAN",
                "float": "DOUBLE", "numeric": "DOUBLE"}
COLUMN = re.compile(r'"name":\s*"([^"]+)",\s*"datatype":\s*(?:dbt\.type_)?"?([a-z_]+)')
# Defaults for columns most tables have.
COMMON = {
    "_fivetran_synced": f"timestamp '{SYNCED}'",
    "livemode": "true",
    "is_deleted": "false",
    "_fivetran_active": "true",
}


def connector_columns(project_dir: Path, macro: str) -> list[tuple[str, str]]:
    text = (project_dir / "macros" / "staging" / f"get_{macro}_columns.sql").read_text()
    if macro == "price_plan":  # the `price` branch; `plan` is the legacy table
        text = text.split("{% else %}")[0]
    columns: dict[str, tuple[str, str]] = {}
    for name, kind in COLUMN.findall(text):
        if name == "fivetran_synced":  # upstream typo for _fivetran_synced
            continue
        # START/END appear twice (Snowflake upper-cased branch); keep the lowercase one.
        if name.lower() not in columns or name.islower():
            columns[name.lower()] = (name, DUCKDB_TYPES[kind])
    return list(columns.values())


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


MACROS = f"""
create or replace macro rnd(tag, k) as (hash(tag, k) % 1000000) / 1000000.0;
create or replace macro rnd2(tag, k, j) as (hash(tag, k, j) % 1000000) / 1000000.0;
create or replace macro t0() as timestamp '{START}';
create or replace macro as_of() as timestamp '{AS_OF}';
create or replace macro window_seconds() as date_diff('second', t0(), as_of());
create or replace macro secs(n) as to_seconds(cast(floor(n) as bigint));
create or replace macro fx(cur) as case cur {' '.join(f"when '{c}' then {r}" for c, r in FX.items())} end;
-- Fraction of the window with density ∝ exp(GROWTH·x), and its CDF.
create or replace macro grow(u) as ln(1 + u * (exp({GROWTH}) - 1)) / {GROWTH};
create or replace macro grow_cdf(x) as (exp({GROWTH} * x) - 1) / (exp({GROWTH}) - 1);
-- Weekend days get ~half a weekday's volume (40% of weekend events move to a random
-- weekday of the previous week), and
-- times of day are triangular around 13:00 (06:00-20:24). Kept inside the window.
create or replace macro weekday(ts, u, v) as case when dayofweek(ts) in (0, 6) and u < 0.4
    then ts - to_days(case dayofweek(ts) when 6 then 1 else 2 end + floor(v * 5)::int) else ts end;
create or replace macro shape(ts, tag, k) as least(as_of(), greatest(t0(),
    cast(date_trunc('day', weekday(ts, rnd2(tag, k, 0), rnd2(tag, k, 3))) as timestamp)
    + secs(86400 * (0.25 + 0.3 * (rnd2(tag, k, 1) + rnd2(tag, k, 2))))));
-- Payouts don't happen on weekends: roll Sat/Sun to Monday.
create or replace macro bizday(d) as cast(d + to_days(case dayofweek(d) when 6 then 2 when 0 then 1 else 0 end) as timestamp);
create or replace macro b62(h0, h1, h2) as {" || ".join(f"substr('{BASE62}', ((h{j} // {62 ** m}) % 62)::int + 1, 1)" for j in range(3) for m in range(8))};
"""


def staging_sql(n_cust: int, n_oneoff: int, n_transfer: int) -> list[str]:
    """Temp tables holding the generated entities with their keys."""
    return [
        # Products: 9 prices each (month / year / week x usd / eur / gbp).
        f"""create temp table _price as
        select p as product_idx, iv, cur.currency,
            'price_' || p || '_' || iv || '_' || cur.currency as price_id, 'prod_' || p as product_id,
            ['month', 'year', 'week'][iv + 1] as recurring_interval,
            cast(round(case iv when 0 then base when 1 then base * 10 else base / 4.0 end * cur.factor) as bigint) as unit_amount
        from (select p, (1 + p % 20) * 1000 - 1 + (p // 20) * 500 as base from range({N_PRODUCTS}) t(p)) products,
             range(3) i(iv),
             (values {", ".join(f"('{c}', {f})" for c, f in LIST_PRICE.items())}) cur(currency, factor)""",

        # Signups over the first 95% of the window, growing. Customer i's signup
        # fraction is increasing in i, so customers < floor(n * grow_cdf(x / 0.95))
        # all signed up before fraction x of the window.
        f"""create temp table _cust as
        select i as cust_idx, 'cus_' || i as customer_id,
            shape(t0() + secs(0.95 * grow((i + rnd('cust_created', i)) / {n_cust}) * window_seconds()), 'cust_shape', i) as created,
            ['US','US','US','US','US','US','US','CA','GB','GB','DE','FR','AU','NL'][1 + floor(rnd('country', i) * 14)::int] as country,
            1 + (rnd('second_card', i) < 0.3)::int as n_cards,
            rnd('taxed', i) < 0.3 as taxed
        from range({n_cust}) t(i)""",
        """alter table _cust add column currency varchar""",
        """update _cust set currency = case country when 'GB' then 'gbp' when 'DE' then 'eur'
            when 'FR' then 'eur' when 'NL' then 'eur' else 'usd' end""",

        """create temp table _card as
        select c.cust_idx, k, c.customer_id,
            'card_' || c.cust_idx || '_' || k as card_id, 'pm_' || c.cust_idx || '_' || k as payment_method_id,
            ['Visa','Visa','Visa','MasterCard','MasterCard','American Express','Discover'][1 + floor(rnd2('brand', c.cust_idx, k) * 7)::int] as brand,
            case when rnd2('funding', c.cust_idx, k) < 0.7 then 'credit' when rnd2('funding', c.cust_idx, k) < 0.97 then 'debit' else 'prepaid' end as funding,
            c.country, c.created + secs(k * 86400 * 30) as created
        from _cust c, range(2) t(k) where k < c.n_cards""",

        # Subscriptions: one for SUBSCRIPTION_SHARE of customers, billed in the
        # customer's currency; interval month 75% / year 20% / week 5%. Lifetimes
        # are geometric (constant cancellation hazard); cancellations take effect
        # at period end and only if before the as-of date.
        f"""create temp table _sub as
        select c.cust_idx as sub_idx, 'sub_' || c.cust_idx as subscription_id, c.customer_id, c.cust_idx, c.currency,
            case when rnd('interval', c.cust_idx) < 0.75 then 0 when rnd('interval', c.cust_idx) < 0.95 then 1 else 2 end as iv,
            least(greatest(c.created, shape(c.created + secs(rnd('sub_start', c.cust_idx) * 86400 * 30), 'sub_shape', c.cust_idx)),
                  as_of() - interval 1 day) as started,
            1 + (rnd('item2', c.cust_idx) < 0.35)::int + (rnd('item3', c.cust_idx) < 0.10)::int as n_items,
            ln(1 - rnd('sub_life', c.cust_idx)) as log_u
        from _cust c where rnd('has_sub', c.cust_idx) < {SUBSCRIPTION_SHARE}""",
        f"""create temp table _sub2 as
        select * exclude (log_u, life_months),
            case when started + to_months(life_months) <= as_of() then started + to_months(life_months) end as ended
        from (select *, case iv when 1 then 12 * (1 + floor(log_u / ln(1 - {YEARLY_CHURN})))::int
                                else 1 + floor(log_u / ln(1 - {MONTHLY_CHURN}))::int end as life_months
              from _sub)""",
        # Billing periods: start + p * interval, while it starts before the end.
        """create temp table _period as
        with s as (select *, coalesce(ended, as_of()) as eff_end from _sub2)
        select s.sub_idx, p,
            case s.iv when 1 then s.started + to_years(p) when 2 then s.started + to_weeks(p) else s.started + to_months(p) end as period_start,
            case s.iv when 1 then s.started + to_years(p + 1) when 2 then s.started + to_weeks(p + 1) else s.started + to_months(p + 1) end as period_end
        from s, range(160) t(p)
        where p < case s.iv when 1 then 4 when 2 then 160 else 37 end
          and case s.iv when 1 then s.started + to_years(p) when 2 then s.started + to_weeks(p) else s.started + to_months(p) end < s.eff_end""",
        """create temp table _sub_last as
        select sub_idx, max(p) as last_p, arg_max(period_start, p) as cur_start, arg_max(period_end, p) as cur_end
        from _period group by 1""",
        """create temp table _item as
        select s.sub_idx, k, 'si_' || s.sub_idx || '_' || k as subscription_item_id,
            pr.price_id, pr.unit_amount,
            case when k = 0 then 0 else floor(rnd2('item_start', s.sub_idx, k) * least(6, l.last_p + 1))::int end as start_p,
            1 + floor(pow(rnd2('qty', s.sub_idx, k), 3) * 20)::int as base_qty
        from _sub2 s join _sub_last l using (sub_idx), range(3) t(k)
        join _price pr on pr.product_idx = (floor(rnd('product', s.sub_idx) * 50)::int + k * 17) % 50
            and pr.iv = s.iv and pr.currency = s.currency
        where k < s.n_items""",
        # Quantity steps every 6 periods, so MRR expands and contracts.
        """create temp table _line as
        select i.sub_idx, per.p, i.k, i.subscription_item_id, i.price_id, i.unit_amount,
            per.period_start, per.period_end,
            greatest(1, i.base_qty + floor((rnd2('qty_step', i.subscription_item_id, per.p // 6) - 0.45) * 4)::int) as quantity
        from _item i join _period per on per.sub_idx = i.sub_idx and per.p >= i.start_p""",

        # Coupons and subscription-level discounts.
        f"""create temp table _coupon as
        select k, 'co_' || lpad(k::varchar, 4, '0') as coupon_id,
            ['once', 'repeating', 'forever'][1 + k % 3] as duration,
            case when k % 3 = 1 then 3 + k % 10 end as duration_in_months,
            case when k % 2 = 0 then 5.0 + (k % 5) * 5 end as percent_off,
            case when k % 2 = 1 then 500 * (1 + k % 4) end as amount_off
        from range({N_COUPONS}) t(k)""",
        """create temp table _subdisc as
        select s.sub_idx, s.subscription_id, s.customer_id, 'di_' || s.sub_idx as discount_id, c.*,
            per.period_start as disc_start,
            case c.duration when 'once' then per.period_start + interval 1 month
                            when 'repeating' then per.period_start + to_months(c.duration_in_months) end as disc_end
        from _sub2 s
        join _coupon c on c.k = floor(rnd('coupon', s.sub_idx) * 100)::int
        join _period per on per.sub_idx = s.sub_idx and per.p = least(floor(rnd('coupon_p', s.sub_idx) * 3)::int,
            (select last_p from _sub_last l where l.sub_idx = s.sub_idx))
        where rnd('has_coupon', s.sub_idx) < 0.12""",

        """create temp table _inv as
        with agg as (
            select sub_idx, p, min(period_start) as period_start, min(period_end) as period_end, sum(unit_amount * quantity) as subtotal
            from _line group by 1, 2
        ), disc as (
            select a.*, d.discount_id, d.coupon_id,
                case when d.discount_id is null then 0
                     when d.percent_off is not null then round(a.subtotal * d.percent_off / 100)
                     else least(d.amount_off, a.subtotal) end::bigint as discount_amount
            from agg a left join _subdisc d on d.sub_idx = a.sub_idx
                and a.period_start >= d.disc_start and (d.disc_end is null or a.period_start < d.disc_end)
        )
        select d.*, s.subscription_id, s.customer_id, s.cust_idx, s.currency,
            'in_' || d.sub_idx || '_' || d.p as invoice_id,
            'pi_in_' || d.sub_idx || '_' || d.p as payment_intent_id,
            case when c.taxed then round((d.subtotal - d.discount_amount) * 0.07) else 0 end::bigint as tax,
            rnd2('inv_status', d.sub_idx, d.p) as r,
            d.p = l.last_p as is_last
        from disc d join _sub2 s using (sub_idx) join _cust c using (cust_idx) join _sub_last l using (sub_idx)""",
        """create temp table _inv2 as
        select *, subtotal - discount_amount + tax as total,
            case when r < 0.015 then 'void'
                 when r < 0.035 then 'uncollectible'
                 when is_last and (r < 0.06 or period_start > as_of() - interval 3 day) then 'open'
                 else 'paid' end as status
        from _inv""",
        """create temp table _inv3 as
        select *, case status when 'void' then 0 when 'uncollectible' then 3 when 'open' then 1
                    else 1 + (rnd2('retry', sub_idx, p) < 0.05)::int end as attempts
        from _inv2""",

        # Charges: invoice payments (failed retries first) and one-off charges.
        """create temp table _inv_charge as
        select i.invoice_id, i.payment_intent_id, i.customer_id, i.cust_idx, i.currency, i.total as amount,
            'ch_in_' || i.sub_idx || '_' || i.p || '_' || a as charge_id,
            least(i.period_start + interval 1 hour + to_days(a), as_of()) as created,
            i.status = 'paid' and a = i.attempts - 1 as succeeded, true as captured
        from _inv3 i, range(3) t(a) where a < i.attempts and i.total > 0""",
        # One-off charges: pick the time first (growing volume), then a customer
        # who had signed up by then, skewed towards long-standing customers.
        f"""create temp table _oneoff as
        select *, guest_draw or n_signed_up = 0 as guest,
            floor(pow(rnd('who', i), 1.8) * n_signed_up)::bigint as cust_idx
        from (
            select i, x, rnd('guest', i) < 0.08 as guest_draw, rnd('ch_status', i) as r,
                least({n_cust}, floor({n_cust} * grow_cdf(least(x / 0.95, 1))))::bigint as n_signed_up
            from (select i, grow(rnd('ch_created', i)) as x from range({n_oneoff}) t(i))
        )""",
        """create temp table _charge as
        select charge_id, payment_intent_id, invoice_id, customer_id, cust_idx, currency, amount, created,
            case when succeeded then 'succeeded' else 'failed' end as status, captured,
            0 as card_k
        from _inv_charge
        union all
        select 'ch_' || o.i, 'pi_' || o.i, null,
            case when o.guest then null else c.customer_id end,
            case when o.guest then null else c.cust_idx end,
            case when o.guest then 'usd' else c.currency end,
            cast(200 + floor(pow(rnd('amount', o.i), 2.5) * 60000) as bigint),
            greatest(coalesce(case when not o.guest then c.created end, t0()),
                     shape(t0() + secs(o.x * window_seconds()), 'ch_shape', o.i)),
            case when o.r < 0.05 then 'failed' else 'succeeded' end,
            not (o.r < 0.06),
            case when o.guest then null else floor(rnd('card_pick', o.i) * c.n_cards)::int end
        from _oneoff o left join _cust c on c.cust_idx = o.cust_idx and not o.guest""",

        """create temp table _refund as
        select 're_' || substr(ch.charge_id, 4) as refund_id, ch.charge_id, ch.payment_intent_id, ch.currency,
            case when rnd('rf_full', ch.charge_id) < 0.6 then ch.amount
                 else floor(ch.amount * (0.2 + 0.6 * rnd('rf_part', ch.charge_id)))::bigint end as amount,
            ch.created + secs(86400 * (1 + rnd('rf_when', ch.charge_id) * 30)) as created,
            ['requested_by_customer', 'requested_by_customer', 'duplicate', 'fraudulent'][1 + floor(rnd('rf_reason', ch.charge_id) * 4)::int] as reason
        from _charge ch
        where ch.status = 'succeeded' and ch.captured and rnd('refund', ch.charge_id) < 0.05
          and ch.created + secs(86400 * (1 + rnd('rf_when', ch.charge_id) * 30)) <= as_of()""",
        """create temp table _dispute as
        select 'dp_' || substr(ch.charge_id, 4) || '_' || n as dispute_id, ch.charge_id, ch.currency, ch.amount,
            ch.created + secs(86400 * (10 + rnd2('dp_when', ch.charge_id, n) * 50 + 60 * n)) as created,
            case when r < 0.35 then 'won' when r < 0.70 then 'lost' when r < 0.80 then 'needs_response'
                 when r < 0.90 then 'under_review' when r < 0.95 then 'warning_closed' else 'warning_under_review' end as status,
            ['fraudulent', 'fraudulent', 'product_not_received', 'duplicate', 'subscription_canceled', 'general'][1 + floor(rnd2('dp_reason', ch.charge_id, n) * 6)::int] as reason
        from (select *, rnd('dp_status', charge_id) as r from _charge) ch, range(2) t(n)
        where ch.status = 'succeeded' and ch.captured and rnd('dispute', ch.charge_id) < 0.004
          and (n = 0 or rnd('dp_twice', ch.charge_id) < 0.05)
          and ch.created + secs(86400 * (10 + rnd2('dp_when', ch.charge_id, n) * 50 + 60 * n)) <= as_of()""",
        f"""create temp table _transfer as
        select 'tr_' || i as transfer_id,
            shape(t0() + secs(grow(rnd('tr_created', i)) * window_seconds()), 'tr_shape', i) as created,
            cast(1000 + floor(pow(rnd('tr_amount', i), 2) * 200000) as bigint) as amount,
            'acct_conn' || (i % 50) as destination,
            rnd('tr_reversed', i) < 0.02 as reversed
        from range({n_transfer}) t(i)""",

        # Balance transactions other than payouts.
        """create temp table _bt as
        select 'txn_' || charge_id as balance_transaction_id, 'charge' as type, 'charge' as reporting_category,
            charge_id as source, created, currency as src_currency,
            round(amount * fx(currency))::bigint as amount,
            (round(round(amount * fx(currency)) * 0.029) + 30 + case when currency <> 'usd' then round(round(amount * fx(currency)) * 0.01) else 0 end)::bigint as fee,
            case when invoice_id is not null then 'Payment for Invoice' end as description
        from _charge where status = 'succeeded' and captured
        union all
        select 'txn_' || refund_id, 'refund', 'refund', refund_id, created, currency,
            -round(amount * fx(currency))::bigint, 0, 'REFUND FOR CHARGE (' || reason || ')'
        from _refund
        union all
        select 'txn_' || dispute_id, 'adjustment', 'dispute', dispute_id, created, currency,
            -round(amount * fx(currency))::bigint, 1500, 'Chargeback withdrawal'
        from _dispute where status not like 'warning%'
        union all
        select 'txn_' || dispute_id || '_rev', 'adjustment', 'dispute_reversal', dispute_id,
            least(created + interval 30 day, as_of()), currency,
            round(amount * fx(currency))::bigint, -1500, 'Chargeback reversal'
        from _dispute where status = 'won'
        union all
        select 'txn_' || transfer_id, 'transfer', 'transfer', transfer_id, created, 'usd', -amount, 0, null
        from _transfer""",
        """create temp table _bt2 as
        select *, amount - fee as net, cast(date_trunc('day', created) + interval 2 day as timestamp) as available_on
        from _bt""",
        # One automatic payout per business day, sweeping what became available
        # since the previous one (weekend availability goes out on Monday).
        """create temp table _payout as
        select 'po_' || strftime(d, '%Y%m%d') as payout_id, d, cast(d + interval 6 hour as timestamp) as created,
            bizday(d + interval 2 day) as arrival_date, net as amount,
            rnd('po_fail', strftime(d, '%Y%m%d')) < 0.005 as failed
        from (select bizday(available_on) as d, sum(net) as net from _bt2 group by 1)
        where net > 0 and d <= as_of()""",
    ]


def final_sql(project_dir: Path) -> dict[str, str]:
    """Table -> select of its populated columns (the rest are filled with NULLs)."""
    return {
        "account": """select 'acct_1DagBench' as id, 'DagBench Inc.' as business_profile_name, '5734' as business_profile_mcc,
            'company' as business_type, true as charges_enabled, 'San Francisco' as company_address_city, 'US' as company_address_country,
            '1 Market St' as company_address_line_1, '94105' as company_address_postal_code, 'CA' as company_address_state,
            'DagBench Inc.' as company_name, 'US' as country, timestamp '2022-06-01 09:00:00' as created, 'usd' as default_currency,
            true as details_submitted, 'billing@dagbench.example' as email, true as payouts_enabled, 'standard' as type""",
        "product": f"""select 'prod_' || p as id, true as active, timestamp '2022-06-01' + to_days(p) as created,
            ['Starter','Basic','Team','Pro','Business'][1 + p % 5] || ' ' || p as name, 'service' as type,
            'Plan ' || p as description, 'DAGBENCH' as statement_descriptor, case when p % 4 = 0 then 'seat' end as unit_label,
            timestamp '2022-06-01' + to_days(p) as updated
            from range({N_PRODUCTS}) t(p)""",
        "price": """select price_id as id, true as active, 'per_unit' as billing_scheme,
            timestamp '2022-06-01' + to_days(product_idx) as created, currency, product_id,
            recurring_interval, 1 as recurring_interval_count, 'licensed' as recurring_usage_type,
            unit_amount, unit_amount::varchar as unit_amount_decimal,
            recurring_interval || 'ly ' || currency as nickname
            from _price""",
        "coupon": """select coupon_id as id, amount_off, timestamp '2022-12-01' + to_days(k) as created,
            case when amount_off is not null then 'usd' end as currency, duration, duration_in_months,
            'Coupon ' || k as name, percent_off, true as valid, 0 as times_redeemed
            from _coupon""",
        "customer": """select c.customer_id as id, 0 as account_balance, 0 as balance, c.created, c.currency,
            'card_' || c.cust_idx || '_0' as default_card_id, 'pm_' || c.cust_idx || '_0' as invoice_settings_default_payment_method,
            coalesce(s.delinquent, false) as delinquent, 'Customer ' || c.cust_idx as description,
            'customer' || c.cust_idx || '@example.com' as email, 'Customer ' || c.cust_idx as name,
            'City ' || (c.cust_idx % 500) as address_city, c.country as address_country, c.cust_idx || ' Main St' as address_line_1,
            lpad((c.cust_idx % 99999)::varchar, 5, '0') as address_postal_code,
            case when c.country = 'US' then ['CA','NY','TX','WA','IL','FL'][1 + c.cust_idx % 6] end as address_state,
            case when rnd('ship', c.cust_idx) < 0.4 then 'City ' || (c.cust_idx % 500) end as shipping_address_city,
            case when rnd('ship', c.cust_idx) < 0.4 then c.country end as shipping_address_country,
            case when rnd('ship', c.cust_idx) < 0.4 then c.cust_idx || ' Main St' end as shipping_address_line_1,
            case when rnd('ship', c.cust_idx) < 0.4 then 'Customer ' || c.cust_idx end as shipping_name,
            upper(substr(md5('prefix' || c.cust_idx), 1, 8)) as invoice_prefix,
            case when c.taxed then 'none' else 'exempt' end as tax_exempt,
            rnd('deleted', c.cust_idx) < 0.01 as is_deleted
            from _cust c left join (
                select customer_id, bool_or(status in ('open', 'uncollectible') and is_last) as delinquent from _inv3 group by 1
            ) s using (customer_id)""",
        "card": """select card_id as id, customer_id, brand, funding, country, created, 'Customer ' || cust_idx as name,
            'City ' || (cust_idx % 500) as address_city, country as address_country, cust_idx || ' Main St' as address_line_1,
            lpad((cust_idx % 99999)::varchar, 5, '0') as address_zip,
            case when rnd2('wallet', cust_idx, k) < 0.15 then 'apple_pay' when rnd2('wallet', cust_idx, k) < 0.25 then 'google_pay' end as wallet_type
            from _card""",
        "payment_method": """select payment_method_id as id, customer_id, 'card' as type, created,
            'City ' || (cust_idx % 500) as billing_detail_address_city, country as billing_detail_address_country,
            cust_idx || ' Main St' as billing_detail_address_line_1,
            lpad((cust_idx % 99999)::varchar, 5, '0') as billing_detail_address_postal_code,
            'customer' || cust_idx || '@example.com' as billing_detail_email, 'Customer ' || cust_idx as billing_detail_name
            from _card""",
        "payment_method_card": """select payment_method_id, brand, funding, 'card' as type,
            case when rnd2('wallet', cust_idx, k) < 0.15 then 'apple_pay' when rnd2('wallet', cust_idx, k) < 0.25 then 'google_pay' end as wallet_type,
            case when rnd2('3ds', cust_idx, k) < 0.2 then 'frictionless' end as three_d_secure_authentication_flow,
            case when rnd2('3ds', cust_idx, k) < 0.2 then 'authenticated' end as three_d_secure_result,
            case when rnd2('3ds', cust_idx, k) < 0.2 then '2.2.0' end as three_d_secure_version
            from _card""",
        "subscription": """select s.subscription_id as id, s.customer_id, s.started as created, s.started as start_date,
            s.started as billing_cycle_anchor, 'charge_automatically' as billing,
            case when s.ended is not null then 'canceled'
                 when li.status in ('open', 'uncollectible') then 'past_due' else 'active' end as status,
            s.ended as canceled_at, s.ended as ended_at,
            case when s.ended is null and rnd('cape', s.sub_idx) < 0.03 then l.cur_end end as cancel_at,
            s.ended is null and rnd('cape', s.sub_idx) < 0.03 as cancel_at_period_end,
            l.cur_start as current_period_start, l.cur_end as current_period_end,
            li.invoice_id as latest_invoice_id, 'pm_' || s.cust_idx || '_0' as default_payment_method_id,
            s.n_items as quantity
            from _sub2 s join _sub_last l using (sub_idx)
            join _inv3 li on li.sub_idx = s.sub_idx and li.p = l.last_p""",
        "subscription_item": """select i.subscription_item_id as id, i.subscription_id, i.price_id as plan_id,
            i.created, l.cur_start as current_period_start, l.cur_end as current_period_end, ln.quantity
            from (select i.*, s.subscription_id, per.period_start as created
                  from _item i join _sub2 s using (sub_idx) join _period per on per.sub_idx = i.sub_idx and per.p = i.start_p) i
            join _sub_last l using (sub_idx)
            join (select subscription_item_id, arg_max(quantity, p) as quantity from _line group by 1) ln using (subscription_item_id)""",
        "subscription_discount": """select discount_id as id, coupon_id, customer_id, subscription_id,
            disc_start as "start", disc_end as "end"
            from _subdisc""",
        "invoice_line_item": """select 'il_' || l.sub_idx || '_' || l.p || '_' || l.k as id,
            'in_' || l.sub_idx || '_' || l.p as invoice_id, l.unit_amount * l.quantity as amount, s.currency,
            l.quantity || ' × ' || ['Starter','Basic','Team','Pro','Business'][1 + pr.product_idx % 5] || ' ' || pr.product_idx
                || ' (at ' || l.unit_amount || ' / ' || pr.recurring_interval || ')' as description,
            true as discountable, l.period_start, l.period_end, l.price_id as plan_id, l.price_id, false as proration,
            l.quantity, s.subscription_id, l.subscription_item_id, 'subscription' as type,
            'il_' || l.sub_idx || '_' || l.p || '_' || l.k as unique_id, l.unit_amount::varchar as unit_amount_excluding_tax
            from _line l join _sub2 s using (sub_idx) join _price pr on pr.price_id = l.price_id""",
        "invoice": """select i.invoice_id as id, i.total as amount_due,
            case when i.status = 'paid' then i.total else 0 end as amount_paid,
            case when i.status in ('open', 'uncollectible') then i.total else 0 end as amount_remaining,
            0 as post_payment_credit_notes_amount, 0 as pre_payment_credit_notes_amount,
            i.attempts as attempt_count, i.attempts > 0 as attempted, i.status not in ('void', 'uncollectible') as auto_advance,
            'charge_automatically' as billing,
            case when i.p = 0 then 'subscription_create' else 'subscription_cycle' end as billing_reason,
            ch.charge_id, i.period_start as created, i.currency, i.customer_id, i.period_start as date,
            i.period_start + interval 1 hour as finalized_at,
            upper(substr(md5('prefix' || i.cust_idx), 1, 8)) || '-' || lpad((i.p + 1)::varchar, 4, '0') as number,
            i.status = 'paid' as paid, 'pm_' || i.cust_idx || '_0' as default_payment_method_id,
            case when i.attempts > 0 and i.total > 0 then i.payment_intent_id end as payment_intent_id,
            i.subscription_id, i.period_end, i.period_start, 0 as starting_balance, 0 as ending_balance,
            'DAGBENCH' as statement_descriptor, i.status, i.subtotal, i.tax, case when i.tax > 0 then 7.0 end as tax_percent,
            i.total, i.period_start + interval 1 hour as status_transitions_finalized_at,
            case when i.status = 'paid' then ch.created end as status_transitions_paid_at,
            case when i.status = 'void' then i.period_start + interval 2 day end as status_transitions_voided_at,
            case when i.status = 'uncollectible' then i.period_start + interval 3 day end as status_transitions_marked_uncollectible_at,
            null as description
            from _inv3 i left join (
                select invoice_id, arg_max(charge_id, created) as charge_id, max(created) as created from _charge where invoice_id is not null group by 1
            ) ch using (invoice_id)""",
        "discount": """select i.discount_id as id, 'invoice' as type, i.invoice_id as type_id, i.discount_amount as amount,
            i.coupon_id, i.customer_id, i.invoice_id, i.subscription_id, d.disc_start as "start", d.disc_end as "end"
            from _inv3 i join _subdisc d using (discount_id) where i.discount_amount > 0""",
        "credit_note": """select 'cn_' || substr(invoice_id, 4) as id, floor(total * 0.5)::bigint as amount,
            least(period_start + interval 5 day, as_of()) as created, currency, 0 as discount_amount,
            floor(total * 0.5)::bigint as subtotal, floor(total * 0.5)::bigint as total, 'Partial credit' as memo,
            upper(substr(md5('prefix' || cust_idx), 1, 8)) || '-CN-' || lpad((p + 1)::varchar, 4, '0') as number, 'product_unsatisfactory' as reason, 'issued' as status,
            'post_payment' as type, invoice_id
            from _inv3 where status = 'paid' and total > 1 and rnd('credit_note', invoice_id) < 0.01""",
        "credit_note_line_item": """select 'cnli_' || substr(invoice_id, 4) as id, 'cn_' || substr(invoice_id, 4) as credit_note_id,
            floor(total * 0.5)::bigint as amount, 0 as discount_amount, 'Partial credit' as description, 1 as quantity,
            'custom_line_item' as type, floor(total * 0.5)::bigint as unit_amount, floor(total * 0.5)::bigint as unit_amount_decimal
            from _inv3 where status = 'paid' and total > 1 and rnd('credit_note', invoice_id) < 0.01""",
        "charge": """select ch.charge_id as id, ch.amount, coalesce(rf.amount, 0) as amount_refunded,
            case when ch.status = 'succeeded' and ch.captured then 'txn_' || ch.charge_id end as balance_transaction_id,
            'DAGBENCH' as calculated_statement_descriptor, ch.captured,
            case when ch.card_k is not null then 'card_' || ch.cust_idx || '_' || ch.card_k end as card_id,
            ch.created, ch.currency, ch.customer_id,
            case when ch.invoice_id is not null then 'Subscription update' else 'One-off purchase' end as description,
            case when ch.status = 'failed' then ['card_declined','card_declined','expired_card','incorrect_cvc','processing_error'][1 + floor(rnd('fail_code', ch.charge_id) * 5)::int] end as failure_code,
            case when ch.status = 'failed' then 'Your card was declined.' end as failure_message,
            ch.invoice_id,
            case when ch.status = 'failed' then 'declined_by_network' else 'approved_by_network' end as outcome_network_status,
            case when rnd('risk', ch.charge_id) < 0.05 then 'elevated' else 'normal' end as outcome_risk_level,
            floor(rnd('risk', ch.charge_id) * 100)::bigint as outcome_risk_score,
            case when ch.status = 'failed' then 'issuer_declined' else 'authorized' end as outcome_type,
            ch.status = 'succeeded' as paid, ch.payment_intent_id,
            case when ch.card_k is not null then 'pm_' || ch.cust_idx || '_' || ch.card_k end as payment_method_id,
            case when ch.customer_id is not null then 'customer' || ch.cust_idx || '@example.com' end as receipt_email,
            coalesce(rf.amount = ch.amount, false) as refunded, 'DAGBENCH' as statement_descriptor, ch.status,
            case when ch.customer_id is not null then 'Customer ' || ch.cust_idx end as billing_detail_name,
            c.country as billing_detail_address_country,
            case when ch.customer_id is not null then 'customer' || ch.cust_idx || '@example.com' end as billing_detail_email
            from _charge ch left join _refund rf using (charge_id) left join _cust c on c.cust_idx = ch.cust_idx""",
        "payment_intent": """select pi.payment_intent_id as id, pi.amount,
            case when pi.status = 'requires_capture' then pi.amount else 0 end as amount_capturable,
            case when pi.status = 'succeeded' then pi.amount else 0 end as amount_received,
            case when pi.status = 'requires_capture' then 'manual' else 'automatic' end as capture_method,
            'automatic' as confirmation_method, pi.created, pi.currency, pi.customer_id, pi.payment_method_id, pi.status,
            case when pi.status = 'requires_payment_method' then pi.last_charge_id end as last_payment_error_charge_id,
            case when pi.status = 'requires_payment_method' then 'card_declined' end as last_payment_error_code,
            case when pi.status = 'requires_payment_method' then 'card_error' end as last_payment_error_type,
            'DAGBENCH' as statement_descriptor
            from (
                select payment_intent_id, max(amount) as amount, min(created) as created, any_value(currency) as currency,
                    any_value(customer_id) as customer_id, arg_max(charge_id, created) as last_charge_id,
                    any_value(case when card_k is not null then 'pm_' || cust_idx || '_' || card_k end) as payment_method_id,
                    case when bool_or(status = 'succeeded' and captured) then 'succeeded'
                         when bool_or(status = 'succeeded') then 'requires_capture'
                         else 'requires_payment_method' end as status
                from _charge group by 1
            ) pi""",
        "refund": """select refund_id as id, amount, 'txn_' || refund_id as balance_transaction_id, charge_id, payment_intent_id,
            created, currency, reason, 'succeeded' as status, upper(to_hex(hash(refund_id) % 100000000)) as receipt_number
            from _refund""",
        "dispute": """select dispute_id as id, amount,
            case when status not like 'warning%' then 'txn_' || dispute_id end as balance_transaction, charge_id, created, currency,
            created + interval 14 day as evidence_details_due_by, status in ('won', 'lost', 'under_review') as evidence_details_has_evidence,
            status = 'needs_response' and created + interval 14 day < as_of() as evidence_details_past_due,
            (status in ('won', 'lost', 'under_review'))::int as evidence_details_submission_count,
            false as is_charge_refundable, reason, status
            from _dispute""",
        "transfer": """select transfer_id as id, amount, case when reversed then amount else 0 end as amount_reversed,
            'txn_' || transfer_id as balance_transaction_id, created, 'usd' as currency,
            destination, 'py_' || substr(transfer_id, 4) as destination_payment, 'py_' || substr(transfer_id, 4) as destination_payment_id,
            reversed, 'card' as source_type, 'group_' || substr(md5(transfer_id), 1, 16) as transfer_group
            from _transfer""",
        "payout": """select payout_id as id, amount, arrival_date, true as automatic, 'txn_' || payout_id as balance_transaction_id,
            created, 'usd' as currency, 'STRIPE PAYOUT' as description, 'ba_1DagBench' as destination_bank_account_id,
            case when failed then 'txn_' || payout_id || '_fail' end as failure_balance_transaction_id,
            case when failed then 'account_closed' end as failure_code,
            case when failed then 'The bank account has been closed.' end as failure_message,
            'standard' as method, 'card' as source_type, 'DAGBENCH' as statement_descriptor,
            case when failed then 'failed' when arrival_date > as_of() then 'in_transit' else 'paid' end as status,
            'bank_account' as type
            from _payout""",
        "payout_balance_transaction": """select p.payout_id, b.balance_transaction_id
            from _bt2 b join _payout p on p.d = bizday(b.available_on)""",
        "balance_transaction": """select balance_transaction_id as id, amount, available_on, created, 'usd' as currency, description,
            case when src_currency <> 'usd' then fx(src_currency) end as exchange_rate, fee, net, reporting_category, source,
            case when available_on <= as_of() then 'available' else 'pending' end as status, type
            from _bt2
            union all
            select 'txn_' || payout_id, -amount, created, created, 'usd', 'STRIPE PAYOUT', null, 0, -amount, 'payout', payout_id,
                'available', 'payout'
            from _payout
            union all
            select 'txn_' || payout_id || '_fail', amount, arrival_date, arrival_date, 'usd', 'Payout failure', null, 0, amount,
                'payout_reversal', payout_id, case when arrival_date <= as_of() then 'available' else 'pending' end, 'payout_failure'
            from _payout where failed""",
        "fee": """select balance_transaction_id, 0 as "index", round(amount * 0.029)::bigint + 30 as amount, 'usd' as currency,
            'Stripe processing fees' as description, 'stripe_fee' as type
            from _bt2 where type = 'charge'
            union all
            select balance_transaction_id, 1, fee - (round(amount * 0.029)::bigint + 30), 'usd', 'Currency conversion fee', 'stripe_fee'
            from _bt2 where type = 'charge' and src_currency <> 'usd'
            union all
            select balance_transaction_id, 0, fee, 'usd', 'Dispute fee', 'stripe_fee'
            from _bt2 where reporting_category = 'dispute'""",
    }


def is_id_column(name: str) -> bool:
    return name == "id" or name.endswith("_id") or name in EXTRA_ID_COLUMNS


def write_table(con: duckdb.DuckDBPyConnection, project_dir: Path, table: str, select: str) -> None:
    columns = connector_columns(project_dir, TABLES[table])
    known = {name.lower() for name, _ in columns}
    con.execute(f"create or replace temp table _out as select *, false as __test from ({select})")
    populated = [row[0] for row in con.execute("describe _out").fetchall() if row[0] != "__test"]
    unknown = {c.lower() for c in populated} - known
    assert not unknown, f"{table}: columns not in get_{TABLES[table]}_columns: {sorted(unknown)}"
    id_cols = [c for c in populated if is_id_column(c.lower())]

    if table in LIVEMODE_TABLES:
        # Test-mode copies: same values, distinct ids (so they never collide with
        # or join to live objects), livemode = false. Staging filters them out.
        assert "id" in populated, table
        exprs = [f"{quote_ident(c)} || '_test'" if c in id_cols else quote_ident(c) for c in populated]
        con.execute(f"insert into _out select {', '.join(exprs)}, true from _out "
                    f"where rnd('test_mode', id) < {TEST_MODE_SHARE}")

    hashes = ", ".join(f"hash({quote_ident(c)}, {j}) as {quote_ident(f'__h{j}_{c}')}"
                       for c in id_cols for j in range(3))
    exprs = []
    for name, dtype in columns:
        col = quote_ident(name)
        if name in id_cols:
            h = [quote_ident(f"__h{j}_{name}") for j in range(3)]
            exprs.append(f"split_part({col}, '_', 1) || '_' || b62({', '.join(h)}) as {col}")
        elif name in populated:
            exprs.append(f"cast({col} as {dtype}) as {col}")
        elif name == "livemode" and table in LIVEMODE_TABLES:
            exprs.append(f"not __test as {col}")
        elif name in COMMON:
            exprs.append(f"cast({COMMON[name]} as {dtype}) as {col}")
        else:
            exprs.append(f"cast(null as {dtype}) as {col}")
    con.execute(f"create table {quote_ident(table)} as select {', '.join(exprs)} "
                f"from (select *{', ' + hashes if hashes else ''} from _out)")
    rows = con.execute(f"select count(*) from {quote_ident(table)}").fetchone()[0]
    print(f"  {table}: {rows:,} rows")


def generate(project_dir: Path, sf: float, output_path: Path) -> None:
    n_cust = max(1, round(CUSTOMERS_PER_SF * sf))
    n_oneoff = max(1, round(n_cust * ONE_OFF_CHARGES_PER_CUSTOMER))
    n_transfer = max(1, round(n_cust * TRANSFERS_PER_CUSTOMER))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)
    con = duckdb.connect(str(output_path))
    try:
        con.execute(MACROS)
        for sql in staging_sql(n_cust, n_oneoff, n_transfer):
            con.execute(sql)
        for table, select in final_sql(project_dir).items():
            write_table(con, project_dir, table, select)
        con.execute("drop table _out")
    finally:
        con.close()


def run_stripe(project_dir: Path) -> None:
    """generate_data.py entry point: `generate_data.py <sf> [--no-constraints]`,
    run from the project directory (the dbgen/project_runner.py contract).
    --no-constraints is accepted and ignored: the sources have no keys."""
    sf = 1.0
    if len(sys.argv) > 1:
        try:
            sf = float(sys.argv[1])
        except ValueError:
            pass
    if sf <= 0:
        raise SystemExit(f"Scale factor must be positive, got {sf}")

    cache_path = CACHE_DIR / f"p12_sf{str(sf).replace('.', '_')}.duckdb"
    output_path = project_dir / OUTPUT_PATH

    if output_path.exists():
        print(f"Deleting existing duckdb file: {output_path}")
        output_path.unlink()

    if cache_path.exists():
        print(f"Using cached duckdb file: {cache_path}")
    else:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_suffix(".duckdb.partial")
        print(f"Generating Stripe source data at sf={sf}")
        generate(project_dir, sf, tmp_path)
        tmp_path.rename(cache_path)
        print(f"Cached generated duckdb file to: {cache_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cache_path, output_path)
