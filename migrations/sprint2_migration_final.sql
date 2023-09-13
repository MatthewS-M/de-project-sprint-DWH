DROP TABLE IF EXISTS public.shipping;
-- shipping
CREATE TABLE public.shipping(
   ID                               serial,
   shipping_id                      BIGINT,
   sale_id                          BIGINT,
   order_id                         BIGINT,
   client_id                        BIGINT,
   payment_amount                   NUMERIC(14,2),
   state_datetime                   TIMESTAMP,
   product_id                       BIGINT,
   description                      text,
   vendor_id                        BIGINT,
   name_category                    text,
   base_country                     text,
   status                           text,
   state                            text,
   shipping_plan_datetime           TIMESTAMP,
   hours_to_plan_shipping           NUMERIC(14,2),
   shipping_transfer_description    text,
   shipping_transfer_rate           NUMERIC(14,3),
   shipping_country                 text,
   shipping_country_base_rate       NUMERIC(14,3),
   vendor_agreement_description     text,
   PRIMARY KEY (ID)
);
CREATE INDEX shipping_id ON public.shipping(shipping_id);
COMMENT ON COLUMN public.shipping.shipping_id is 'id of shipping of sale';

-- select count(*) from shipping;
drop table if exists public.shipping_country_rates;
create table public.shipping_country_rates(
    id serial,
    shipping_country text,
    shipping_country_base_rate numeric(14,3),
    constraint shipping_country_rates_id_pkey primary key (id)
);
create index shipping_country_rates_country_index on shipping_country_rates(shipping_country);

-- update table public.shipping_country_rates set (shipping)

-- select shipping_country, count(shipping_country_base_rate) from shipping group by rollup (1);

-- select distinct on (shipping_country) shipping_country, shipping_country_base_rate from shipping;

create sequence country_rates_seq start 1;
insert into shipping_country_rates(id, shipping_country, shipping_country_base_rate)
select nextval('country_rates_seq') as id, sc, scbr
from (
select distinct on (shipping_country)
                                      shipping_country as sc,
                                      shipping_country_base_rate as scbr
                                      from shipping) as s;
drop sequence country_rates_seq;

-- truncate table shipping_country_rates;

drop table if exists public.shipping_agreement;
create table public.shipping_agreement(
    agreement_id bigint,
    agreement_number text,
    agreement_rate numeric(14,2),
    agreement_commission numeric(14,2),
    constraint shipping_agreement_id_pkey primary key (agreement_id)
);

-- select distinct (regexp_split_to_array(vendor_agreement_description,E'\\:'))[1] from shipping order by 1;

-- create sequence shipping_agreement_seq start 1;
insert into shipping_agreement(agreement_id, agreement_number, agreement_rate, agreement_commission)
select cast(id as bigint), num, cast(rate as numeric(14,2)), com::numeric(14,2) from (
select s.desc[1] as id, s.desc[2] as num, s.desc[3] as rate, s.desc[4] as com from (
    select distinct regexp_split_to_array(vendor_agreement_description,E'\:') as "desc" from shipping order by 1) as s) as ag;

-- truncate shipping_agreement;

drop table if exists public.shipping_transfer;
create table public.shipping_transfer(
    id serial primary key,
    transfer_type text,
    transfer_model text,
    shipping_transfer_rate numeric(14,3)
);
create index shipping_transfer_type_index on public.shipping_transfer(transfer_type);

-- select distinct shipping_transfer_description, shipping_transfer_rate from shipping;

create sequence shipping_transfer_type_seq start 1;
insert into shipping_transfer(id, transfer_type, transfer_model, shipping_transfer_rate)
select nextval('shipping_transfer_type_seq'), type, model, rate from (
    select s.desc[1] as type, s.desc[2] as model, rate from (
    select distinct regexp_split_to_array(shipping_transfer_description,E'\:') as "desc", shipping_transfer_rate as rate from shipping order by 1)
        as s
            ) as gen;
drop sequence shipping_transfer_type_seq;

drop table if exists public.shipping_info cascade;
create table public.shipping_info(
    shipping_id bigint not null,
    vendor_id bigint not null,
    payment_amount numeric(14,2),
    shipping_plan_datetime timestamp without time zone,
    shipping_transfer_id bigint not null,
    shipping_agreement_id bigint not null,
    shipping_country_rate_id bigint not null,
--     constraint shipping_info_shipping_id_pkey primary key (shipping_id),
    constraint shipping_info_transfer_id_fkey foreign key (shipping_transfer_id) references shipping_transfer(id),
    constraint shipping_info_agreement_id_fkey foreign key (shipping_agreement_id) references shipping_agreement(agreement_id),
    constraint shipping_info_country_rate_id_fkey foreign key (shipping_country_rate_id) references shipping_country_rates(id)
);
create index shipping_info_vendor_id_index on public.shipping_info(shipping_id, vendor_id);

-- select shipping_id from shipping;

insert into shipping_info(shipping_id, vendor_id, payment_amount, shipping_plan_datetime, shipping_transfer_id, shipping_agreement_id, shipping_country_rate_id)
select sub_ship.shipping_id, sub_ship.vendor_id, sub_ship.payment_amount, sub_ship.shipping_plan_datetime, st.id as transfer_id, sub_ship.agreement_id, sc.country_rate_id from (
    select shipping_id, vendor_id, payment_amount, shipping_plan_datetime,
           cast((regexp_split_to_array(shipping_transfer_description,E'\:'))[1] as text) as transfer_type,
           cast((regexp_split_to_array(shipping_transfer_description,E'\:'))[2] as text) as transfer_model,
           (regexp_split_to_array(vendor_agreement_description,E'\:'))[1]::bigint as agreement_id,
           shipping_country
       from shipping) as sub_ship
    inner join (select id, transfer_type, transfer_model from shipping_transfer) as st on sub_ship.transfer_type=st.transfer_type and sub_ship.transfer_model=st.transfer_model
    inner join (select agreement_id from shipping_agreement) as sa on sub_ship.agreement_id=sa.agreement_id
    inner join (select shipping_country, id as country_rate_id from shipping_country_rates) as sc on sub_ship.shipping_country=sc.shipping_country;

truncate table shipping_info;
-- select count(*) from shipping_info;


-- select count(*)*4 from shipping;

drop table if exists public.shipping_status;
create table public.shipping_status(
    shipping_id bigint,
    status text,
    state text,
    shipping_start_fact_datetime timestamp without time zone,
    shipping_end_fact_datetime timestamp without time zone
);


-- with final_state as (
--     select shipping_id, status, state, max(case when state = 'recieved' then state_datetime else null end) as last_time from shipping group by 1,2,3
-- )
-- first_state as (
--     select shipping_id, max(case when state = 'booked' then state_datetime else null end) as first_time from shipping group by 1
-- )

insert into shipping_status(shipping_id, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
with final_state as (
    select shipping_id, status, state, last_time from (
        select shipping_id,
               last_value(status) over (
                   partition by shipping_id
                   order by state_datetime
                   range between unbounded preceding and unbounded following) as status,
               last_value(state) over (
                    partition by shipping_id
                    order by state_datetime
                    range between unbounded preceding and unbounded following) as state,
               max(case when state = 'recieved' then state_datetime else null end) as last_time
        from shipping group by 1, status, state, state_datetime) as foo where last_time is not null
),
first_state as (
    select shipping_id,
           max(case when state = 'booked' then state_datetime else null end) as first_time
    from shipping group by 1)
select fins.shipping_id, fins.status, fins.state, first_time, fins.last_time from first_state firs left join final_state fins using(shipping_id);

-- where (cast(s.state_datetime as text)=cast(fins.last_time as text) or last_time is null) an;
--
-- select count(*) from (select shipping_id, max(case when state = 'recieved' then state_datetime else null end) as first_time from shipping group by 1) as b;
select count(*) from shipping_status
-- truncate shipping_status;

create or replace view shipping_datamart as
select distinct shipping_id, vendor_id, transfer_type,
       DATE_PART('day', shipping_end_fact_datetime - shipping_start_fact_datetime) as full_day_at_shipping, -- extract(day from shipping_end_fact_datetime - shipping_start_fact_datetime)
       case when coalesce(shipping_end_fact_datetime,null) > shipping_plan_datetime then 1 else 0 end as is_delay,
       case when status = 'finished' then 1 else 0 end as is_shipping_finish,
       case when shipping_end_fact_datetime > shipping_plan_datetime then DATE_PART('day', shipping_end_fact_datetime - shipping_plan_datetime) else 0 end as delay_day_at_shipping,
       payment_amount,
       payment_amount * (shipping_country_base_rate+agreement_rate+shipping_transfer_rate) as vat,
       payment_amount * agreement_commission as profit
from shipping_status ss
    full outer join shipping_info si using (shipping_id)
    join shipping_transfer st on si.shipping_transfer_id = st.id
    join shipping_agreement sa on si.shipping_agreement_id = sa.agreement_id
    join shipping_country_rates scr on si.shipping_country_rate_id = scr.id;

-- select count(*) from shipping_datamart;