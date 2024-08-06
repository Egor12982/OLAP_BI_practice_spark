create table if not exists tmp.writeoffs_by_customers_qty
(
    office_id         Int64, 
    state_id          LowCardinality(String),
    state_descr       String,
    price_writeoff    Int64
)
engine MergeTree()
order by (office_id, state_id, price_writeoff)
SETTINGS index_granularity = 8192;


create table if not exists tmp.state
(
    state_id     LowCardinality(String),
    state_descr  String
)
engine = MergeTree()
order by state_id
SETTINGS index_granularity = 8192;
