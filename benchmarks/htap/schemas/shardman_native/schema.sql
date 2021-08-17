BEGIN;
CREATE TABLE IF NOT EXISTS warehouse (
  w_id smallint NOT NULL,
  w_name varchar(10),
  w_street_1 varchar(20),
  w_street_2 varchar(20),
  w_city varchar(20),
  w_state char(2),
  w_zip char(9),
  w_tax decimal(4,2),
  w_ytd decimal(12,2)
) WITH (distributed_by = 'w_id');

CREATE TABLE IF NOT EXISTS district (
  d_id smallint NOT NULL,
  d_w_id smallint NOT NULL,
  d_name varchar(10),
  d_street_1 varchar(20),
  d_street_2 varchar(20),
  d_city varchar(20),
  d_state char(2),
  d_zip char(9),
  d_tax decimal(4,2),
  d_ytd decimal(12,2),
  d_next_o_id int
)  WITH (distributed_by = 'd_w_id', colocate_with = 'warehouse');


CREATE TABLE IF NOT EXISTS customer (
  c_id int NOT NULL,
  c_d_id smallint NOT NULL,
  c_w_id smallint NOT NULL,
  c_nationkey int NOT NULL,
  c_first varchar(16),
  c_middle char(2),
  c_last varchar(16),
  c_street_1 varchar(20),
  c_street_2 varchar(20),
  c_city varchar(20),
  c_state char(2),
  c_zip char(9),
  c_phone char(16),
  c_since timestamp,
  c_credit char(2),
  c_credit_lim bigint,
  c_discount decimal(4,2),
  c_balance decimal(12,2),
  c_ytd_payment decimal(12,2),
  c_payment_cnt smallint,
  c_delivery_cnt smallint,
  c_data text
) WITH (distributed_by = 'c_w_id', colocate_with = 'warehouse');

CREATE TABLE IF NOT EXISTS history (
  id BIGINT,
  h_c_id int,
  h_c_d_id smallint,
  h_c_w_id smallint,
  h_d_id smallint,
  h_w_id smallint,
  h_date timestamp,
  h_amount decimal(6,2),
  h_data varchar(24)
)  WITH (distributed_by = 'h_w_id', colocate_with = 'warehouse');

CREATE TABLE IF NOT EXISTS orders (
  o_id bigint NOT NULL,
  o_d_id smallint NOT NULL,
  o_w_id smallint NOT NULL,
  o_c_id int,
  o_entry_d timestamp,
  o_carrier_id smallint,
  o_ol_cnt smallint,
  o_all_local smallint
) WITH (distributed_by = 'o_w_id', colocate_with = 'warehouse');

CREATE TABLE IF NOT EXISTS new_orders (
  no_o_id bigint NOT NULL,
  no_d_id smallint NOT NULL,
  no_w_id smallint NOT NULL
)  WITH (distributed_by = 'no_w_id', colocate_with = 'warehouse');

CREATE TABLE IF NOT EXISTS order_line (
  ol_o_id bigint NOT NULL,
  ol_d_id smallint NOT NULL,
  ol_w_id smallint NOT NULL,
  ol_number smallint NOT NULL,
  ol_i_id int,
  ol_supply_w_id smallint,
  ol_delivery_d timestamp,
  ol_quantity smallint,
  ol_amount decimal(6,2),
  ol_dist_info char(24)
) WITH (distributed_by = 'ol_w_id', colocate_with = 'warehouse');

CREATE TABLE IF NOT EXISTS stock (
  s_i_id int NOT NULL,
  s_w_id smallint NOT NULL,
  s_quantity smallint,
  s_dist_01 char(24),
  s_dist_02 char(24),
  s_dist_03 char(24),
  s_dist_04 char(24),
  s_dist_05 char(24),
  s_dist_06 char(24),
  s_dist_07 char(24),
  s_dist_08 char(24),
  s_dist_09 char(24),
  s_dist_10 char(24),
  s_ytd decimal(8,0),
  s_order_cnt smallint,
  s_remote_cnt smallint,
  s_data varchar(50)
)  WITH (distributed_by = 's_w_id', colocate_with = 'warehouse');

CREATE TABLE IF NOT EXISTS item (
  i_id int NOT NULL,
  i_im_id int,
  i_name varchar(24),
  i_price decimal(5,2),
  i_data varchar(50)
);
END;

BEGIN;
ALTER TABLE item ADD CONSTRAINT item_pk PRIMARY KEY (i_id);
SELECT shardman.make_table_global('item');
END;

BEGIN;
CREATE TABLE IF NOT EXISTS region (
  r_regionkey int NOT NULL,
  r_name varchar(25) NOT NULL,
  r_comment varchar(152) NOT NULL
);
END;

BEGIN;
ALTER TABLE region ADD CONSTRAINT region_pk PRIMARY KEY (r_regionkey);
SELECT shardman.make_table_global('region');
END;

BEGIN;
CREATE TABLE IF NOT EXISTS nation (
  n_nationkey int NOT NULL,
  n_name varchar(25) NOT NULL,
  n_regionkey int NOT NULL,
  n_comment varchar(152) NOT NULL
);
END;

BEGIN;
ALTER TABLE nation ADD CONSTRAINT nation_pk PRIMARY KEY (n_nationkey);
SELECT shardman.make_table_global('nation');
END;

BEGIN;
CREATE TABLE IF NOT EXISTS supplier (
  su_suppkey int NOT NULL,
  su_name varchar(25) NOT NULL,
  su_address varchar(40) NOT NULL,
  su_nationkey int NOT NULL,
  su_phone varchar(15) NOT NULL,
  su_acctbal double precision NOT NULL,
  su_comment varchar(101) NOT NULL
);
END;

BEGIN;
ALTER TABLE supplier ADD CONSTRAINT supplier_pk PRIMARY KEY (su_suppkey);
SELECT shardman.make_table_global('supplier');
END;



