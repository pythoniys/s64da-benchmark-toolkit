BEGIN;
CREATE TABLE IF NOT EXISTS warehouse (
  w_id smallint PRIMARY KEY,
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
  i_id int PRIMARY KEY,
  i_im_id int,
  i_name varchar(24),
  i_price decimal(5,2),
  i_data varchar(50)
)
  
WITH (global);
DROP FUNCTION IF EXISTS delivery(INT, INT, INT, TIMESTAMPTZ);
CREATE FUNCTION delivery(
    in_w_id INT
  , in_o_carrier_id INT
  , in_dist_per_ware INT
  , in_timestamp TIMESTAMPTZ
) RETURNS VOID AS $$
DECLARE
  d_id INT;
  this_no_o_id INT;
  this_o_c_id INT;
  ol_total NUMERIC(12,2);
BEGIN
  FOR d_id IN 1..in_dist_per_ware LOOP
    SELECT COALESCE(MIN(no_o_id), 0)
    FROM new_orders
    WHERE no_d_id = d_id
      AND no_w_id = in_w_id
    INTO this_no_o_id;

    DELETE FROM new_orders
    WHERE no_o_id = this_no_o_id
      AND no_d_id = d_id
      AND no_w_id = in_w_id;

    SELECT o_c_id
    FROM orders
    WHERE o_id = this_no_o_id
      AND o_d_id = d_id
      AND o_w_id = in_w_id
    INTO this_o_c_id;

    UPDATE orders
    SET o_carrier_id = in_o_carrier_id
    WHERE o_id = this_no_o_id
      AND o_d_id = d_id
      AND o_w_id = in_w_id;

    UPDATE order_line
    SET ol_delivery_d = in_timestamp
    WHERE ol_o_id = this_no_o_id
      AND ol_d_id = d_id
      AND ol_w_id = in_w_id;

    SELECT SUM(ol_amount)
    FROM order_line
    WHERE ol_o_id = this_no_o_id
      AND ol_d_id = d_id
      AND ol_w_id = in_w_id
    INTO ol_total;

    UPDATE customer
    SET c_balance = c_balance + ol_total
      , c_delivery_cnt = c_delivery_cnt + 1
    WHERE c_id = this_o_c_id
      AND c_d_id = d_id
      AND c_w_id = in_w_id;
  END LOOP;
END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS new_order(INT, INT, INT, INT, INT, INT[], INT[], INT[], TIMESTAMPTZ);
CREATE FUNCTION new_order(
    in_w_id INT
  , in_c_id INT
  , in_d_id INT
  , in_ol_cnt INT
  , in_all_local INT
  , in_itemids INT[]
  , in_supware INT[]
  , in_qty INT[]
  , in_timestamp TIMESTAMPTZ
) RETURNS BOOLEAN
AS $$
DECLARE
  a RECORD;
  b RECORD;
  item_record RECORD;
  stock_record RECORD;

  return_value RECORD;

  ol_number INT;
  ol_supply_w_id INT;
  ol_i_id INT;
  ol_quantity INT;

  ol_amount NUMERIC(6,2);
BEGIN
  SELECT
      c_discount
    , c_last
    , c_credit
    , w_tax
  INTO a
  FROM customer, warehouse
  WHERE w_id = in_w_id
    AND c_w_id = w_id
    AND c_d_id = in_d_id
    AND c_id = in_c_id;

  UPDATE district
  SET d_next_o_id = d_next_o_id + 1
  WHERE d_id = in_d_id
    AND d_w_id = in_w_id
  RETURNING d_next_o_id, d_tax INTO b;

  INSERT INTO orders(
      o_id
    , o_d_id
    , o_w_id
    , o_c_id
    , o_entry_d
    , o_ol_cnt
    , o_all_local
  ) VALUES (
      b.d_next_o_id
    , in_d_id
    , in_w_id
    , in_c_id
    , in_timestamp
    , in_ol_cnt
    , in_all_local
  );

  INSERT INTO new_orders(no_o_id, no_d_id, no_w_id)
  VALUES (b.d_next_o_id, in_d_id, in_w_id);

  FOR ol_number IN 1 .. in_ol_cnt LOOP
    ol_i_id = in_itemids[ol_number];

    SELECT i_price, i_name, i_data
    INTO item_record
    FROM item
    WHERE i_id = ol_i_id;

    IF item_record IS NULL THEN
      RETURN FALSE;
    END IF;

    ol_supply_w_id = in_supware[ol_number];
    ol_quantity = in_qty[ol_number];

    UPDATE stock
    SET s_quantity = CASE
          WHEN s_quantity > ol_quantity THEN s_quantity - ol_quantity
          ELSE s_quantity - ol_quantity + 91
        END
      , s_order_cnt = s_order_cnt + 1
      , s_remote_cnt = CASE
                         WHEN ol_supply_w_id <> in_w_id THEN s_remote_cnt + 1
                         ELSE s_remote_cnt
                       END
    WHERE s_i_id = ol_i_id
      AND s_w_id = ol_supply_w_id
    RETURNING
        s_data, s_quantity,
        CASE
          WHEN in_d_id = 1 THEN s_dist_01
          WHEN in_d_id = 2 THEN s_dist_02
          WHEN in_d_id = 3 THEN s_dist_03
          WHEN in_d_id = 4 THEN s_dist_04
          WHEN in_d_id = 5 THEN s_dist_05
          WHEN in_d_id = 6 THEN s_dist_06
          WHEN in_d_id = 7 THEN s_dist_07
          WHEN in_d_id = 8 THEN s_dist_08
          WHEN in_d_id = 9 THEN s_dist_09
          WHEN in_d_id = 10 THEN s_dist_10
        END AS ol_dist_info
    INTO stock_record;

    ol_amount = ol_quantity * item_record.i_price * (1 + a.w_tax + b.d_tax) * (1 - a.c_discount);

    INSERT INTO order_line(
        ol_o_id
      , ol_d_id
      , ol_w_id
      , ol_number
      , ol_i_id
      , ol_supply_w_id
      , ol_quantity
      , ol_amount
      , ol_dist_info
    ) VALUES (
        b.d_next_o_id
      , in_d_id
      , in_w_id
      , ol_number
      , ol_i_id
      , ol_supply_w_id
      , ol_quantity
      , ol_amount
      , stock_record.ol_dist_info
    );

  END LOOP;

  RETURN TRUE;
END
$$ LANGUAGE PLPGSQL PARALLEL SAFE;

DROP FUNCTION IF EXISTS order_status(INT, INT, INT, VARCHAR, BOOL);
CREATE FUNCTION order_status(
    in_c_w_id INT
  , in_c_d_id INT
  , in_c_id INT
  , in_c_last VARCHAR(24)
  , in_byname BOOL
) RETURNS VOID AS $$
DECLARE
  namecnt BIGINT;
  customer_rec RECORD;
  order_rec RECORD;
  order_line_rec RECORD;
BEGIN
  IF in_byname THEN
    SELECT count(c_id)
    FROM customer
    WHERE c_w_id = in_c_w_id
      AND c_d_id = in_c_d_id
      AND c_last = in_c_last
    INTO namecnt;

    IF namecnt % 2 = 1 THEN
      namecnt = namecnt + 1;
    END IF;

    SELECT c_balance, c_first, c_middle, c_last, c_id
    FROM customer
    WHERE c_w_id = in_c_w_id
      AND c_d_id = in_c_d_id
      AND c_last = in_c_last
    ORDER BY c_first
    OFFSET namecnt / 2
    LIMIT 1
    INTO customer_rec;
  ELSE
    SELECT c_balance, c_first, c_middle, c_last, c_id
    FROM customer
    WHERE c_w_id = in_c_w_id
      AND c_d_id = in_c_d_id
      AND c_id = in_c_id
    INTO customer_rec;
  END IF;

  SELECT o_id, o_entry_d, o_carrier_id
  FROM orders
  WHERE o_w_id = in_c_w_id
    AND o_d_id = in_c_d_id
    AND o_c_id = in_c_id
    AND o_id = (
      SELECT max(o_id)
      FROM orders
      WHERE o_w_id = in_c_w_id
        AND o_d_id = in_c_d_id
        AND o_c_id = in_c_id
    )
  INTO order_rec;
END
$$ LANGUAGE PLPGSQL PARALLEL SAFE;

DROP FUNCTION IF EXISTS payment(INT, INT, INT, INT, INT, NUMERIC(12,2), BOOL, CHARACTER VARYING(16), TIMESTAMPTZ);
CREATE FUNCTION payment(
    in_w_id INT
  , in_d_id INT
  , in_c_d_id INT
  , in_c_id INT
  , in_c_w_id INT
  , in_h_amount NUMERIC(12,2)
  , in_byname BOOL
  , in_c_last CHARACTER VARYING(16)
  , in_timestamp TIMESTAMPTZ
) RETURNS VOID AS $$
DECLARE
  w_record RECORD;
  d_record RECORD;
  namecount BIGINT;
BEGIN

  UPDATE warehouse
  SET w_ytd = w_ytd + in_h_amount
  WHERE w_id = in_w_id;

  SELECT
      w_street_1
    , w_street_2
    , w_city
    , w_state
    , w_zip
    , w_name
  INTO w_record
  FROM warehouse
  WHERE w_id = in_w_id;

  UPDATE district
  SET d_ytd = d_ytd + in_h_amount
  WHERE d_w_id = in_w_id
    AND d_id = in_d_id;

  SELECT
      d_street_1
    , d_street_2
    , d_city
    , d_state
    , d_zip
    , d_name
  INTO d_record
  FROM district
  WHERE d_w_id = in_w_id
    AND d_id = in_d_id;

  IF in_byname = true THEN
    SELECT count(c_id)
    FROM customer
    INTO namecount
    WHERE c_w_id = in_c_w_id
      AND c_d_id = in_c_d_id
      AND c_last = in_c_last;

    IF namecount % 2 = 0 THEN
      namecount = namecount + 1;
    END IF;

    SELECT c_id
    INTO in_c_id
    FROM customer
    WHERE c_w_id = in_c_w_id
      AND c_d_id = in_c_d_id
      AND c_last = in_c_last
    ORDER BY c_first
    OFFSET namecount / 2
    LIMIT 1;
  END IF;

  UPDATE customer
  SET c_balance = c_balance - in_h_amount
    , c_ytd_payment = c_ytd_payment + in_h_amount
    , c_data =
      CASE
        WHEN c_credit = 'BC' THEN
          substr(
            format('| %4s %2s %4s %2s %4s $%s %12s %24s',
                c_id
              , c_d_id
              , c_w_id
              , in_d_id
              , in_w_id
              , to_char(in_h_amount, '9999999.99')
              , extract(epoch from in_timestamp)
              , c_data
            ), 1, 500
          )
        ELSE c_data
      END
  WHERE c_w_id = in_c_w_id
    AND c_d_id = in_c_d_id
    AND c_id = in_c_id;

  INSERT INTO history(
      h_c_d_id
    , h_c_w_id
    , h_c_id
    , h_d_id
    , h_w_id
    , h_date
    , h_amount
    , h_data
  ) VALUES(
      in_c_d_id
    , in_c_w_id
    , in_c_id
    , in_d_id
    , in_w_id
    , in_timestamp
    , in_h_amount
    , format('%10s %10s    ', w_record.w_name, d_record.d_name)
  );
END
$$
LANGUAGE plpgsql PARALLEL SAFE;

DROP FUNCTION IF EXISTS stock_level(INT, INT, INT);
CREATE FUNCTION stock_level(
    in_w_id INT
  , in_d_id INT
  , in_threshold INT
) RETURNS INT AS $$
DECLARE
  this_d_next_o_id INT;
  low_stock_count INT;
BEGIN
  SELECT d_next_o_id
  FROM district
  WHERE d_id = in_d_id
    AND d_w_id = in_w_id
  INTO this_d_next_o_id;

  SELECT COUNT(DISTINCT(s_i_id))
  FROM order_line, stock
  WHERE ol_w_id = in_w_id
    AND ol_d_id = in_d_id
    AND ol_o_id <  this_d_next_o_id
    AND ol_o_id >= this_d_next_o_id
    AND s_w_id = in_w_id
    AND s_i_id = ol_i_id
    AND s_quantity < in_threshold
  INTO low_stock_count;

  RETURN low_stock_count;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

END;

BEGIN;
CREATE TABLE IF NOT EXISTS region (
  r_regionkey int PRIMARY KEY,
  r_name varchar(25) NOT NULL,
  r_comment varchar(152) NOT NULL
)
WITH(global);
END;

BEGIN;
CREATE TABLE IF NOT EXISTS nation (
  n_nationkey int PRIMARY KEY,
  n_name varchar(25) NOT NULL,
  n_regionkey int NOT NULL,
  n_comment varchar(152) NOT NULL
)
WITH(global);
END;

BEGIN;
CREATE TABLE IF NOT EXISTS supplier (
  su_suppkey int PRIMARY KEY,
  su_name varchar(25) NOT NULL,
  su_address varchar(40) NOT NULL,
  su_nationkey int NOT NULL,
  su_phone varchar(15) NOT NULL,
  su_acctbal double precision NOT NULL,
  su_comment varchar(101) NOT NULL
)
WITH(global);
END;
