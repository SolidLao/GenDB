-- TPC-H Schema (simplified for prototype testing)

CREATE TABLE nation (
    n_nationkey  INTEGER PRIMARY KEY,
    n_name       CHAR(25) NOT NULL,
    n_regionkey  INTEGER NOT NULL,
    n_comment    VARCHAR(152)
);

CREATE TABLE region (
    r_regionkey  INTEGER PRIMARY KEY,
    r_name       CHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
);

CREATE TABLE supplier (
    s_suppkey    INTEGER PRIMARY KEY,
    s_name       CHAR(25) NOT NULL,
    s_address    VARCHAR(40) NOT NULL,
    s_nationkey  INTEGER NOT NULL REFERENCES nation(n_nationkey),
    s_phone      CHAR(15) NOT NULL,
    s_acctbal    DECIMAL(15,2) NOT NULL,
    s_comment    VARCHAR(101) NOT NULL
);

CREATE TABLE part (
    p_partkey     INTEGER PRIMARY KEY,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        CHAR(25) NOT NULL,
    p_brand       CHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        INTEGER NOT NULL,
    p_container   CHAR(10) NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment     VARCHAR(23) NOT NULL
);

CREATE TABLE partsupp (
    ps_partkey    INTEGER NOT NULL REFERENCES part(p_partkey),
    ps_suppkey    INTEGER NOT NULL REFERENCES supplier(s_suppkey),
    ps_availqty   INTEGER NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment    VARCHAR(199) NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE customer (
    c_custkey    INTEGER PRIMARY KEY,
    c_name       VARCHAR(25) NOT NULL,
    c_address    VARCHAR(40) NOT NULL,
    c_nationkey  INTEGER NOT NULL REFERENCES nation(n_nationkey),
    c_phone      CHAR(15) NOT NULL,
    c_acctbal    DECIMAL(15,2) NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment    VARCHAR(117) NOT NULL
);

CREATE TABLE orders (
    o_orderkey      INTEGER PRIMARY KEY,
    o_custkey       INTEGER NOT NULL REFERENCES customer(c_custkey),
    o_orderstatus   CHAR(1) NOT NULL,
    o_totalprice    DECIMAL(15,2) NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk         CHAR(15) NOT NULL,
    o_shippriority  INTEGER NOT NULL,
    o_comment       VARCHAR(79) NOT NULL
);

CREATE TABLE lineitem (
    l_orderkey      INTEGER NOT NULL REFERENCES orders(o_orderkey),
    l_partkey       INTEGER NOT NULL,
    l_suppkey       INTEGER NOT NULL,
    l_linenumber    INTEGER NOT NULL,
    l_quantity      DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount      DECIMAL(15,2) NOT NULL,
    l_tax           DECIMAL(15,2) NOT NULL,
    l_returnflag    CHAR(1) NOT NULL,
    l_linestatus    CHAR(1) NOT NULL,
    l_shipdate      DATE NOT NULL,
    l_commitdate    DATE NOT NULL,
    l_receiptdate   DATE NOT NULL,
    l_shipinstruct  CHAR(25) NOT NULL,
    l_shipmode      CHAR(10) NOT NULL,
    l_comment       VARCHAR(44) NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber),
    FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey)
);
