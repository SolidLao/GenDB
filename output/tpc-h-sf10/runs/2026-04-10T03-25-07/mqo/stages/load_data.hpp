#pragma once
#include "common.hpp"

static inline void load_data(Ctx& c, [[maybe_unused]] uint32_t active) {
    MQO_TIME_PHASE("load_data");

    std::string sl = c.sd + "/lineitem", so = c.sd + "/orders";
    std::string sc = c.sd + "/customer", sp = c.sd + "/part";
    std::string sps = c.sd + "/partsupp", ss = c.sd + "/supplier";
    std::string sn = c.sd + "/nation",   sr = c.sd + "/region";

    c.nli   = read_rowcount(sl);  c.nord  = read_rowcount(so);
    c.ncust = read_rowcount(sc);  c.npart = read_rowcount(sp);
    c.nps   = read_rowcount(sps); c.nsupp = read_rowcount(ss);

    // Dictionaries
    c.d_ptype    = read_dict(sp  + "/p_type_dict.bin");
    c.d_pbrand   = read_dict(sp  + "/p_brand_dict.bin");
    c.d_pcont    = read_dict(sp  + "/p_container_dict.bin");
    c.d_pmfgr    = read_dict(sp  + "/p_mfgr_dict.bin");
    c.d_shipmode = read_dict(sl  + "/l_shipmode_dict.bin");
    c.d_shipinst = read_dict(sl  + "/l_shipinstruct_dict.bin");
    c.d_ordpri   = read_dict(so  + "/o_orderpriority_dict.bin");
    c.d_nname    = read_dict(sn  + "/n_name_dict.bin");
    c.d_rname    = read_dict(sr  + "/r_name_dict.bin");
    c.d_mkseg    = read_dict(sc  + "/c_mktsegment_dict.bin");

    // Nation / Region
    c.n_regionkey = c.mc<int32_t>(sn, "n_regionkey");
    c.n_name_col  = c.mc<uint8_t>(sn, "n_name");
    c.r_name_col  = c.mc<uint8_t>(sr, "r_name");

    // Lineitem
    c.l_orderkey     = c.mc<int32_t>(sl, "l_orderkey");
    c.l_partkey      = c.mc<int32_t>(sl, "l_partkey");
    c.l_suppkey      = c.mc<int32_t>(sl, "l_suppkey");
    c.l_shipdate     = c.mc<int32_t>(sl, "l_shipdate");
    c.l_commitdate   = c.mc<int32_t>(sl, "l_commitdate");
    c.l_receiptdate  = c.mc<int32_t>(sl, "l_receiptdate");
    c.l_quantity     = c.mc<double>(sl,  "l_quantity");
    c.l_extendedprice= c.mc<double>(sl,  "l_extendedprice");
    c.l_discount     = c.mc<double>(sl,  "l_discount");
    c.l_tax          = c.mc<double>(sl,  "l_tax");
    c.l_returnflag   = c.mc<int8_t>(sl,  "l_returnflag");
    c.l_linestatus   = c.mc<int8_t>(sl,  "l_linestatus");
    c.l_shipmode_col = c.mc<uint8_t>(sl, "l_shipmode");
    c.l_shipinst_col = c.mc<uint8_t>(sl, "l_shipinstruct");

    // Orders
    c.o_orderkey_col = c.mc<int32_t>(so, "o_orderkey");
    c.o_custkey      = c.mc<int32_t>(so, "o_custkey");
    c.o_orderdate    = c.mc<int32_t>(so, "o_orderdate");
    c.o_shippriority = c.mc<int32_t>(so, "o_shippriority");
    c.o_orderstatus  = c.mc<int8_t>(so,  "o_orderstatus");
    c.o_orderpri     = c.mc<uint8_t>(so, "o_orderpriority");
    c.o_totalprice   = c.mc<double>(so,  "o_totalprice");
    c.o_cmt_off      = c.mco(so, "o_comment");
    c.o_cmt_dat      = c.mcd(so, "o_comment");

    // Customer
    c.c_nationkey = c.mc<int32_t>(sc, "c_nationkey");
    c.c_mktseg    = c.mc<uint8_t>(sc, "c_mktsegment");
    c.c_acctbal   = c.mc<double>(sc,  "c_acctbal");
    c.c_nm_off = c.mco(sc,"c_name");  c.c_nm_dat = c.mcd(sc,"c_name");
    c.c_ph_off = c.mco(sc,"c_phone"); c.c_ph_dat = c.mcd(sc,"c_phone");
    c.c_ad_off = c.mco(sc,"c_address");c.c_ad_dat = c.mcd(sc,"c_address");
    c.c_cm_off = c.mco(sc,"c_comment");c.c_cm_dat = c.mcd(sc,"c_comment");

    // Supplier
    c.s_nationkey = c.mc<int32_t>(ss, "s_nationkey");
    c.s_acctbal   = c.mc<double>(ss,  "s_acctbal");
    c.s_nm_off = c.mco(ss,"s_name");   c.s_nm_dat = c.mcd(ss,"s_name");
    c.s_ad_off = c.mco(ss,"s_address"); c.s_ad_dat = c.mcd(ss,"s_address");
    c.s_ph_off = c.mco(ss,"s_phone");   c.s_ph_dat = c.mcd(ss,"s_phone");
    c.s_cm_off = c.mco(ss,"s_comment"); c.s_cm_dat = c.mcd(ss,"s_comment");

    // Part
    c.p_size      = c.mc<int32_t>(sp, "p_size");
    c.p_type_col  = c.mc<uint8_t>(sp, "p_type");
    c.p_brand_col = c.mc<uint8_t>(sp, "p_brand");
    c.p_cont_col  = c.mc<uint8_t>(sp, "p_container");
    c.p_mfgr_col  = c.mc<uint8_t>(sp, "p_mfgr");
    c.p_nm_off = c.mco(sp,"p_name"); c.p_nm_dat = c.mcd(sp,"p_name");

    // Partsupp
    c.ps_partkey    = c.mc<int32_t>(sps, "ps_partkey");
    c.ps_suppkey    = c.mc<int32_t>(sps, "ps_suppkey");
    c.ps_availqty   = c.mc<int32_t>(sps, "ps_availqty");
    c.ps_supplycost = c.mc<double>(sps,  "ps_supplycost");

    // Indexes
    c.opk_idx    = c.mm(c.id + "/orders_pk_index.bin").as<int32_t>();
    c.li_ok_idx  = c.mm(c.id + "/lineitem_orderkey_idx.bin").as<DRE>();
    c.li_pk_off  = c.mm(c.id + "/lineitem_partkey_grouped_offsets.bin").as<uint32_t>();
    c.li_pk_rows = c.mm(c.id + "/lineitem_partkey_grouped_rows.bin").as<uint32_t>();
    c.li_sk_off  = c.mm(c.id + "/lineitem_suppkey_grouped_offsets.bin").as<uint32_t>();
    c.li_sk_rows = c.mm(c.id + "/lineitem_suppkey_grouped_rows.bin").as<uint32_t>();
    c.ps_pk_idx  = c.mm(c.id + "/partsupp_partkey_idx.bin").as<DRE>();
    c.ps_hash    = c.mm(c.id + "/partsupp_composite_hash.bin").as<PHE>();
    c.ps_hmask   = 16777215u;
    c.ps_sk_off  = c.mm(c.id + "/partsupp_suppkey_grouped_offsets.bin").as<uint32_t>();
    c.ps_sk_rows = c.mm(c.id + "/partsupp_suppkey_grouped_rows.bin").as<uint32_t>();
    c.sup_nk_off = c.mm(c.id + "/supplier_nationkey_grouped_offsets.bin").as<uint32_t>();
    c.sup_nk_rows= c.mm(c.id + "/supplier_nationkey_grouped_rows.bin").as<uint32_t>();
    c.cust_ok_off= c.mm(c.id + "/custkey_to_orders_offsets.bin").as<uint32_t>();
    c.cust_ok_rows=c.mm(c.id + "/custkey_to_orders_rows.bin").as<uint32_t>();

    printf("[MQO] Data loaded: lineitem=%zu orders=%zu customer=%zu part=%zu partsupp=%zu supplier=%zu\n",
           c.nli, c.nord, c.ncust, c.npart, c.nps, c.nsupp);
}
