mod common;
use common::*;
use rayon::prelude::*;
use std::io::Write;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let gd = &args[1];
    let rd = &args[2];

    prewarm_rayon(0);

    {
    let _total = PhaseTimer::new("total");

    let m_lok = MmapCol::open(&format!("{}/lineitem/l_orderkey.bin", gd));
    let m_lqty = MmapCol::open(&format!("{}/lineitem/l_quantity.bin", gd));

    let l_orderkey = m_lok.as_slice::<i32>();
    let l_quantity = m_lqty.as_slice::<f64>();
    let li_nrows = l_orderkey.len();

    // Phase 1: find orderkeys with sum(l_quantity) > 300
    let qualifying: Vec<(i32, f64)>;
    {
        let _t = PhaseTimer::new("main_scan");

        let n_threads = rayon::current_num_threads();
        let chunk = li_nrows / n_threads;

        let thread_results: Vec<Vec<(i32, f64)>> = (0..n_threads).into_par_iter().map(|tid| {
            let mut start = tid * chunk;
            let mut end = if tid == n_threads - 1 { li_nrows } else { start + chunk };

            // Align to orderkey boundaries
            if tid > 0 && start < li_nrows {
                let prev = l_orderkey[start - 1];
                while start < end && l_orderkey[start] == prev { start += 1; }
            }
            if tid < n_threads - 1 && end < li_nrows {
                let bnd = l_orderkey[end - 1];
                while end < li_nrows && l_orderkey[end] == bnd { end += 1; }
            }

            let mut local = Vec::new();
            let mut i = start;
            while i < end {
                let ok = unsafe { *l_orderkey.get_unchecked(i) };
                let mut sq = 0.0f64;
                while i < end && unsafe { *l_orderkey.get_unchecked(i) } == ok {
                    sq += unsafe { *l_quantity.get_unchecked(i) };
                    i += 1;
                }
                if sq > 300.0 {
                    local.push((ok, sq));
                }
            }
            local
        }).collect();

        let mut merged = Vec::new();
        for v in thread_results { merged.extend(v); }
        qualifying = merged;
    }

    // Phase 2: enrich with orders info
    let m_olookup = MmapCol::open(&format!("{}/indexes/orders_orderkey_lookup.bin", gd));
    let m_ocust = MmapCol::open(&format!("{}/orders/o_custkey.bin", gd));
    let m_odate = MmapCol::open(&format!("{}/orders/o_orderdate.bin", gd));
    let m_oprice = MmapCol::open(&format!("{}/orders/o_totalprice.bin", gd));

    let ol_data = m_olookup.mmap.as_ref();
    let orders_max_key = u32::from_ne_bytes(ol_data[0..4].try_into().unwrap());
    let orders_lookup: &[i32] = unsafe {
        std::slice::from_raw_parts(ol_data[4..].as_ptr() as *const i32, (ol_data.len() - 4) / 4)
    };
    let o_custkey = m_ocust.as_slice::<i32>();
    let o_orderdate = m_odate.as_slice::<i32>();
    let o_totalprice = m_oprice.as_slice::<f64>();

    struct QualOrder { orderkey: i32, sum_qty: f64, totalprice: f64, orderdate: i32, custkey: i32 }
    unsafe impl Send for QualOrder {}
    unsafe impl Sync for QualOrder {}

    let mut results: Vec<QualOrder>;
    {
        let _t = PhaseTimer::new("build_joins");
        results = qualifying.par_iter().filter_map(|&(ok, sq)| {
            if (ok as u32) > orders_max_key { return None; }
            let orow = orders_lookup[ok as usize];
            if orow < 0 { return None; }
            let orow = orow as usize;
            Some(QualOrder {
                orderkey: ok, sum_qty: sq,
                totalprice: o_totalprice[orow],
                orderdate: o_orderdate[orow],
                custkey: o_custkey[orow],
            })
        }).collect();
    }

    {
        let _t = PhaseTimer::new("sort_and_limit");
        results.sort_by(|a, b| b.totalprice.partial_cmp(&a.totalprice).unwrap()
            .then(a.orderdate.cmp(&b.orderdate)));
        results.truncate(100);
    }

    {
        let _t = PhaseTimer::new("output");
        let m_clookup = MmapCol::open(&format!("{}/indexes/customer_custkey_lookup.bin", gd));
        let m_cnoff = MmapCol::open(&format!("{}/customer/c_name_offsets.bin", gd));
        let m_cndat = MmapCol::open(&format!("{}/customer/c_name_data.bin", gd));

        let cl_data = m_clookup.mmap.as_ref();
        let cust_max_key = u32::from_ne_bytes(cl_data[0..4].try_into().unwrap());
        let cust_lookup: &[i32] = unsafe {
            std::slice::from_raw_parts(cl_data[4..].as_ptr() as *const i32, (cl_data.len() - 4) / 4)
        };
        let c_name_offsets = m_cnoff.as_slice::<i64>();
        let c_name_data = m_cndat.mmap.as_ref();

        let path = format!("{}/Q18.csv", rd);
        let mut f = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
        writeln!(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty").unwrap();

        for r in &results {
            let name = if (r.custkey as u32) <= cust_max_key {
                let crow = cust_lookup[r.custkey as usize];
                if crow >= 0 {
                    let s = c_name_offsets[crow as usize] as usize;
                    let e = c_name_offsets[crow as usize + 1] as usize;
                    std::str::from_utf8(&c_name_data[s..e]).unwrap_or("")
                } else { "" }
            } else { "" };

            writeln!(f, "{},{},{},{},{:.2},{:.2}",
                name, r.custkey, r.orderkey,
                epoch_days_to_date_str(r.orderdate),
                r.totalprice, r.sum_qty).unwrap();
        }
    }
    } // end total scope
    std::process::exit(0);
}
