mod common;
use common::*;
use rayon::prelude::*;
use std::io::Write;

const DATE_CUT: i32 = 9204; // 1995-03-15
const TOPK: usize = 10;

#[repr(C)]
struct LiIdx { start: u32, count: u32 }

#[derive(Clone, Copy)]
struct ResultRow {
    revenue: f64,
    orderkey: i32,
    orderdate: i32,
    order_row_idx: i32,
}

fn heap_insert(heap: &mut Vec<ResultRow>, r: ResultRow) {
    if heap.len() < TOPK {
        heap.push(r);
        return;
    }
    // Find the worst element
    let mut worst_idx = 0;
    for i in 1..heap.len() {
        if heap[i].revenue < heap[worst_idx].revenue
            || (heap[i].revenue == heap[worst_idx].revenue && heap[i].orderdate > heap[worst_idx].orderdate)
        {
            worst_idx = i;
        }
    }
    if r.revenue > heap[worst_idx].revenue
        || (r.revenue == heap[worst_idx].revenue && r.orderdate < heap[worst_idx].orderdate)
    {
        heap[worst_idx] = r;
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let gdir = &args[1];
    let rdir = &args[2];

    prewarm_rayon(0);

    {
    let _total = PhaseTimer::new("total");

    // Load dict
    let building_code: u8;
    {
        let _t = PhaseTimer::new("dim_filter");
        let content = std::fs::read_to_string(format!("{}/customer/c_mktsegment_dict.bin", gdir)).unwrap();
        building_code = content.lines()
            .filter_map(|line| {
                let parts: Vec<&str> = line.splitn(2, '|').collect();
                if parts.len() == 2 && parts[1] == "BUILDING" {
                    parts[0].parse::<u8>().ok()
                } else { None }
            })
            .next().unwrap();
    }

    let c_mktseg = MmapCol::open(&format!("{}/customer/c_mktsegment.bin", gdir));
    let cust_lookup_mm = MmapCol::open(&format!("{}/indexes/customer_custkey_lookup.bin", gdir));
    let o_orderdate_mm = MmapCol::open(&format!("{}/orders/o_orderdate.bin", gdir));
    let o_custkey_mm = MmapCol::open(&format!("{}/orders/o_custkey.bin", gdir));
    let o_orderkey_mm = MmapCol::open(&format!("{}/orders/o_orderkey.bin", gdir));
    let o_shipprio_mm = MmapCol::open(&format!("{}/orders/o_shippriority.bin", gdir));
    let li_idx_mm = MmapCol::open(&format!("{}/indexes/lineitem_orderkey_index.bin", gdir));
    let l_shipdate_mm = MmapCol::open(&format!("{}/lineitem/l_shipdate.bin", gdir));
    let l_extprice_mm = MmapCol::open(&format!("{}/lineitem/l_extendedprice.bin", gdir));
    let l_discount_mm = MmapCol::open(&format!("{}/lineitem/l_discount.bin", gdir));

    let c_mkt = c_mktseg.as_slice::<u8>();
    let n_orders = o_orderdate_mm.len() / 4;
    let o_orderdate = o_orderdate_mm.as_slice::<i32>();
    let o_custkey = o_custkey_mm.as_slice::<i32>();
    let o_orderkey = o_orderkey_mm.as_slice::<i32>();
    let o_shipprio = o_shipprio_mm.as_slice::<i32>();

    let cust_lookup_data = cust_lookup_mm.mmap.as_ref();
    let cust_max = u32::from_ne_bytes(cust_lookup_data[0..4].try_into().unwrap());
    let cust_lookup: &[i32] = unsafe {
        std::slice::from_raw_parts(cust_lookup_data[4..].as_ptr() as *const i32,
                                   (cust_lookup_data.len() - 4) / 4)
    };

    let li_idx_data = li_idx_mm.mmap.as_ref();
    let li_max = u32::from_ne_bytes(li_idx_data[0..4].try_into().unwrap());
    let li_idx: &[LiIdx] = unsafe {
        std::slice::from_raw_parts(li_idx_data[4..].as_ptr() as *const LiIdx,
                                   (li_idx_data.len() - 4) / 8)
    };

    let l_shipdate = l_shipdate_mm.as_slice::<i32>();
    let l_extprice = l_extprice_mm.as_slice::<f64>();
    let l_discount = l_discount_mm.as_slice::<f64>();

    // Parallel scan
    let thread_heaps: Vec<Vec<ResultRow>>;
    {
        let _t = PhaseTimer::new("main_scan");

        let chunk_size = 100000;
        let n_chunks = (n_orders + chunk_size - 1) / chunk_size;

        thread_heaps = (0..n_chunks).into_par_iter().map(|chunk_idx| {
            let start = chunk_idx * chunk_size;
            let end = (start + chunk_size).min(n_orders);
            let mut my_heap = Vec::with_capacity(TOPK);

            for i in start..end {
                if o_orderdate[i] >= DATE_CUT { continue; }

                let ck = o_custkey[i];
                if (ck as u32) > cust_max { continue; }
                let cust_row = cust_lookup[ck as usize];
                if cust_row < 0 { continue; }
                if c_mkt[cust_row as usize] != building_code { continue; }

                let ok = o_orderkey[i];
                if (ok as u32) > li_max { continue; }
                let le = &li_idx[ok as usize];
                if le.count == 0 { continue; }

                let mut rev = 0.0f64;
                let js = le.start as usize;
                let je = js + le.count as usize;
                for j in js..je {
                    unsafe {
                        if *l_shipdate.get_unchecked(j) > DATE_CUT {
                            rev += *l_extprice.get_unchecked(j) * (1.0 - *l_discount.get_unchecked(j));
                        }
                    }
                }
                if rev <= 0.0 { continue; }

                heap_insert(&mut my_heap, ResultRow {
                    revenue: rev, orderkey: ok, orderdate: o_orderdate[i], order_row_idx: i as i32
                });
            }
            my_heap
        }).collect();
    }

    // Merge and output
    {
        let _t = PhaseTimer::new("output");
        let mut final_heap: Vec<ResultRow> = Vec::with_capacity(TOPK);
        for h in &thread_heaps {
            for r in h {
                heap_insert(&mut final_heap, *r);
            }
        }
        final_heap.sort_by(|a, b| b.revenue.partial_cmp(&a.revenue).unwrap()
            .then(a.orderdate.cmp(&b.orderdate)));

        let path = format!("{}/Q3.csv", rdir);
        let mut f = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
        writeln!(f, "l_orderkey,revenue,o_orderdate,o_shippriority").unwrap();
        for r in &final_heap {
            writeln!(f, "{},{:.4},{},{}", r.orderkey, r.revenue,
                epoch_days_to_date_str(r.orderdate),
                o_shipprio[r.order_row_idx as usize]).unwrap();
        }
    }
    } // end total scope
    std::process::exit(0);
}
