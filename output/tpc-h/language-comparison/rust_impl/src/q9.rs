mod common;
use common::*;
use rayon::prelude::*;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use memchr::memmem;

#[repr(C)]
struct PSEntry { start: u32, count: u32 }

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let gdir = &args[1];
    let rdir = &args[2];

    prewarm_rayon(0);

    {
    let _total = PhaseTimer::new("total");

    // mmap all files
    let m_n_nk = MmapCol::open(&format!("{}/nation/n_nationkey.bin", gdir));
    let m_n_no = MmapCol::open(&format!("{}/nation/n_name_offsets.bin", gdir));
    let m_n_nd = MmapCol::open(&format!("{}/nation/n_name_data.bin", gdir));
    let m_s_nk = MmapCol::open(&format!("{}/supplier/s_nationkey.bin", gdir));
    let m_p_no = MmapCol::open(&format!("{}/part/p_name_offsets.bin", gdir));
    let m_p_nd = MmapCol::open(&format!("{}/part/p_name_data.bin", gdir));
    let m_o_od = MmapCol::open(&format!("{}/orders/o_orderdate.bin", gdir));
    let m_ol = MmapCol::open(&format!("{}/indexes/orders_orderkey_lookup.bin", gdir));
    let m_ps_sk = MmapCol::open(&format!("{}/partsupp/ps_suppkey.bin", gdir));
    let m_ps_sc = MmapCol::open(&format!("{}/partsupp/ps_supplycost.bin", gdir));
    let m_ps_idx = MmapCol::open(&format!("{}/indexes/partsupp_pk_index.bin", gdir));
    let m_l_pk = MmapCol::open(&format!("{}/lineitem/l_partkey.bin", gdir));
    let m_l_sk = MmapCol::open(&format!("{}/lineitem/l_suppkey.bin", gdir));
    let m_l_ok = MmapCol::open(&format!("{}/lineitem/l_orderkey.bin", gdir));
    let m_l_ep = MmapCol::open(&format!("{}/lineitem/l_extendedprice.bin", gdir));
    let m_l_disc = MmapCol::open(&format!("{}/lineitem/l_discount.bin", gdir));
    let m_l_qty = MmapCol::open(&format!("{}/lineitem/l_quantity.bin", gdir));

    let n_nationkey = m_n_nk.as_slice::<i32>();
    let n_name_off = m_n_no.as_slice::<i64>();
    let n_name_data = m_n_nd.mmap.as_ref();
    let s_nationkey = m_s_nk.as_slice::<i32>();
    let p_name_off = m_p_no.as_slice::<i64>();
    let p_name_data = m_p_nd.mmap.as_ref();
    let o_orderdate = m_o_od.as_slice::<i32>();

    let ol_data = m_ol.mmap.as_ref();
    let orders_lookup: &[i32] = unsafe {
        std::slice::from_raw_parts(ol_data[4..].as_ptr() as *const i32, (ol_data.len() - 4) / 4)
    };

    let ps_suppkey = m_ps_sk.as_slice::<i32>();
    let ps_supplycost = m_ps_sc.as_slice::<f64>();
    let ps_idx_data = m_ps_idx.mmap.as_ref();
    let ps_index: &[PSEntry] = unsafe {
        std::slice::from_raw_parts(ps_idx_data[4..].as_ptr() as *const PSEntry, (ps_idx_data.len() - 4) / 8)
    };

    let l_partkey = m_l_pk.as_slice::<i32>();
    let l_suppkey = m_l_sk.as_slice::<i32>();
    let l_orderkey = m_l_ok.as_slice::<i32>();
    let l_extprice = m_l_ep.as_slice::<f64>();
    let l_discount = m_l_disc.as_slice::<f64>();
    let l_quantity = m_l_qty.as_slice::<f64>();

    let n_parts = p_name_off.len() - 1;
    let n_lineitem = l_partkey.len();

    // Load nation names
    let mut nation_names = vec![String::new(); 25];
    {
        let _t = PhaseTimer::new("load_nation");
        for i in 0..25 {
            let nk = n_nationkey[i] as usize;
            let s = n_name_off[i] as usize;
            let e = n_name_off[i + 1] as usize;
            nation_names[nk] = String::from_utf8_lossy(&n_name_data[s..e]).to_string();
        }
    }

    // Filter parts — parallel bitset
    let bitset_words = (n_parts + 63) / 64;
    let part_bitset: Vec<AtomicU64> = (0..bitset_words).map(|_| AtomicU64::new(0)).collect();
    {
        let _t = PhaseTimer::new("filter_part");
        let finder = memmem::Finder::new(b"green");
        (0..n_parts).into_par_iter().for_each(|i| {
            let s = p_name_off[i] as usize;
            let e = p_name_off[i + 1] as usize;
            let name = &p_name_data[s..e];
            if finder.find(name).is_some() {
                part_bitset[i >> 6].fetch_or(1u64 << (i & 63), Ordering::Relaxed);
            }
        });
    }

    // Main scan — parallel
    let n_threads = rayon::current_num_threads();
    // 25 nations × 8 years = 200 slots per thread
    let agg_size = 256;
    let all_agg: Vec<f64> = vec![0.0; n_threads * agg_size];
    let all_agg_ptr = all_agg.as_ptr() as usize; // for unsafe Send

    {
        let _t = PhaseTimer::new("main_scan");

        let chunk = (n_lineitem + n_threads - 1) / n_threads;
        (0..n_threads).into_par_iter().for_each(|tid| {
            let my_agg = unsafe {
                std::slice::from_raw_parts_mut(
                    (all_agg_ptr as *mut f64).add(tid * agg_size), agg_size)
            };

            let start = tid * chunk;
            let end = (start + chunk).min(n_lineitem);

            for i in start..end {
                let pk = unsafe { *l_partkey.get_unchecked(i) } as u32;
                let idx = (pk - 1) as usize;
                let word = unsafe { part_bitset.get_unchecked(idx >> 6).load(Ordering::Relaxed) };
                if word & (1u64 << (idx & 63)) == 0 { continue; }

                let sk = unsafe { *l_suppkey.get_unchecked(i) };
                let ok = unsafe { *l_orderkey.get_unchecked(i) } as u32;

                let ps_entry = unsafe { &*ps_index.as_ptr().add(pk as usize) };
                let mut supplycost = 0.0f64;
                let ps_start = ps_entry.start as usize;
                let ps_count = ps_entry.count as usize;
                for j in ps_start..ps_start + ps_count {
                    unsafe {
                        if *ps_suppkey.get_unchecked(j) == sk {
                            supplycost = *ps_supplycost.get_unchecked(j);
                            break;
                        }
                    }
                }

                let order_row = unsafe { *orders_lookup.get_unchecked(ok as usize) };
                let year_off = (year_from_days(unsafe { *o_orderdate.get_unchecked(order_row as usize) }) - 1992) as usize;
                let nk = unsafe { *s_nationkey.get_unchecked((sk - 1) as usize) } as usize;

                let amount = unsafe {
                    *l_extprice.get_unchecked(i) * (1.0 - *l_discount.get_unchecked(i))
                    - supplycost * *l_quantity.get_unchecked(i)
                };
                unsafe { *my_agg.get_unchecked_mut(nk * 8 + year_off) += amount; }
            }
        });
    }

    // Merge and output
    {
        let _t = PhaseTimer::new("output");
        let mut global_agg = vec![0.0f64; 256];
        for t in 0..n_threads {
            let base = t * agg_size;
            for j in 0..200 {
                global_agg[j] += all_agg[base + j];
            }
        }

        let mut out: Vec<(usize, i32, f64)> = Vec::with_capacity(175);
        for n in 0..25 {
            for y in 0..8 {
                let v = global_agg[n * 8 + y];
                if v != 0.0 { out.push((n, 1992 + y as i32, v)); }
            }
        }
        out.sort_by(|a, b| nation_names[a.0].cmp(&nation_names[b.0]).then(b.1.cmp(&a.1)));

        let path = format!("{}/Q9.csv", rdir);
        let mut f = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
        writeln!(f, "nation,o_year,sum_profit").unwrap();
        for (n, y, v) in &out {
            writeln!(f, "{},{},{:.2}", nation_names[*n], y, v).unwrap();
        }
    }
    } // end total scope
    std::process::exit(0);
}
