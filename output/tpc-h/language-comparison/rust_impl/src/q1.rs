mod common;
use common::*;
use rayon::prelude::*;
use std::io::Write;

const NUM_GROUPS: usize = 6;
const THRESHOLD: i32 = 10471;

#[derive(Default, Clone)]
struct Acc {
    sum_qty: f64, sum_base_price: f64, sum_disc_price: f64,
    sum_charge: f64, sum_discount: f64, count: i64,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let gendb = &args[1];
    let results = &args[2];

    prewarm_rayon(0);

    {
    let _total = PhaseTimer::new("total");

    // Data loading
    let (n, shipdate, returnflag, linestatus, quantity, extprice, discount, tax);
    {
        let _t = PhaseTimer::new("data_loading");
        let m_sd = MmapCol::open(&format!("{}/lineitem/l_shipdate.bin", gendb));
        shipdate = m_sd;
        shipdate.advise_sequential();
        n = shipdate.as_slice::<i32>().len();
        returnflag = MmapCol::open(&format!("{}/lineitem/l_returnflag.bin", gendb));
        returnflag.advise_sequential();
        linestatus = MmapCol::open(&format!("{}/lineitem/l_linestatus.bin", gendb));
        linestatus.advise_sequential();
        quantity = MmapCol::open(&format!("{}/lineitem/l_quantity.bin", gendb));
        quantity.advise_sequential();
        extprice = MmapCol::open(&format!("{}/lineitem/l_extendedprice.bin", gendb));
        extprice.advise_sequential();
        discount = MmapCol::open(&format!("{}/lineitem/l_discount.bin", gendb));
        discount.advise_sequential();
        tax = MmapCol::open(&format!("{}/lineitem/l_tax.bin", gendb));
        tax.advise_sequential();
    }

    // Zone map
    let zm_col = MmapCol::open(&format!("{}/indexes/lineitem_shipdate_zonemap.bin", gendb));
    let zm_data = zm_col.mmap.as_ref();
    let zm_num_blocks = u32::from_ne_bytes(zm_data[0..4].try_into().unwrap()) as usize;
    let zm_block_size = u32::from_ne_bytes(zm_data[4..8].try_into().unwrap()) as usize;

    #[repr(C)]
    struct ZoneEntry { min_val: i32, max_val: i32 }
    let zm_entries: &[ZoneEntry] = unsafe {
        std::slice::from_raw_parts(zm_data[8..].as_ptr() as *const ZoneEntry, zm_num_blocks)
    };

    let sd = shipdate.as_slice::<i32>();
    let rf = returnflag.as_slice::<u8>();
    let ls = linestatus.as_slice::<u8>();
    let qty = quantity.as_slice::<f64>();
    let ep = extprice.as_slice::<f64>();
    let disc = discount.as_slice::<f64>();
    let tx = tax.as_slice::<f64>();

    let mut rf_map = [0u8; 256];
    rf_map[b'A' as usize] = 0; rf_map[b'N' as usize] = 1; rf_map[b'R' as usize] = 2;
    let mut ls_map = [0u8; 256];
    ls_map[b'F' as usize] = 0; ls_map[b'O' as usize] = 1;

    // Main scan — parallel over zone-map blocks
    let final_groups;
    {
        let _t = PhaseTimer::new("main_scan");

        // Collect qualifying blocks
        let blocks: Vec<(usize, bool)> = (0..zm_num_blocks)
            .filter(|&b| zm_entries[b].min_val <= THRESHOLD)
            .map(|b| (b, zm_entries[b].max_val <= THRESHOLD))
            .collect();

        let thread_results: Vec<[Acc; NUM_GROUPS]> = blocks
            .par_iter()
            .map(|&(b, all_pass)| {
                let start = b * zm_block_size;
                let end = (start + zm_block_size).min(n);
                let mut local = [Acc::default(), Acc::default(), Acc::default(),
                                 Acc::default(), Acc::default(), Acc::default()];

                if all_pass {
                    for i in start..end {
                        unsafe {
                            let key = (*rf_map.get_unchecked(*rf.get_unchecked(i) as usize) * 2
                                + *ls_map.get_unchecked(*ls.get_unchecked(i) as usize)) as usize;
                            let a = local.get_unchecked_mut(key);
                            let e = *ep.get_unchecked(i);
                            let d = *disc.get_unchecked(i);
                            let dp = e * (1.0 - d);
                            a.sum_qty += *qty.get_unchecked(i);
                            a.sum_base_price += e;
                            a.sum_disc_price += dp;
                            a.sum_charge += dp * (1.0 + *tx.get_unchecked(i));
                            a.sum_discount += d;
                            a.count += 1;
                        }
                    }
                } else {
                    for i in start..end {
                        unsafe {
                            if *sd.get_unchecked(i) > THRESHOLD { continue; }
                            let key = (*rf_map.get_unchecked(*rf.get_unchecked(i) as usize) * 2
                                + *ls_map.get_unchecked(*ls.get_unchecked(i) as usize)) as usize;
                            let a = local.get_unchecked_mut(key);
                            let e = *ep.get_unchecked(i);
                            let d = *disc.get_unchecked(i);
                            let dp = e * (1.0 - d);
                            a.sum_qty += *qty.get_unchecked(i);
                            a.sum_base_price += e;
                            a.sum_disc_price += dp;
                            a.sum_charge += dp * (1.0 + *tx.get_unchecked(i));
                            a.sum_discount += d;
                            a.count += 1;
                        }
                    }
                }
                local
            })
            .collect();

        // Merge
        let mut merged = [Acc::default(), Acc::default(), Acc::default(),
                          Acc::default(), Acc::default(), Acc::default()];
        for local in &thread_results {
            for g in 0..NUM_GROUPS {
                merged[g].sum_qty += local[g].sum_qty;
                merged[g].sum_base_price += local[g].sum_base_price;
                merged[g].sum_disc_price += local[g].sum_disc_price;
                merged[g].sum_charge += local[g].sum_charge;
                merged[g].sum_discount += local[g].sum_discount;
                merged[g].count += local[g].count;
            }
        }
        final_groups = merged;
    }

    // Output
    {
        let _t = PhaseTimer::new("output");
        let rf_chars = [b'A', b'N', b'R'];
        let ls_chars = [b'F', b'O'];

        let mut rows: Vec<(u8, u8, &Acc)> = Vec::new();
        for g in 0..NUM_GROUPS {
            if final_groups[g].count == 0 { continue; }
            rows.push((rf_chars[g / 2], ls_chars[g % 2], &final_groups[g]));
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let path = format!("{}/Q1.csv", results);
        let mut f = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
        writeln!(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order").unwrap();
        for (r, l, a) in &rows {
            let c = a.count as f64;
            writeln!(f, "{},{},{:.2},{:.2},{:.4},{:.6},{:.2},{:.2},{:.2},{}",
                *r as char, *l as char,
                a.sum_qty, a.sum_base_price, a.sum_disc_price, a.sum_charge,
                a.sum_qty / c, a.sum_base_price / c, a.sum_discount / c,
                a.count).unwrap();
        }
    }
    } // end total scope
    std::process::exit(0);
}
