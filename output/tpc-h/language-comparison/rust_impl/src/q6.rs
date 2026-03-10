mod common;
use common::*;
use rayon::prelude::*;
use std::io::Write;

const DATE_LO: i32 = 8766;
const DATE_HI: i32 = 9131;
const DISC_LO: f64 = 0.05;
const DISC_HI: f64 = 0.07;
const QTY_THR: f64 = 24.0;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let gendb = &args[1];
    let results = &args[2];

    prewarm_rayon(0);

    let col_sd = MmapCol::open(&format!("{}/lineitem/l_shipdate.bin", gendb));
    let col_disc = MmapCol::open(&format!("{}/lineitem/l_discount.bin", gendb));
    let col_qty = MmapCol::open(&format!("{}/lineitem/l_quantity.bin", gendb));
    let col_ep = MmapCol::open(&format!("{}/lineitem/l_extendedprice.bin", gendb));
    let nrows = col_sd.as_slice::<i32>().len();

    {
        let _total = PhaseTimer::new("total");

        // Zone map
        let zm_col = MmapCol::open(&format!("{}/indexes/lineitem_shipdate_zonemap.bin", gendb));
        let zm_data = zm_col.mmap.as_ref();
        let num_blocks = u32::from_ne_bytes(zm_data[0..4].try_into().unwrap()) as usize;
        let block_size = u32::from_ne_bytes(zm_data[4..8].try_into().unwrap()) as usize;

        #[repr(C)]
        struct ZE { min_date: i32, max_date: i32 }
        let zones: &[ZE] = unsafe {
            std::slice::from_raw_parts(zm_data[8..].as_ptr() as *const ZE, num_blocks)
        };

        let mut full_blocks = Vec::new();
        let mut partial_blocks = Vec::new();
        {
            let _t = PhaseTimer::new("data_loading");
            for b in 0..num_blocks {
                if zones[b].max_date < DATE_LO || zones[b].min_date >= DATE_HI { continue; }
                if zones[b].min_date >= DATE_LO && zones[b].max_date < DATE_HI {
                    full_blocks.push(b);
                } else {
                    partial_blocks.push(b);
                }
            }
        }

        let n_full = full_blocks.len();
        let mut qual_blocks = full_blocks;
        qual_blocks.extend(partial_blocks);

        let sd = col_sd.as_slice::<i32>();
        let disc = col_disc.as_slice::<f64>();
        let qty = col_qty.as_slice::<f64>();
        let ep = col_ep.as_slice::<f64>();

        let revenue;
        {
            let _t = PhaseTimer::new("main_scan");

            revenue = qual_blocks.par_iter().enumerate().map(|(qi, &b)| {
                let start = b * block_size;
                let end = (start + block_size).min(nrows);
                let mut local_sum = 0.0f64;

                if qi < n_full {
                    for i in start..end {
                        let d = unsafe { *disc.get_unchecked(i) };
                        if d >= DISC_LO && d <= DISC_HI && unsafe { *qty.get_unchecked(i) } < QTY_THR {
                            local_sum += unsafe { *ep.get_unchecked(i) } * d;
                        }
                    }
                } else {
                    for i in start..end {
                        if unsafe { *sd.get_unchecked(i) } >= DATE_LO
                            && unsafe { *sd.get_unchecked(i) } < DATE_HI
                            && unsafe { *disc.get_unchecked(i) } >= DISC_LO
                            && unsafe { *disc.get_unchecked(i) } <= DISC_HI
                            && unsafe { *qty.get_unchecked(i) } < QTY_THR
                        {
                            local_sum += unsafe { *ep.get_unchecked(i) } * unsafe { *disc.get_unchecked(i) };
                        }
                    }
                }
                local_sum
            }).sum::<f64>();
        }

        {
            let _t = PhaseTimer::new("output");
            let path = format!("{}/Q6.csv", results);
            let mut f = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
            writeln!(f, "revenue\n{:.4}", revenue).unwrap();
        }
    }

    std::process::exit(0);
}
