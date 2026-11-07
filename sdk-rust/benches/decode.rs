//! Per-checkpoint cost of the SDK's hot path on recorded Mainnet frames:
//! zstd decoding, frame parsing (raw mode) and protobuf decoding (typed).
//!
//! ```sh
//! cargo bench -p miraicloud --bench decode            # [-- ROUNDS]
//! ```
//! Fixtures: `tests/fixtures/{lean,full}.frames.zst` (see
//! `tests/fixtures.rs`). Times are per checkpoint, p50 and mean over all
//! checkpoints and rounds.

use std::hint::black_box;
use std::time::Instant;

use miraicloud::streams::__private::{Frame, Parser, decode_checkpoint};

fn compressed(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).expect(&path)
}

/// p50 and mean of `samples` (ns).
fn stats(samples: &mut [f64]) -> (f64, f64) {
    samples.sort_by(f64::total_cmp);
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    (samples[samples.len() / 2], mean)
}

fn report(name: &str, samples: &mut [f64]) {
    let (p50, mean) = stats(samples);
    println!(
        "  {name:<28} p50 {:>9.2} µs   mean {:>9.2} µs",
        p50 / 1e3,
        mean / 1e3
    );
}

fn main() {
    let rounds: usize = std::env::args()
        .skip(1)
        .find_map(|a| a.parse().ok())
        .unwrap_or(20);
    for name in ["lean.frames.zst", "full.frames.zst"] {
        let zst = compressed(name);
        let body = zstd::decode_all(&zst[..]).unwrap();
        let mut probe = vec![];
        Parser::default().push(&body, &mut probe).unwrap();
        let n = probe.len();
        println!(
            "{name}: {n} checkpoints, {:.1} KB/checkpoint compressed, {:.1} KB decompressed",
            zst.len() as f64 / n as f64 / 1e3,
            body.len() as f64 / n as f64 / 1e3
        );
        let (mut unzstd, mut parse, mut typed) = (vec![], vec![], vec![]);
        for _ in 0..rounds {
            let t = Instant::now();
            black_box(zstd::decode_all(&zst[..]).unwrap());
            unzstd.push(t.elapsed().as_nanos() as f64 / n as f64);

            let t = Instant::now();
            let mut frames = vec![];
            Parser::default()
                .push(black_box(&body), &mut frames)
                .unwrap();
            parse.push(t.elapsed().as_nanos() as f64 / n as f64);

            for f in &frames {
                let Frame::Checkpoint { seq, checkpoint } = f else {
                    continue;
                };
                let t = Instant::now();
                black_box(decode_checkpoint(*seq, checkpoint).unwrap());
                typed.push(t.elapsed().as_nanos() as f64);
            }
        }
        report("zstd decode", &mut unzstd);
        report("frame parse (raw mode)", &mut parse);
        report("protobuf decode (typed)", &mut typed);
    }
}
