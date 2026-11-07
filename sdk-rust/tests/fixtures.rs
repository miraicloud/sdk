//! The frame parser against recorded Mainnet checkpoints: the same frames
//! however the bytes are chunked, and every checkpoint decodes.
//!
//! - `lean.frames.zst`: 24 checkpoints, read mask
//!   `summary.bcs,transactions.digest,transactions.events.bcs`.
//! - `full.frames.zst`: 3 full checkpoints.
//!
//! Both are stream bodies as the gateway sends them (one zstd frame per
//! stream frame).

use miraicloud::streams::__private::{Frame, Parser, decode_checkpoint};

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    zstd::decode_all(std::fs::File::open(&path).expect(&path)).unwrap()
}

fn parse(chunks: &[&[u8]]) -> Vec<Frame> {
    let mut p = Parser::default();
    let mut out = vec![];
    for c in chunks {
        p.push(c, &mut out).unwrap();
    }
    out
}

/// Splits at pseudo-random points, 1 byte to 64 KiB apart.
fn random_chunks(body: &[u8], mut seed: u64) -> Vec<&[u8]> {
    let mut out = vec![];
    let mut rest = body;
    while !rest.is_empty() {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let n = (1 + (seed % 65_536) as usize).min(rest.len());
        let (a, b) = rest.split_at(n);
        out.push(a);
        rest = b;
    }
    out
}

fn check(name: &str, count: usize) {
    let body = fixture(name);
    let whole = parse(&[&body]);
    assert_eq!(whole.len(), count);
    for seed in 1..20u64 {
        assert_eq!(
            parse(&random_chunks(&body, seed)),
            whole,
            "{name} seed {seed}"
        );
    }
    let mut prev = None;
    for frame in &whole {
        let Frame::Checkpoint { seq, checkpoint } = frame else {
            panic!("{name}: unexpected {frame:?}")
        };
        if let Some(p) = prev {
            assert!(*seq > p, "{name}: ascending");
        }
        prev = Some(*seq);
        let cp = decode_checkpoint(*seq, checkpoint).unwrap();
        assert!(
            !cp.transactions.is_empty(),
            "{name} {seq}: has transactions"
        );
    }
}

#[test]
fn lean_fixture() {
    check("lean.frames.zst", 24);
}

#[test]
fn full_fixture() {
    check("full.frames.zst", 3);
    // Full checkpoints carry the canonical fields.
    let whole = parse(&[&fixture("full.frames.zst")]);
    let Frame::Checkpoint { seq, checkpoint } = &whole[0] else {
        panic!()
    };
    let cp = decode_checkpoint(*seq, checkpoint).unwrap();
    assert_eq!(cp.sequence_number, Some(*seq));
    assert!(cp.summary.is_some() && cp.objects.is_some());
}
