//! Against production; run with
//! `MIRAICLOUD_API_KEY=mc1_… cargo test -- --ignored` (a key with
//! `stream:read`). `MIRAICLOUD_STREAM_URL` overrides the stream URL
//! (default: Mainnet).

use std::time::Duration;

use futures::StreamExt;
use miraicloud::streams::Event;

fn builder() -> miraicloud::streams::Builder {
    let mut b = miraicloud::streams::Client::builder();
    if let Ok(key) = std::env::var("MIRAICLOUD_API_KEY") {
        b = b.api_key(key);
    }
    match std::env::var("MIRAICLOUD_STREAM_URL") {
        Ok(url) => b.url(url),
        Err(_) => b,
    }
}

/// `n` events; checkpoints must be contiguous (gaps are returned as such).
async fn take(builder: miraicloud::streams::Builder, n: usize) -> Vec<Event> {
    let mut sub = builder.build().unwrap().subscribe();
    let mut out: Vec<Event> = Vec::new();
    let mut next = None;
    while out.len() < n {
        let ev = tokio::time::timeout(Duration::from_secs(120), sub.next())
            .await
            .expect("progress")
            .expect("open")
            .expect("event");
        match &ev {
            Event::Checkpoint { seq, checkpoint } => {
                if let Some(n) = next {
                    assert_eq!(*seq, n, "contiguous");
                }
                if let Some(s) = checkpoint.sequence_number {
                    assert_eq!(s, *seq);
                }
                next = Some(seq + 1);
            }
            Event::Gap { from, to } => {
                if let Some(n) = next {
                    assert_eq!(*from, n, "gap starts at the cursor");
                }
                next = Some(to + 1);
            }
        }
        out.push(ev);
    }
    assert_eq!(sub.cursor(), next.map(|n| n - 1));
    out
}

fn first_seq(events: &[Event]) -> u64 {
    match &events[0] {
        Event::Checkpoint { seq, .. } => *seq,
        Event::Gap { from, .. } => *from,
    }
}

#[tokio::test]
#[ignore]
async fn live_full_checkpoints_decode() {
    // Full canonical checkpoints decode with the crates.io sui-rpc types.
    let events = take(builder(), 10).await;
    for ev in &events {
        let Event::Checkpoint { checkpoint, .. } = ev else {
            panic!("unexpected gap")
        };
        assert!(checkpoint.summary.is_some() && checkpoint.contents.is_some());
    }
}

#[tokio::test]
#[ignore]
async fn old_resume_is_backfilled_from_the_archive() {
    // Tip from a first subscription, then resume 5000 back (~20 min, older
    // than the gateway's ring): the client reads that part from the archive,
    // then hands over to the stream without holes or duplicates.
    let tip = first_seq(&take(builder().read_mask(["digest"]), 1).await);
    let start = tip - 5_000;
    let events = take(builder().read_mask(["digest"]).start_at(start), 5_100).await;
    assert_eq!(first_seq(&events), start);
    let gaps = events
        .iter()
        .filter(|e| matches!(e, Event::Gap { .. }))
        .count();
    assert_eq!(gaps, 0, "the archive has every checkpoint");
}

#[tokio::test]
#[ignore]
async fn raw_matches_typed() {
    // The same 20 checkpoints (lean mask, from the ring) through
    // `subscribe_raw` and `subscribe`: same order, and the raw payload
    // decodes to the typed checkpoint.
    let lean = || {
        builder().read_mask([
            "summary.bcs",
            "transactions.digest",
            "transactions.events.bcs",
        ])
    };
    let tip = first_seq(&take(builder().read_mask(["digest"]), 1).await);
    let typed = take(lean().start_at(tip - 100), 20).await;
    let mut raw = lean().start_at(tip - 100).build().unwrap().subscribe_raw();
    for want in &typed {
        let ev = tokio::time::timeout(Duration::from_secs(120), raw.next())
            .await
            .expect("progress")
            .expect("open")
            .expect("event");
        match (ev, want) {
            (
                miraicloud::streams::RawEvent::Checkpoint { seq, data },
                Event::Checkpoint { seq: s, checkpoint },
            ) => {
                assert_eq!(seq, *s);
                let decoded =
                    miraicloud::streams::__private::decode_checkpoint(seq, &data).unwrap();
                assert_eq!(&decoded, checkpoint, "checkpoint {seq}");
            }
            (miraicloud::streams::RawEvent::Gap { from, to }, Event::Gap { from: f, to: t }) => {
                assert_eq!((from, to), (*f, *t));
            }
            (got, want) => panic!("raw {got:?} vs typed {want:?}"),
        }
    }
    assert_eq!(raw.cursor(), typed_cursor(&typed));
}

fn typed_cursor(events: &[Event]) -> Option<u64> {
    events.last().map(|e| match e {
        Event::Checkpoint { seq, .. } => *seq,
        Event::Gap { to, .. } => *to,
    })
}
