//! The stream's wire format (after zstd decoding): frames of a 5-byte
//! header (`kind: u8`, `len: u32` big-endian) and `len` bytes of body.
//!
//! | kind | body |
//! |------|------|
//! | `0` checkpoint | protobuf `sui.rpc.v2.SubscribeCheckpointsResponse` |
//! | `1` gap | `from`, `to` (inclusive) as u64 big-endian |
//! | `2` keepalive | empty |
//!
//! Unknown kinds are skipped.

use bytes::{Buf, Bytes, BytesMut};

pub const CONTENT_TYPE: &str = "application/vnd.miraicloud.checkpoints.v1+proto";
const HEADER_LEN: usize = 5;
const CHECKPOINT: u8 = 0;
const GAP: u8 = 1;
/// A frame larger than this is treated as a corrupt stream (the largest
/// Mainnet checkpoints are ~20 MB).
const MAX_FRAME: usize = 256 * 1024 * 1024;

/// A parsed frame. Checkpoint bodies share the parser's buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// `cursor` (the sequence number) and the encoded `Checkpoint` (empty if
    /// the response had none).
    Checkpoint {
        seq: u64,
        checkpoint: Bytes,
    },
    Gap {
        from: u64,
        to: u64,
    },
    /// A keepalive or an unknown kind.
    Other,
}

/// Incremental frame parser.
#[derive(Default)]
pub struct Parser {
    buf: BytesMut,
}

impl Parser {
    /// Feed decompressed bytes; appends the frames completed by them.
    pub fn push(&mut self, chunk: &[u8], out: &mut Vec<Frame>) -> Result<(), String> {
        self.buf.extend_from_slice(chunk);
        while self.buf.len() >= HEADER_LEN {
            let len = u32::from_be_bytes(self.buf[1..5].try_into().expect("4 bytes")) as usize;
            if len > MAX_FRAME {
                return Err(format!("frame of {len} bytes"));
            }
            if self.buf.len() < HEADER_LEN + len {
                // Reserve once for the whole frame instead of growing in steps.
                self.buf.reserve(HEADER_LEN + len - self.buf.len());
                break;
            }
            let kind = self.buf[0];
            self.buf.advance(HEADER_LEN);
            let body = self.buf.split_to(len).freeze();
            out.push(match kind {
                CHECKPOINT => {
                    let (seq, checkpoint) = split_response(&body)?;
                    Frame::Checkpoint { seq, checkpoint }
                }
                GAP if len == 16 => Frame::Gap {
                    from: u64::from_be_bytes(body[..8].try_into().expect("8 bytes")),
                    to: u64::from_be_bytes(body[8..].try_into().expect("8 bytes")),
                },
                GAP => return Err(format!("gap frame of {len} bytes")),
                _ => Frame::Other,
            });
        }
        Ok(())
    }
}

/// `cursor` and the encoded `checkpoint` of a `SubscribeCheckpointsResponse`
/// (fields 1 and 2), without decoding the checkpoint.
pub fn split_response(body: &Bytes) -> Result<(u64, Bytes), String> {
    let mut buf = &body[..];
    let (mut cursor, mut checkpoint) = (None, Bytes::new());
    while buf.has_remaining() {
        let key = prost::encoding::decode_varint(&mut buf).map_err(|e| e.to_string())?;
        let (field, wire) = (key >> 3, key & 7);
        match (field, wire) {
            (1, 0) => {
                cursor = Some(prost::encoding::decode_varint(&mut buf).map_err(|e| e.to_string())?)
            }
            (2, 2) => {
                let len = prost::encoding::decode_varint(&mut buf).map_err(|e| e.to_string())?;
                let len = usize::try_from(len).map_err(|e| e.to_string())?;
                if len > buf.len() {
                    return Err("truncated checkpoint".into());
                }
                let start = body.len() - buf.len();
                checkpoint = body.slice(start..start + len);
                buf.advance(len);
            }
            (_, wire) => {
                let wire = prost::encoding::WireType::try_from(wire).map_err(|e| e.to_string())?;
                prost::encoding::skip_field(
                    wire,
                    field as u32,
                    &mut buf,
                    prost::encoding::DecodeContext::default(),
                )
                .map_err(|e| e.to_string())?;
            }
        }
    }
    let cursor = cursor.ok_or("checkpoint frame without cursor")?;
    Ok((cursor, checkpoint))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::streams::Checkpoint;
    use prost::Message;
    use sui_rpc::proto::sui::rpc::v2::SubscribeCheckpointsResponse;

    pub fn checkpoint_frame(seq: u64, checkpoint: Option<Checkpoint>) -> Vec<u8> {
        let mut r = SubscribeCheckpointsResponse::default();
        r.cursor = Some(seq);
        r.checkpoint = checkpoint;
        let body = r.encode_to_vec();
        let mut out = vec![CHECKPOINT];
        out.extend((body.len() as u32).to_be_bytes());
        out.extend(body);
        out
    }

    pub fn gap_frame(from: u64, to: u64) -> Vec<u8> {
        let mut out = vec![GAP, 0, 0, 0, 16];
        out.extend(from.to_be_bytes());
        out.extend(to.to_be_bytes());
        out
    }

    fn cp(seq: u64) -> Checkpoint {
        let mut c = Checkpoint::default();
        c.sequence_number = Some(seq);
        c.digest = Some(format!("d{seq}"));
        c
    }

    #[test]
    fn parses_frames_across_arbitrary_splits() {
        let mut stream = checkpoint_frame(5, Some(cp(5)));
        stream.extend([2, 0, 0, 0, 0]); // keepalive
        stream.extend(gap_frame(6, 9));
        stream.extend([9, 0, 0, 0, 2, 1, 2]); // unknown kind
        stream.extend(checkpoint_frame(10, None));
        let parse = |chunks: &[&[u8]]| {
            let mut p = Parser::default();
            let mut out = vec![];
            for c in chunks {
                p.push(c, &mut out).unwrap();
            }
            out
        };
        let whole = parse(&[&stream]);
        assert_eq!(
            whole,
            [
                Frame::Checkpoint {
                    seq: 5,
                    checkpoint: cp(5).encode_to_vec().into()
                },
                Frame::Other,
                Frame::Gap { from: 6, to: 9 },
                Frame::Other,
                Frame::Checkpoint {
                    seq: 10,
                    checkpoint: Bytes::new()
                },
            ]
        );
        for i in 0..stream.len() {
            assert_eq!(parse(&[&stream[..i], &stream[i..]]), whole, "split {i}");
        }
        let bytes: Vec<&[u8]> = stream.chunks(1).collect();
        assert_eq!(parse(&bytes), whole);
    }

    #[test]
    fn rejects_corrupt_frames() {
        let mut p = Parser::default();
        assert!(p.push(&[0, 0xff, 0xff, 0xff, 0xff], &mut vec![]).is_err());
        let mut p = Parser::default();
        assert!(p.push(&[1, 0, 0, 0, 3, 1, 2, 3], &mut vec![]).is_err());
        // A checkpoint frame without a cursor.
        let mut p = Parser::default();
        assert!(p.push(&[0, 0, 0, 0, 0], &mut vec![]).is_err());
    }
}
