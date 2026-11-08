/**
 * The checkpoint stream's wire format (after zstd decoding): frames of a 5-byte header
 * (`kind: u8`, `len: u32` big-endian) and `len` bytes of body.
 *
 * | kind | body |
 * |------|------|
 * | `0` checkpoint | protobuf `sui.rpc.v2.SubscribeCheckpointsResponse` |
 * | `1` gap | `from`, `to` (inclusive) as u64 big-endian |
 * | `2` keepalive | empty |
 *
 * Unknown kinds are skipped.
 */

import { Data } from "effect";

/**
 * The media type of the checkpoint stream, sent as `Accept` (and the
 * response's `Content-Type`). The service names it this way today; the
 * frames are versioned by it.
 */
export const CONTENT_TYPE = "application/vnd.miraicloud.checkpoints.v1+proto";

const HEADER_LEN = 5;
const CHECKPOINT = 0;
const GAP = 1;
/** A frame larger than this is a corrupt stream (the largest Mainnet checkpoints are ~20 MB). */
const MAX_FRAME = 64 * 1024 * 1024;

export type Frame = Data.TaggedEnum<{
  /** `cursor` (the sequence number) and the encoded `Checkpoint` (empty if the response had none). */
  Checkpoint: { readonly seq: bigint; readonly checkpoint: Uint8Array };
  Gap: { readonly from: bigint; readonly to: bigint };
  /** A keepalive or an unknown kind. */
  Other: {};
}>;
export const Frame = Data.taggedEnum<Frame>();

/**
 * Incremental frame parser. A frame within one chunk is a view of that
 * chunk (zero-copy); one spread over chunks is joined once.
 */
export class FrameParser {
  private chunks: Uint8Array[] = [];
  /** Offset of the unread part of `chunks[0]`. */
  private offset = 0;
  private length = 0;

  /** Feed decompressed bytes; returns the frames completed by them. */
  push(chunk: Uint8Array): Frame[] {
    const out: Frame[] = [];
    if (chunk.length > 0) {
      this.chunks.push(chunk);
      this.length += chunk.length;
    }
    while (this.length >= HEADER_LEN) {
      const header = this.peek(HEADER_LEN);
      const len = new DataView(header.buffer, header.byteOffset, HEADER_LEN).getUint32(1);
      if (len > MAX_FRAME) throw new Error(`frame of ${len} bytes`);
      if (this.length < HEADER_LEN + len) break;
      const kind = header[0]!;
      this.skip(HEADER_LEN);
      const body = this.take(len);
      out.push(parseFrame(kind, body));
    }
    return out;
  }

  /** The next `n` bytes without consuming them. */
  private peek(n: number): Uint8Array {
    const first = this.chunks[0]!;
    if (first.length - this.offset >= n) return first.subarray(this.offset, this.offset + n);
    const out = new Uint8Array(n);
    let filled = 0;
    let off = this.offset;
    for (const c of this.chunks) {
      const part = c.subarray(off, off + (n - filled));
      out.set(part, filled);
      filled += part.length;
      off = 0;
      if (filled === n) break;
    }
    return out;
  }

  private skip(n: number): void {
    this.take(n);
  }

  /** Consume the next `n` bytes (`n` ≤ `length`). */
  private take(n: number): Uint8Array {
    this.length -= n;
    const first = this.chunks[0];
    if (n === 0 || !first) return new Uint8Array(0);
    if (first.length - this.offset >= n) {
      const view = first.subarray(this.offset, this.offset + n);
      this.offset += n;
      if (this.offset === first.length) {
        this.chunks.shift();
        this.offset = 0;
      }
      return view;
    }
    const out = new Uint8Array(n);
    let filled = 0;
    while (filled < n) {
      const c = this.chunks[0]!;
      const part = c.subarray(this.offset, this.offset + (n - filled));
      out.set(part, filled);
      filled += part.length;
      this.offset += part.length;
      if (this.offset === c.length) {
        this.chunks.shift();
        this.offset = 0;
      }
    }
    return out;
  }
}

const parseFrame = (kind: number, body: Uint8Array): Frame => {
  switch (kind) {
    case CHECKPOINT: {
      const { cursor, checkpoint } = splitResponse(body);
      return Frame.Checkpoint({ seq: cursor, checkpoint });
    }
    case GAP: {
      if (body.length !== 16) throw new Error(`gap frame of ${body.length} bytes`);
      const view = new DataView(body.buffer, body.byteOffset, 16);
      return Frame.Gap({ from: view.getBigUint64(0), to: view.getBigUint64(8) });
    }
    default:
      return Frame.Other();
  }
};

/** A protobuf varint at `pos`: [value, next position]. */
const varint = (buf: Uint8Array, pos: number): [bigint, number] => {
  let value = 0n;
  let shift = 0n;
  for (let i = 0; i < 10; i++) {
    if (pos >= buf.length) throw new Error("truncated varint");
    const b = buf[pos++]!;
    value |= BigInt(b & 0x7f) << shift;
    if (b < 0x80) return [value, pos];
    shift += 7n;
  }
  throw new Error("varint too long");
};

/**
 * `cursor` and the encoded `checkpoint` (fields 1 and 2) of a
 * `SubscribeCheckpointsResponse`, without decoding the checkpoint.
 */
export const splitResponse = (body: Uint8Array): { cursor: bigint; checkpoint: Uint8Array } => {
  let cursor: bigint | undefined;
  let checkpoint = body.subarray(0, 0);
  let pos = 0;
  while (pos < body.length) {
    let key: bigint;
    [key, pos] = varint(body, pos);
    const field = key >> 3n;
    const wire = Number(key & 7n);
    if (field === 1n && wire === 0) {
      [cursor, pos] = varint(body, pos);
    } else if (wire === 0) {
      [, pos] = varint(body, pos);
    } else if (wire === 2) {
      let len: bigint;
      [len, pos] = varint(body, pos);
      const end = pos + Number(len);
      if (end > body.length) throw new Error("truncated field");
      if (field === 2n) checkpoint = body.subarray(pos, end);
      pos = end;
    } else if (wire === 1) {
      pos += 8;
    } else if (wire === 5) {
      pos += 4;
    } else {
      throw new Error(`unsupported wire type ${wire}`);
    }
  }
  if (pos > body.length) throw new Error("truncated field");
  if (cursor === undefined) throw new Error("checkpoint frame without cursor");
  return { cursor, checkpoint };
};
