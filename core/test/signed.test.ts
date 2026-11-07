/** Signed requests: the message the owner signs, and canonical addresses. */

import { describe, expect, test } from "bun:test";
import { Option } from "effect";
import { message, parseAddress } from "../src/signed.js";

const ENDPOINT = "https://api.example.test";

describe("signed requests", () => {
  test("the message is the documented plain text", () => {
    expect(
      message({ audience: ENDPOINT, address: "0xabc", method: "post", path: "/v1/keys", bodySha256: "00", timestamp: 5, nonce: "n" }),
    ).toBe("mirai.cloud request\naudience: https://api.example.test\naddress: 0xabc\nmethod: POST\npath: /v1/keys\nbody-sha256: 00\ntimestamp: 5\nnonce: n");
  });

  test("addresses are canonical, or refused", () => {
    expect(parseAddress("0x2")).toEqual(Option.some(`0x${"0".repeat(63)}2`));
    expect(Option.isNone(parseAddress("0xzz"))).toBe(true);
    expect(Option.isNone(parseAddress("2"))).toBe(true);
  });
});
