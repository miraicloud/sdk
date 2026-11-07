/**
 * The errors' wire format: each schema encodes an error class (`../errors.ts`)
 * as `{ "_tag": …, "message": … }` with its HTTP status, and decodes back to
 * that class. So handlers fail with those classes and clients get them back.
 */

import { Schema, SchemaTransformation } from "effect";
import * as E from "../errors.js";

type ErrorClass<Tag extends keyof typeof E.STATUS> = new (args: { readonly message: string }) => { readonly _tag: Tag; readonly message: string };

const codec = <Tag extends keyof typeof E.STATUS, C extends ErrorClass<Tag>>(tag: Tag, Class: C) =>
  Schema.Struct({ _tag: Schema.Literal(tag), message: Schema.String })
    .annotate({ identifier: tag })
    .pipe(
      Schema.decodeTo(
        Schema.instanceOf(Class),
        SchemaTransformation.transform({
          decode: ({ message }) => new Class({ message }) as InstanceType<C>,
          encode: (e) => ({ _tag: tag, message: e.message }) as const,
        }),
      ),
    )
    .annotate({ httpApiStatus: E.STATUS[tag] });

export const Unauthorized = codec("Unauthorized", E.Unauthorized);
export const Forbidden = codec("Forbidden", E.Forbidden);
export const NotFound = codec("NotFound", E.NotFound);
export const Invalid = codec("Invalid", E.Invalid);
export const QuotaExceeded = codec("QuotaExceeded", E.QuotaExceeded);
export const RateLimited = codec("RateLimited", E.RateLimited);
export const Unavailable = codec("Unavailable", E.Unavailable);
