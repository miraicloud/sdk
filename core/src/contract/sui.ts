/**
 * The HTTP read API: Sui transactions, objects, checkpoints and validators
 * as JSON. Schemas for `../sui.ts`'s types (`satisfies` keeps each in step
 * with its type), and the `ReadApi` definition clients are derived from.
 *
 * Caching (every response says how long in `Cache-Control`):
 * - transactions, checkpoints by number or digest, objects at a version:
 *   immutable, cached for good;
 * - an object's latest state and `checkpoints/latest`: never cached, always live;
 * - validators: never cached, always live;
 * - not found (it may appear a moment later) and invalid requests: not cached.
 */

import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiMiddleware, HttpApiSecurity, OpenApi } from "effect/http-api";
import { HEADERS } from "../signed.js";
import * as S from "../sui.js";
import { Forbidden, Invalid, NotFound, QuotaExceeded, Unauthorized, Unavailable } from "./errors.js";

type Exactly<A> = Schema.Codec<A, A>;

const Int = Schema.Int;
const UnixMs = Schema.Int.annotate({ description: "Unix milliseconds" });
const Address = Schema.String.annotate({ description: "0x + 64 lowercase hex" });
const Digest = Schema.String.annotate({ description: "Base58" });
const MoveType = Schema.String.annotate({ description: "A full Move type, e.g. 0x…2::coin::Coin<0x…2::sui::SUI>" });
const Json = Schema.Json;
/** An open enum: the known values are documented; new ones may appear. */
const Open = (known: ReadonlyArray<string>) => Schema.String.annotate({ description: `One of ${known.join(", ")}; new values may appear.` });

export const Amount = Schema.String.check(Schema.isPattern(/^-?\d+$/)).annotate({
  identifier: "Amount",
  description: "An integer in the coin's base unit (MIST for SUI), as a decimal string",
}) satisfies Exactly<S.Amount>;

export const Bcs = Schema.Struct({ type: Schema.String, base64: Schema.String }).annotate({ identifier: "Bcs" }) satisfies Exactly<S.Bcs>;

export const ObjectRef = Schema.Struct({ objectId: Address, version: Int, digest: Digest }).annotate({ identifier: "ObjectRef" }) satisfies Exactly<S.ObjectRef>;

export const Owner = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("address"), address: Address }).annotate({ identifier: "OwnerAddress" }),
  Schema.Struct({ kind: Schema.Literal("object"), objectId: Address }).annotate({ identifier: "OwnerObject" }),
  Schema.Struct({ kind: Schema.Literal("shared"), initialVersion: Int }).annotate({ identifier: "OwnerShared" }),
  Schema.Struct({ kind: Schema.Literal("immutable") }).annotate({ identifier: "OwnerImmutable" }),
  Schema.Struct({ kind: Schema.Literal("consensusAddress"), address: Address, startVersion: Int }).annotate({ identifier: "OwnerConsensusAddress" }),
]).annotate({ identifier: "Owner" }) satisfies Exactly<S.Owner>;

export const GasCostSummary = Schema.Struct({
  computation: Amount,
  storage: Amount,
  storageRebate: Amount,
  nonRefundable: Amount,
  fee: Amount.annotate({ description: "computation + storage − storageRebate" }),
}).annotate({ identifier: "GasCostSummary" }) satisfies Exactly<S.GasCostSummary>;

// ---------------------------------------------------------------------------
// Transactions

const Argument = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("gas") }),
  Schema.Struct({ kind: Schema.Literal("input"), index: Int }),
  Schema.Struct({ kind: Schema.Literal("result"), index: Int, subresult: Schema.NullOr(Int) }),
]).annotate({ identifier: "Argument" }) satisfies Exactly<S.Argument>;

const Input = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("pure"), base64: Schema.String }).annotate({ identifier: "InputPure" }),
  Schema.Struct({ kind: Schema.Literal("object"), objectId: Address, version: Int, digest: Digest }).annotate({ identifier: "InputObject" }),
  Schema.Struct({ kind: Schema.Literal("shared"), objectId: Address, initialVersion: Int, access: Schema.Literals(["read", "write", "nonExclusiveWrite"]) }).annotate({
    identifier: "InputShared",
  }),
  Schema.Struct({ kind: Schema.Literal("receiving"), objectId: Address, version: Int, digest: Digest }).annotate({ identifier: "InputReceiving" }),
  Schema.Struct({
    kind: Schema.Literal("fundsWithdrawal"),
    amount: Schema.NullOr(Amount),
    coinType: MoveType,
    source: Open(["sender", "sponsor", "senderAllowance"]),
    funder: Schema.NullOr(Address),
  }).annotate({ identifier: "InputFundsWithdrawal" }),
]).annotate({ identifier: "Input" }) satisfies Exactly<S.Input>;

const Command = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("moveCall"),
    target: Schema.String.annotate({ description: "package::module::function" }),
    package: Address,
    module: Schema.String,
    function: Schema.String,
    typeArguments: Schema.Array(MoveType),
    arguments: Schema.Array(Argument),
  }).annotate({ identifier: "CommandMoveCall" }),
  Schema.Struct({ kind: Schema.Literal("transferObjects"), objects: Schema.Array(Argument), address: Argument }).annotate({ identifier: "CommandTransferObjects" }),
  Schema.Struct({ kind: Schema.Literal("splitCoins"), coin: Argument, amounts: Schema.Array(Argument) }).annotate({ identifier: "CommandSplitCoins" }),
  Schema.Struct({ kind: Schema.Literal("mergeCoins"), coin: Argument, sources: Schema.Array(Argument) }).annotate({ identifier: "CommandMergeCoins" }),
  Schema.Struct({ kind: Schema.Literal("publish"), moduleCount: Int, dependencies: Schema.Array(Address) }).annotate({ identifier: "CommandPublish" }),
  Schema.Struct({ kind: Schema.Literal("makeMoveVector"), elementType: Schema.NullOr(MoveType), elements: Schema.Array(Argument) }).annotate({
    identifier: "CommandMakeMoveVector",
  }),
  Schema.Struct({ kind: Schema.Literal("upgrade"), package: Address, moduleCount: Int, dependencies: Schema.Array(Address), ticket: Argument }).annotate({
    identifier: "CommandUpgrade",
  }),
]).annotate({ identifier: "Command" }) satisfies Exactly<S.Command>;

const MoveAbort = Schema.Struct({
  code: Amount.annotate({ description: "The u64 abort code" }),
  package: Address,
  module: Schema.String,
  function: Schema.NullOr(Schema.String),
  functionIndex: Int,
  instruction: Int,
  clever: Schema.NullOr(
    Schema.Struct({
      errorCode: Int,
      lineNumber: Int,
      constantName: Schema.NullOr(Schema.String),
      constantType: Schema.NullOr(Schema.String),
      rendered: Schema.NullOr(Schema.String),
    }),
  ),
}).annotate({ identifier: "MoveAbort" }) satisfies Exactly<S.MoveAbort>;

const ExecutionError = Schema.Struct({
  kind: Open(["moveAbort", "insufficientGas", "insufficientCoinBalance", "commandArgumentError", "…"]),
  message: Schema.String,
  command: Schema.NullOr(Int),
  abort: Schema.NullOr(MoveAbort),
  details: Schema.NullOr(Json),
}).annotate({ identifier: "ExecutionError" }) satisfies Exactly<S.ExecutionError>;

const Gas = Schema.Struct({
  payer: Address,
  sponsored: Schema.Boolean,
  budget: Amount,
  price: Amount.annotate({ description: "MIST per gas unit" }),
  payment: Schema.Array(ObjectRef),
  used: GasCostSummary,
  object: Schema.NullOr(ObjectRef),
}).annotate({ identifier: "Gas" }) satisfies Exactly<S.Gas>;

const Expiration = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("none") }),
  Schema.Struct({ kind: Schema.Literal("epoch"), epoch: Int }),
  Schema.Struct({ kind: Schema.Literal("other"), details: Json }),
]).annotate({ identifier: "Expiration" }) satisfies Exactly<S.Expiration>;

const ObjectChange = Schema.Struct({
  kind: Schema.Literals(S.OBJECT_CHANGE_KINDS),
  objectId: Address,
  type: Schema.NullOr(MoveType),
  version: Schema.NullOr(Int),
  digest: Schema.NullOr(Digest),
  owner: Schema.NullOr(Owner),
  previousVersion: Schema.NullOr(Int),
  previousDigest: Schema.NullOr(Digest),
  previousOwner: Schema.NullOr(Owner),
  accumulator: Schema.NullOr(Json),
}).annotate({ identifier: "ObjectChange" }) satisfies Exactly<S.ObjectChange>;

const BalanceChange = Schema.Struct({ address: Address, coinType: MoveType, amount: Amount }).annotate({ identifier: "BalanceChange" }) satisfies Exactly<S.BalanceChange>;

const Event = Schema.Struct({
  index: Int,
  type: MoveType,
  package: Address,
  module: Schema.String,
  sender: Address,
  fields: Json,
  bcs: Schema.optionalKey(Bcs),
}).annotate({ identifier: "Event" }) satisfies Exactly<S.Event>;

const UnchangedConsensusObject = Schema.Struct({
  kind: Open(["readOnlyRoot", "mutateConsensusStreamEnded", "readConsensusStreamEnded", "canceled", "perEpochConfig"]),
  objectId: Address,
  version: Schema.NullOr(Int),
  digest: Schema.NullOr(Digest),
  type: Schema.NullOr(MoveType),
}).annotate({ identifier: "UnchangedConsensusObject" }) satisfies Exactly<S.UnchangedConsensusObject>;

const Signature = Schema.Struct({
  scheme: Open(["ed25519", "secp256k1", "secp256r1", "multisig", "zklogin", "passkey"]),
  base64: Schema.String,
  publicKey: Schema.NullOr(Schema.String),
  details: Schema.NullOr(Json),
}).annotate({ identifier: "Signature" }) satisfies Exactly<S.Signature>;

// ---------------------------------------------------------------------------
// Objects

export const SuiObject = Schema.Struct({
  objectId: Address,
  version: Int,
  digest: Digest,
  type: Schema.String.annotate({ description: '"package", or the struct\'s type' }),
  owner: Owner,
  previousTransaction: Digest,
  storageRebate: Amount,
  fields: Schema.NullOr(Json),
  balance: Schema.NullOr(Amount),
  package: Schema.NullOr(
    Schema.Struct({
      originalId: Address,
      version: Int,
      modules: Schema.Array(Schema.String),
      linkage: Schema.Array(Schema.Struct({ originalId: Address, upgradedId: Address, upgradedVersion: Int })),
    }),
  ),
  display: Schema.optionalKey(Schema.Struct({ fields: Schema.NullOr(Schema.Record(Schema.String, Schema.String)), errors: Schema.NullOr(Json) })),
  checkpoint: Schema.optionalKey(Schema.Struct({ sequenceNumber: Int, timestamp: UnixMs })),
  bcs: Schema.optionalKey(Bcs),
}).annotate({ identifier: "Object" }) satisfies Exactly<S.SuiObject>;

export const Transaction = Schema.Struct({
  digest: Digest,
  checkpoint: Int,
  timestamp: UnixMs,
  epoch: Int,
  index: Schema.NullOr(Int),
  kind: Open(S.TRANSACTION_KINDS),
  sender: Address,
  status: Schema.Literals(["success", "failure"]),
  error: Schema.NullOr(ExecutionError),
  gas: Gas,
  expiration: Expiration,
  inputs: Schema.Array(Input),
  commands: Schema.Array(Command),
  system: Schema.NullOr(Json),
  objectChanges: Schema.Array(ObjectChange),
  balanceChanges: Schema.Array(BalanceChange),
  events: Schema.Array(Event),
  unchangedConsensusObjects: Schema.Array(UnchangedConsensusObject),
  dependencies: Schema.Array(Digest),
  effectsDigest: Digest,
  eventsDigest: Schema.NullOr(Digest),
  lamportVersion: Int,
  signatures: Schema.optionalKey(Schema.Array(Signature)),
  objects: Schema.optionalKey(Schema.Array(SuiObject)),
  bcs: Schema.optionalKey(Schema.Struct({ transaction: Bcs, effects: Bcs, events: Schema.NullOr(Bcs) })),
}).annotate({ identifier: "Transaction" }) satisfies Exactly<S.Transaction>;

// ---------------------------------------------------------------------------
// Checkpoints

const Commitment = Schema.Struct({ kind: Open(["ecmhLiveObjectSet", "checkpointArtifacts"]), digest: Digest }).annotate({
  identifier: "Commitment",
}) satisfies Exactly<S.Commitment>;

const CheckpointTransaction = Schema.Struct({
  digest: Digest,
  index: Int,
  effectsDigest: Digest,
  kind: Open(S.TRANSACTION_KINDS),
  sender: Address,
  status: Schema.Literals(["success", "failure"]),
  fee: Amount,
}).annotate({ identifier: "CheckpointTransaction" }) satisfies Exactly<S.CheckpointTransaction>;

export const Checkpoint = Schema.Struct({
  sequenceNumber: Int,
  digest: Digest,
  epoch: Int,
  timestamp: UnixMs,
  previousDigest: Schema.NullOr(Digest),
  contentsDigest: Digest,
  totalNetworkTransactions: Int,
  transactionCount: Int,
  gas: GasCostSummary,
  epochRollingGas: GasCostSummary,
  prev: Schema.NullOr(Int),
  next: Int,
  endOfEpoch: Schema.NullOr(
    Schema.Struct({
      nextEpochProtocolVersion: Int,
      nextEpochCommittee: Schema.Array(Schema.Struct({ publicKey: Schema.String, weight: Int })),
      epochCommitments: Schema.Array(Commitment),
    }),
  ),
  commitments: Schema.Array(Commitment),
  transactions: Schema.Array(CheckpointTransaction),
  signature: Schema.optionalKey(Schema.Struct({ epoch: Int, base64: Schema.String, bitmap: Schema.String })),
  bcs: Schema.optionalKey(Schema.Struct({ summary: Bcs, contents: Bcs })),
}).annotate({ identifier: "Checkpoint" }) satisfies Exactly<S.Checkpoint>;

// ---------------------------------------------------------------------------
// Validators

const nullableAddresses = { network: Schema.NullOr(Schema.String), p2p: Schema.NullOr(Schema.String), primary: Schema.NullOr(Schema.String), worker: Schema.NullOr(Schema.String) };
const ApyWindow = Schema.Struct({ fromEpoch: Int, toEpoch: Int }).annotate({ identifier: "ApyWindow" }) satisfies Exactly<S.ApyWindow>;

const validatorFields = {
  rank: Int,
  address: Address,
  name: Schema.String,
  description: Schema.String,
  imageUrl: Schema.String,
  projectUrl: Schema.String,
  stakingPoolId: Address,
  operationCapId: Address,
  votingPower: Int.annotate({ description: "Out of 10,000" }),
  stake: Amount,
  stakeShare: Schema.Number,
  commissionRate: Int.annotate({ description: "Basis points" }),
  gasPrice: Amount,
  nextEpoch: Schema.Struct({ stake: Amount, gasPrice: Amount, commissionRate: Int }),
  pool: Schema.Struct({
    activationEpoch: Schema.NullOr(Int),
    deactivationEpoch: Schema.NullOr(Int),
    rewardsPool: Amount,
    poolTokenBalance: Amount,
    exchangeRate: Schema.Number,
    pendingStake: Amount,
    pendingWithdraw: Amount,
    pendingPoolTokenWithdraw: Amount,
  }),
  apy: Schema.NullOr(Schema.Number),
  atRiskEpochs: Schema.NullOr(Int),
  pendingRemoval: Schema.Boolean,
  reportedBy: Schema.Array(Address),
  keys: Schema.Struct({ protocol: Schema.String, network: Schema.String, worker: Schema.String, proofOfPossession: Schema.String }),
  nextEpochKeys: Schema.Struct({ protocol: Schema.NullOr(Schema.String), network: Schema.NullOr(Schema.String), worker: Schema.NullOr(Schema.String) }),
  addresses: Schema.Struct({ network: Schema.String, p2p: Schema.String, primary: Schema.String, worker: Schema.String }),
  nextEpochAddresses: Schema.Struct(nullableAddresses),
};

export const Validator = Schema.Struct(validatorFields).annotate({ identifier: "Validator" }) satisfies Exactly<S.Validator>;

export const ValidatorDetail = Schema.Struct({ ...validatorFields, epoch: Int, apyWindow: Schema.NullOr(ApyWindow) }).annotate({
  identifier: "ValidatorDetail",
}) satisfies Exactly<S.ValidatorDetail>;

export const ValidatorSet = Schema.Struct({
  epoch: Int,
  epochStart: UnixMs,
  protocolVersion: Int,
  referenceGasPrice: Amount,
  totalStake: Amount,
  count: Int,
  nakamotoCoefficient: Int,
  thresholds: Schema.Struct({ minJoiningStake: Amount, lowStake: Amount, veryLowStake: Amount, lowStakeGracePeriodEpochs: Int, maxValidators: Int }),
  apyWindow: Schema.NullOr(ApyWindow),
  validators: Schema.Array(Validator),
}).annotate({ identifier: "ValidatorSet" }) satisfies Exactly<S.ValidatorSet>;

// ---------------------------------------------------------------------------
// The API

/**
 * How data-plane calls authenticate: the account owner's signature, or an
 * API key with `rpc:read`. The gateway checks it before the request reaches
 * the read service, so nothing is provided to handlers.
 */
export class ReadAuth extends HttpApiMiddleware.Service<ReadAuth, { provides: never; requires: never }>()("mirai/ReadAuth", {
  requiredForClient: true,
  security: {
    signature: HttpApiSecurity.apiKey({ key: HEADERS.signature, in: "header" }),
    bearer: HttpApiSecurity.bearer,
  },
  error: [Unauthorized, Forbidden, QuotaExceeded],
}) {}

const include = (names: ReadonlyArray<string>) =>
  Schema.optionalKey(Schema.String.annotate({ description: `Comma-separated expansions: ${names.join(", ")}` }));
const errors = [Invalid, NotFound, Unavailable];

export class TransactionsGroup extends HttpApiGroup.make("transactions")
  .add(
    HttpApiEndpoint.get("get", "/:digest", {
      params: { digest: Digest },
      query: { include: include(S.TRANSACTION_INCLUDES) },
      success: Transaction,
      error: errors,
    }),
  )
  .middleware(ReadAuth)
  .prefix("/v1/transactions")
  .annotateMerge(OpenApi.annotations({ title: "Transactions", description: "A finalized transaction, by digest. Immutable: cached for good." })) {}

export class ObjectsGroup extends HttpApiGroup.make("objects")
  .add(
    HttpApiEndpoint.get("get", "/:objectId", {
      params: { objectId: Address },
      query: {
        version: Schema.optionalKey(Schema.String.annotate({ description: "A version (decimal); absent: the latest" })),
        include: include(S.OBJECT_INCLUDES),
      },
      success: SuiObject,
      error: errors,
    }),
  )
  .middleware(ReadAuth)
  .prefix("/v1/objects")
  .annotateMerge(OpenApi.annotations({ title: "Objects", description: "An object's latest state (live, never cached), or the state at a version (immutable)." })) {}

export class CheckpointsGroup extends HttpApiGroup.make("checkpoints")
  .add(
    HttpApiEndpoint.get("get", "/:checkpoint", {
      params: { checkpoint: Schema.String.annotate({ description: "A sequence number, a digest, or `latest`" }) },
      query: { include: include(S.CHECKPOINT_INCLUDES) },
      success: Checkpoint,
      error: errors,
    }),
  )
  .middleware(ReadAuth)
  .prefix("/v1/checkpoints")
  .annotateMerge(OpenApi.annotations({ title: "Checkpoints", description: "A checkpoint and its transactions. Immutable, except `latest` (live, never cached)." })) {}

export class ValidatorsGroup extends HttpApiGroup.make("validators")
  .add(
    HttpApiEndpoint.get("list", "/", { success: ValidatorSet, error: errors }),
    HttpApiEndpoint.get("get", "/:validatorId", {
      params: { validatorId: Address.annotate({ description: "The validator's address or its staking pool id" }) },
      success: ValidatorDetail,
      error: errors,
    }),
  )
  .middleware(ReadAuth)
  .prefix("/v1/validators")
  .annotateMerge(OpenApi.annotations({ title: "Validators", description: "The active validator set (current epoch). Live, never cached." })) {}

export class ReadApi extends HttpApi.make("mirai-read")
  .add(TransactionsGroup)
  .add(ObjectsGroup)
  .add(CheckpointsGroup)
  .add(ValidatorsGroup)
  .annotateMerge(OpenApi.annotations({ title: "mirai.cloud read API" })) {}

