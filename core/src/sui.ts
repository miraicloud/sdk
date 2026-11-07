/**
 * Sui data as the HTTP read API serves it: transactions, objects,
 * checkpoints and validators.
 *
 * Conventions:
 * - **Numbers vs strings.** Ordinals and clocks are JSON numbers: checkpoint
 *   sequence numbers, epochs, versions, indices, protocol versions,
 *   timestamps (unix ms), counts, voting power, commission (basis points).
 *   Anything denominated in a coin (balances, stake, gas costs, gas prices,
 *   amounts) or able to exceed 2^53 (abort codes) is a decimal string
 *   (`Amount`).
 * - **Ids** are `0x` + 64 lowercase hex; digests are base58; Move types are
 *   full canonical strings, as the chain writes them.
 * - **Absent vs null.** Data that doesn't exist is `null`; an opt-in
 *   expansion (`?include=`) that wasn't asked for is left out.
 * - **Enums** are lowerCamel strings. Those that mirror a growing Sui enum
 *   (transaction kind, execution error kind, signature scheme, …) are open:
 *   a new Sui release may add values, so treat unknown ones gracefully.
 * - **Move values** (`fields`) are the chain's JSON rendering: integers up
 *   to u32 as numbers, u64 and wider as strings, addresses as hex,
 *   structs as objects.
 *
 * Plain types and constants: importing this never loads Effect's `Schema`
 * (the schemas are in `contract`).
 */

/** An integer in a coin's base unit (MIST for SUI), as a decimal string; may be negative. */
export type Amount = string;

/** An `Amount` as a bigint, for arithmetic. */
export const toBigInt = (amount: Amount): bigint => BigInt(amount);

export type Json = null | number | boolean | string | JsonArray | JsonObject;
export interface JsonArray extends ReadonlyArray<Json> {}
export interface JsonObject {
  readonly [key: string]: Json;
}

/** Raw BCS bytes and the Move type or structure they encode. */
export interface Bcs {
  readonly type: string;
  readonly base64: string;
}

export interface ObjectRef {
  readonly objectId: string;
  readonly version: number;
  readonly digest: string;
}

export const OWNER_KINDS = ["address", "object", "shared", "immutable", "consensusAddress"] as const;
export type OwnerKind = (typeof OWNER_KINDS)[number];

/** Who owns an object. */
export type Owner =
  | { readonly kind: "address"; readonly address: string }
  /** Owned by another object (a dynamic field, a wrapped child). */
  | { readonly kind: "object"; readonly objectId: string }
  | { readonly kind: "shared"; readonly initialVersion: number }
  | { readonly kind: "immutable" }
  | { readonly kind: "consensusAddress"; readonly address: string; readonly startVersion: number };

/** Gas costs, in MIST. */
export interface GasCostSummary {
  readonly computation: Amount;
  readonly storage: Amount;
  readonly storageRebate: Amount;
  readonly nonRefundable: Amount;
  /** What the payer lost: computation + storage − storage rebate. */
  readonly fee: Amount;
}

// ---------------------------------------------------------------------------
// Transactions

export const TRANSACTION_KINDS = [
  "programmable",
  "programmableSystem",
  "changeEpoch",
  "genesis",
  "consensusCommitPrologue",
  "authenticatorStateUpdate",
  "endOfEpoch",
  "randomnessStateUpdate",
] as const;
/** Open: new kinds may appear. */
export type TransactionKind = (typeof TRANSACTION_KINDS)[number] | (string & {});

export const TRANSACTION_INCLUDES = ["signatures", "objects", "bcs"] as const;
export type TransactionInclude = (typeof TRANSACTION_INCLUDES)[number];

export type TransactionStatus = "success" | "failure";

export interface Transaction {
  readonly digest: string;
  readonly checkpoint: number;
  /** Unix ms of its checkpoint. */
  readonly timestamp: number;
  readonly epoch: number;
  /** Its position in the checkpoint; null when not reported. */
  readonly index: number | null;
  readonly kind: TransactionKind;
  readonly sender: string;
  readonly status: TransactionStatus;
  readonly error: ExecutionError | null;
  readonly gas: Gas;
  readonly expiration: Expiration;
  /** A programmable transaction's inputs; empty for other kinds. */
  readonly inputs: ReadonlyArray<Input>;
  /** A programmable transaction's commands; empty for other kinds. */
  readonly commands: ReadonlyArray<Command>;
  /** Other kinds: the kind's payload (bytes as base64, package modules left out); null for programmable ones. */
  readonly system: Json | null;
  readonly objectChanges: ReadonlyArray<ObjectChange>;
  readonly balanceChanges: ReadonlyArray<BalanceChange>;
  readonly events: ReadonlyArray<Event>;
  readonly unchangedConsensusObjects: ReadonlyArray<UnchangedConsensusObject>;
  /** Digests of the transactions this one depends on. */
  readonly dependencies: ReadonlyArray<string>;
  readonly effectsDigest: string;
  readonly eventsDigest: string | null;
  readonly lamportVersion: number;
  /** `?include=signatures` */
  readonly signatures?: ReadonlyArray<Signature>;
  /** `?include=objects`: the objects it read and wrote, in full. */
  readonly objects?: ReadonlyArray<SuiObject>;
  /** `?include=bcs` */
  readonly bcs?: { readonly transaction: Bcs; readonly effects: Bcs; readonly events: Bcs | null };
}

export interface Gas {
  /** Who paid; differs from the sender when sponsored. */
  readonly payer: string;
  readonly sponsored: boolean;
  /** MIST. */
  readonly budget: Amount;
  /** MIST per gas unit. */
  readonly price: Amount;
  /** The coins paying for gas; may be empty (paid from an address balance). */
  readonly payment: ReadonlyArray<ObjectRef>;
  readonly used: GasCostSummary;
  /** The gas coin as the transaction left it; null when no coin was written. */
  readonly object: ObjectRef | null;
}

export type Expiration = { readonly kind: "none" } | { readonly kind: "epoch"; readonly epoch: number } | { readonly kind: "other"; readonly details: Json };

export interface ExecutionError {
  /** e.g. `moveAbort`, `insufficientGas`. Open: new kinds may appear. */
  readonly kind: string;
  /** The chain's description. */
  readonly message: string;
  /** The failing command's index. */
  readonly command: number | null;
  readonly abort: MoveAbort | null;
  /** The details of other kinds (size, argument, upgrade errors, …). */
  readonly details: Json | null;
}

export interface MoveAbort {
  /** The u64 abort code. */
  readonly code: Amount;
  readonly package: string;
  readonly module: string;
  readonly function: string | null;
  readonly functionIndex: number;
  readonly instruction: number;
  readonly clever: {
    readonly errorCode: number;
    readonly lineNumber: number;
    readonly constantName: string | null;
    readonly constantType: string | null;
    readonly rendered: string | null;
  } | null;
}

export type Argument =
  | { readonly kind: "gas" }
  | { readonly kind: "input"; readonly index: number }
  | { readonly kind: "result"; readonly index: number; readonly subresult: number | null };

export type SharedAccess = "read" | "write" | "nonExclusiveWrite";

export type Input =
  /** Pure bytes: their type depends on the function they're passed to. */
  | { readonly kind: "pure"; readonly base64: string }
  | { readonly kind: "object"; readonly objectId: string; readonly version: number; readonly digest: string }
  | { readonly kind: "shared"; readonly objectId: string; readonly initialVersion: number; readonly access: SharedAccess }
  | { readonly kind: "receiving"; readonly objectId: string; readonly version: number; readonly digest: string }
  | {
      readonly kind: "fundsWithdrawal";
      readonly amount: Amount | null;
      readonly coinType: string;
      /** `sender`, `sponsor`, `senderAllowance`; open. */
      readonly source: string;
      readonly funder: string | null;
    };

export type Command =
  | {
      readonly kind: "moveCall";
      /** `package::module::function` */
      readonly target: string;
      readonly package: string;
      readonly module: string;
      readonly function: string;
      readonly typeArguments: ReadonlyArray<string>;
      readonly arguments: ReadonlyArray<Argument>;
    }
  | { readonly kind: "transferObjects"; readonly objects: ReadonlyArray<Argument>; readonly address: Argument }
  | { readonly kind: "splitCoins"; readonly coin: Argument; readonly amounts: ReadonlyArray<Argument> }
  | { readonly kind: "mergeCoins"; readonly coin: Argument; readonly sources: ReadonlyArray<Argument> }
  | { readonly kind: "publish"; readonly moduleCount: number; readonly dependencies: ReadonlyArray<string> }
  | { readonly kind: "makeMoveVector"; readonly elementType: string | null; readonly elements: ReadonlyArray<Argument> }
  | { readonly kind: "upgrade"; readonly package: string; readonly moduleCount: number; readonly dependencies: ReadonlyArray<string>; readonly ticket: Argument };

export const OBJECT_CHANGE_KINDS = [
  "created",
  "mutated",
  "deleted",
  "wrapped",
  "unwrapped",
  "unwrappedThenDeleted",
  "createdThenDeleted",
  "published",
  "accumulator",
] as const;
export type ObjectChangeKind = (typeof OBJECT_CHANGE_KINDS)[number];

/** What a transaction did to an object. A transfer is a `mutated` row whose `owner` differs from `previousOwner`. */
export interface ObjectChange {
  readonly kind: ObjectChangeKind;
  readonly objectId: string;
  readonly type: string | null;
  /** After the transaction; null when the object no longer exists. */
  readonly version: number | null;
  readonly digest: string | null;
  readonly owner: Owner | null;
  /** Before the transaction; null when it didn't exist. */
  readonly previousVersion: number | null;
  readonly previousDigest: string | null;
  readonly previousOwner: Owner | null;
  /** Address balance writes (`accumulator` rows only). */
  readonly accumulator: Json | null;
}

/** A coin balance change; `amount` is signed. */
export interface BalanceChange {
  readonly address: string;
  readonly coinType: string;
  readonly amount: Amount;
}

export interface Event {
  readonly index: number;
  readonly type: string;
  /** The package and module that emitted it (may differ from the type's). */
  readonly package: string;
  readonly module: string;
  readonly sender: string;
  readonly fields: Json;
  /** `?include=bcs` */
  readonly bcs?: Bcs;
}

export interface UnchangedConsensusObject {
  /** `readOnlyRoot`, `mutateConsensusStreamEnded`, `readConsensusStreamEnded`, `canceled`, `perEpochConfig`; open. */
  readonly kind: string;
  readonly objectId: string;
  readonly version: number | null;
  readonly digest: string | null;
  readonly type: string | null;
}

export interface Signature {
  /** `ed25519`, `secp256k1`, `secp256r1`, `multisig`, `zklogin`, `passkey`; open. */
  readonly scheme: string;
  /** The serialized signature (flag, signature, public key), base64. */
  readonly base64: string;
  /** Simple schemes: the public key, base64. */
  readonly publicKey: string | null;
  /** Multisig committee, zkLogin issuer and max epoch, passkey client data. */
  readonly details: Json | null;
}

// ---------------------------------------------------------------------------
// Objects

export const OBJECT_INCLUDES = ["display", "bcs", "checkpoint"] as const;
export type ObjectInclude = (typeof OBJECT_INCLUDES)[number];

export interface PackageSummary {
  readonly originalId: string;
  readonly version: number;
  readonly modules: ReadonlyArray<string>;
  readonly linkage: ReadonlyArray<{ readonly originalId: string; readonly upgradedId: string; readonly upgradedVersion: number }>;
}

export interface SuiObject {
  readonly objectId: string;
  readonly version: number;
  readonly digest: string;
  /** `package`, or the struct's type. */
  readonly type: string;
  readonly owner: Owner;
  readonly previousTransaction: string;
  /** MIST. */
  readonly storageRebate: Amount;
  /** The Move struct's fields; null for packages. */
  readonly fields: Json | null;
  /** `Coin<T>`: its balance. */
  readonly balance: Amount | null;
  /** Packages: their modules and linkage (no bytecode). */
  readonly package: PackageSummary | null;
  /** `?include=display`: rendered from the type's current Display template. */
  readonly display?: { readonly fields: { readonly [key: string]: string } | null; readonly errors: Json | null };
  /** `?include=checkpoint`: where `previousTransaction` landed. */
  readonly checkpoint?: { readonly sequenceNumber: number; readonly timestamp: number };
  /** `?include=bcs` */
  readonly bcs?: Bcs;
}

// ---------------------------------------------------------------------------
// Checkpoints

export const CHECKPOINT_INCLUDES = ["signature", "bcs"] as const;
export type CheckpointInclude = (typeof CHECKPOINT_INCLUDES)[number];

export interface Commitment {
  /** `ecmhLiveObjectSet`, `checkpointArtifacts`; open. */
  readonly kind: string;
  readonly digest: string;
}

export interface CheckpointTransaction {
  readonly digest: string;
  readonly index: number;
  readonly effectsDigest: string;
  readonly kind: TransactionKind;
  readonly sender: string;
  readonly status: TransactionStatus;
  /** MIST. */
  readonly fee: Amount;
}

export interface Checkpoint {
  readonly sequenceNumber: number;
  readonly digest: string;
  readonly epoch: number;
  readonly timestamp: number;
  /** Null at genesis. */
  readonly previousDigest: string | null;
  readonly contentsDigest: string;
  readonly totalNetworkTransactions: number;
  readonly transactionCount: number;
  /** This checkpoint's transactions' gas. */
  readonly gas: GasCostSummary;
  /** Cumulative since the epoch started. */
  readonly epochRollingGas: GasCostSummary;
  /** Null at 0. */
  readonly prev: number | null;
  /** Always set: 404 until it exists. */
  readonly next: number;
  readonly endOfEpoch: {
    readonly nextEpochProtocolVersion: number;
    readonly nextEpochCommittee: ReadonlyArray<{ readonly publicKey: string; readonly weight: number }>;
    readonly epochCommitments: ReadonlyArray<Commitment>;
  } | null;
  readonly commitments: ReadonlyArray<Commitment>;
  /** Every transaction, system ones included, in order. */
  readonly transactions: ReadonlyArray<CheckpointTransaction>;
  /** `?include=signature`: the validators' aggregated signature. */
  readonly signature?: { readonly epoch: number; readonly base64: string; readonly bitmap: string };
  /** `?include=bcs` */
  readonly bcs?: { readonly summary: Bcs; readonly contents: Bcs };
}

// ---------------------------------------------------------------------------
// Validators

export interface StakingPool {
  readonly activationEpoch: number | null;
  readonly deactivationEpoch: number | null;
  readonly rewardsPool: Amount;
  readonly poolTokenBalance: Amount;
  /** SUI per pool token: stake / pool token balance. */
  readonly exchangeRate: number;
  readonly pendingStake: Amount;
  readonly pendingWithdraw: Amount;
  readonly pendingPoolTokenWithdraw: Amount;
}

export interface Validator {
  /** 1-based, by stake. */
  readonly rank: number;
  readonly address: string;
  readonly name: string;
  readonly description: string;
  readonly imageUrl: string;
  readonly projectUrl: string;
  readonly stakingPoolId: string;
  readonly operationCapId: string;
  /** Out of 10,000. */
  readonly votingPower: number;
  /** MIST. */
  readonly stake: Amount;
  /** Stake as a fraction of the total. */
  readonly stakeShare: number;
  /** Basis points (800 = 8%). */
  readonly commissionRate: number;
  /** MIST per gas unit: this validator's quote for the epoch. */
  readonly gasPrice: Amount;
  readonly nextEpoch: { readonly stake: Amount; readonly gasPrice: Amount; readonly commissionRate: number };
  readonly pool: StakingPool;
  /** Annualized staking yield after commission, as a fraction, over `apyWindow`; null if unavailable. */
  readonly apy: number | null;
  /** Epochs below the low-stake threshold; null when not at risk. */
  readonly atRiskEpochs: number | null;
  readonly pendingRemoval: boolean;
  /** Validators that reported this one. */
  readonly reportedBy: ReadonlyArray<string>;
  /** Base64. */
  readonly keys: { readonly protocol: string; readonly network: string; readonly worker: string; readonly proofOfPossession: string };
  readonly nextEpochKeys: { readonly protocol: string | null; readonly network: string | null; readonly worker: string | null };
  /** Multiaddrs. */
  readonly addresses: { readonly network: string; readonly p2p: string; readonly primary: string; readonly worker: string };
  readonly nextEpochAddresses: { readonly network: string | null; readonly p2p: string | null; readonly primary: string | null; readonly worker: string | null };
}

/** The epochs every validator's `apy` is measured over. */
export interface ApyWindow {
  readonly fromEpoch: number;
  readonly toEpoch: number;
}

export interface ValidatorSet {
  readonly epoch: number;
  /** Unix ms. */
  readonly epochStart: number;
  readonly protocolVersion: number;
  readonly referenceGasPrice: Amount;
  readonly totalStake: Amount;
  readonly count: number;
  /** The fewest validators (by voting power) whose combined power exceeds a third: enough to halt the chain. */
  readonly nakamotoCoefficient: number;
  readonly thresholds: {
    readonly minJoiningStake: Amount;
    readonly lowStake: Amount;
    readonly veryLowStake: Amount;
    readonly lowStakeGracePeriodEpochs: number;
    readonly maxValidators: number;
  };
  readonly apyWindow: ApyWindow | null;
  /** By stake, largest first. */
  readonly validators: ReadonlyArray<Validator>;
}

/** One validator, with the epoch and APY window it's relative to. */
export interface ValidatorDetail extends Validator {
  readonly epoch: number;
  readonly apyWindow: ApyWindow | null;
}
