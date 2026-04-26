# NNS Path Y integration (this fork)

[NNS / nns-vesl](https://github.com/nocktoshi/nns-vesl) in **Path Y** mode indexes `.nock` registrations by scanning Nockchain blocks and decoding **`note_data` on transaction outputs**. Rust reference: `nocktoshi/nns-vesl` → `src/chain_follower.rs`, `src/claim_note.rs`.

## Branches in this fork

| Branch | Purpose for NNS |
|--------|------------------|
| **`feat/grpc-note-data`** | Exposes output **`note_data`** on public gRPC block/tx responses so the NNS follower can find `nns/v1/claim` payloads. **Use this (or equivalent) for chain-mode NNS.** |
| **`feat/memo-signed`** | Implements **[upstream PR #85](https://github.com/nockchain/nockchain/pull/85)** — `create-tx --memo-data` so the wallet CLI can attach opaque bytes to outputs. Overlaps with gRPC work; merging the two branches may require manual conflict resolution in `nockchain-wallet` and wallet Hoon. |

## Wallet / CLI gap

Structured NNS claims require **multiple NoteData keys** and a **JAM**’d `nns/v1/claim` tuple, not a single memo string. Until wallet `RecipientSpec` / `$order` carry optional structured note data per PR review on #85, use **`ClaimNoteV1::to_note_data`** in nns-vesl as the wire spec, or extend `create-tx` accordingly.

Canonical doc (stays with NNS):  
https://github.com/nocktoshi/nns-vesl/blob/main/docs/claim-note-wallet-support.md  
