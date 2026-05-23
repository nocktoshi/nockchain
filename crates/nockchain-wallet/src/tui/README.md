# Wallet TUI

Interactive full-screen terminal UI for `nockchain-wallet`, built with [ratatui](https://github.com/ratatui-org/ratatui) and [crossterm](https://github.com/crossterm-rs/crossterm). The TUI exposes the same wallet kernel commands as the CLI, plus a session-scoped JSON HTTP API for automation.

## Launch

```bash
nockchain-wallet tui
```

`main.rs` detects `Commands::Tui` and calls `tui::run`, passing the initialized `Wallet`, optional synced planner snapshot, and wallet data directory. Non-TUI CLI paths are unchanged.

## Architecture

The TUI is intentionally isolated from dispatch and the Hoon kernel. Wallet work always goes through `crate::dispatch::execute_wallet_command`; the TUI never pokes the kernel directly.

```
┌─────────────────────────────────────────────────────────────┐
│  event_loop.rs   async event loop (keys, ticks, jobs)       │
│    ├─ handlers/  keyboard + paste routing per Screen        │
│    ├─ components/ ratatui widgets (draw only)             │
│    └─ store/       UIStore + UiAction → apply_ui_action     │
├─────────────────────────────────────────────────────────────┤
│  command_runner.rs   TuiRuntime, background wallet jobs     │
│    └─ dispatch::execute_wallet_command                      │
├─────────────────────────────────────────────────────────────┤
│  view/ + wallet_outcome.rs   structured WalletEvent output  │
├─────────────────────────────────────────────────────────────┤
│  wallet_api/     JSON HTTP API (same runtime, same wallet)  │
└─────────────────────────────────────────────────────────────┘
```

### Event loop (`event_loop.rs`)

1. Draw the frame via `components::root::draw_ui`.
2. `tokio::select!` on:
   - wallet job completions (menu commands, send, NNS register, …)
   - balance sidebar refresh
   - send-simple planner preview
   - NNS name lookup
   - home identity fetch (address + `.nock` name)
   - CoinGecko price fetch
   - 120 ms tick (spinners, border pulse)
   - crossterm key/paste events from a background thread
3. Restore the terminal on exit or fatal error.

Bracketed paste mode is enabled so address fields and other editors receive full clipboard text as `Event::Paste`.

### State management (`store/`)

UI state follows a small Redux-style pattern:

- **`UIStore`** holds `UiState` plus cached session settings for the Settings screen.
- All mutations go through **`UIStore::dispatch(UiAction)`** → **`apply_ui_action`** in `store/apply.rs`.
- Screen-specific fields live on the **`Screen`** enum in `screens.rs` (not scattered globals).

This keeps draw code pure and makes transitions testable (see unit tests in `store/mod.rs`).

### Wallet execution (`command_runner.rs`)

**`TuiRuntime`** is the shared session handle:

| Field | Purpose |
| --- | --- |
| `wallet` | `Arc<Mutex<Wallet>>` — single kernel instance for TUI + API |
| `snapshot` | Planner snapshot for `create-tx` |
| `cli` | Live `WalletCli` (gRPC endpoint updated from Settings) |
| `wallet_event_sink` | Structured `WalletEvent` capture from dispatch hooks |
| `tui_markdown_sink` | Kernel `%markdown` fallback (seed phrase, legacy output) |
| `session_config` | Persisted settings (`session.json` + API mirror) |
| `api_auth_token` | Per-session bearer token (memory only) |
| `api_job_tx` | Channel to the API job loop on the TUI `LocalSet` |

Background jobs suspend the alternate screen, run a command, then resume. Balance refresh on Home uses a separate path (`ShowBalance`) with a **nonce** so stale results are ignored.

### Output layer (`view/` + `wallet_outcome.rs`)

The TUI prefers structured **`WalletEvent`** values emitted by the wallet kernel (`[%raw …]` effects). `view::render_command_output` formats them for the status panel. When no structured event exists, it falls back to captured kernel markdown.

Home-specific helpers (balance nicks, active address parsing) also live in `view/`.

## UI layout

After the splash screen, the shell is a two-panel layout (`components/root.rs`):

- **Activity panel (top ~65%)** — current screen: Home tabs, Receive, Send, NNS buy, menus, wizards, prompts.
- **Status panel (bottom ~35%)** — command output, sync progress, loading indicator. Hidden for some flows (e.g. Receive address fetch, NNS create-tx).

When a prompt overlay is active (`TextPrompt`, `Confirm`, …), a prompt bar occupies the bottom of the activity panel instead.

### Home (`Screen::Home`)

Two tabs:

| Tab | Keys | Content |
| --- | --- | --- |
| **Wallet** | `1`, `h`/`l` | Balance hero (NOCK + USD), address / `.nock` name, Send / Receive / Register `.nock` |
| **Menu** | `2` | Full wallet menu (`MAIN_MENU` in `components/menus.rs`) |

Wallet tab shortcuts: **`s`** Send, **`r`** Receive, **`n`** NNS buy.

### Other screens

Defined in `screens.rs`. Notable flows:

- **SendSimple** — amount + recipient form, planner preview, confirm → `CreateTx` + `SendTx`
- **NnsBuy** — name search via NNS HTTP API, register via planner + kernel
- **CreateTx** — multi-step wizard (`create_tx.rs`, `ct_dispatch.rs`)
- **Running** — transient overlay while a wallet command executes; restores the previous screen on completion

Screen handlers live under `handlers/`; draw code under `components/`.

## JSON HTTP API (`wallet_api/`)

Each TUI session starts an HTTP server (default `127.0.0.1:8765`, configurable in Settings). All routes require the session bearer token (shown under **Settings → API token & curl examples**).

| Route | Method | Description |
| --- | --- | --- |
| `/health` | GET | Liveness |
| `/v1/wallet/state` | GET | Read `WalletSessionState` |
| `/v1/wallet/state` | POST | Update session (persists `session.json`, may restart listener) |
| `/v1/wallet/command` | POST | Run a wallet command: `{"argv":["show"]}` |

Jobs are queued to `run_api_job_loop` on the TUI **`LocalSet`** because `Wallet` is not `Send`. Example curls are in `docs/api-curl.txt`.

Session settings schema: `wallet-session-v1` in `wallet_api/state.rs` (`public_grpc_server_addr`, `api_listen`).

## Session persistence (`session.rs`)

`session.json` lives in the wallet data directory. On startup:

1. Load from disk (or seed from CLI connection defaults).
2. Best-effort GET from the local API to sync state.
3. Apply gRPC endpoint to the live `WalletCli`.

Settings changes from the TUI POST to the API and rewrite the file.

## Module map

```
tui/
├── mod.rs              Entry: tui::run
├── event_loop.rs       Terminal + async loop
├── app_state.rs        UiState, BalancePanelState, PriceState, …
├── screens.rs          Screen enum + TuiControl
├── store/              UiAction + apply_ui_action
├── handlers/           Key/paste dispatch per screen
├── components/         ratatui draw (home, splash, menus, buttons, …)
├── command_runner.rs   TuiRuntime + job scheduling
├── view/               WalletEvent → display text
├── wallet_outcome.rs   Structured event types (shared with dispatch)
├── format.rs           NOCK/nicks formatting helpers
├── session.rs          session.json load/save/sync
├── session_client.rs   HTTP client for session API
├── wallet_api/         axum server, auth, executor
├── nns/                NNS lookup, resolve, register helpers
├── create_tx.rs        Create-tx wizard state
├── ct_dispatch.rs      Create-tx keyboard dispatch
├── send_simple.rs      Simple send planner glue
├── prompt_overlay.rs   Prompt/confirm overlay detection
├── paste.rs            Bracketed-paste helpers
├── clipboard.rs        Copy-to-clipboard (where supported)
└── hooks/              Terminal restore, crossterm channel, logging
```

## Adding a feature

Typical checklist:

1. **Screen** — add a variant to `screens.rs` if needed.
2. **Actions** — extend `UiAction` and handle it in `store/apply.rs`.
3. **Handler** — route keys in `handlers/mod.rs` → new `handlers/foo.rs`.
4. **Draw** — add `components/foo.rs`, call from `components/root.rs`.
5. **Wallet I/O** — schedule work via `command_runner::schedule_wallet_command` or a dedicated spawn helper; apply results through a completion channel in `event_loop.rs`.
6. **Output** — if the kernel should return new data, add a `WalletEvent` variant in `wallet_outcome.rs` and a renderer in `view/mod.rs`.

Keep draw functions free of async and wallet locks. Keep kernel calls inside `command_runner` / dispatch.

## Testing

- `tui/mod.rs` — slash-command normalization
- `store/mod.rs` — action reducers (nonce staleness, Running guard, …)
- `wallet_api/state.rs` — session JSON roundtrip

Run:

```bash
cargo test -p nockchain-wallet tui::
```

## Dependencies on the rest of the crate

| Outside `tui/` | Role |
| --- | --- |
| `dispatch.rs` | Sole wallet command executor |
| `command.rs` | `Commands`, `WalletCli` |
| `Wallet` | Kernel handle |
| `create_tx.rs` (crate root) | Transaction planner types used by wizards |

The TUI does **not** change CLI behavior when `tui` is not invoked.
