## Run Filler

```shell
BOT_PRIVATE_KEY="...." \
GRPC_X_TOKEN="aabbccddeeff0011223344" \
RPC_URL="https://api.rpcpool.com/...." \
RUST_LOG=filler=info,dlob=info,swift=info \
cargo run --release -- --mainnet
```

## Event Flow Diagram

The following diagram illustrates the flow of events in the Filler Bot, from receiving gRPC and websocket events, updating the orderbook (DLOB), to sending and confirming transactions:

```mermaid
flowchart TD
    subgraph Event_Sources
        A1["gRPC Slot Update"]
        A2["gRPC Account Update"]
        A3["gRPC Transaction Update"]
        A4["Swift Order Websocket"]
    end

    subgraph DLOB_and_Notifier
        B1["DLOB (Orderbook State)"]
        B2["DLOBNotifier"]
    end

    subgraph FillerBot_MainLoop
        C1["Slot Receiver (from gRPC)"]
        C2["Swift Order Stream"]
        C3["Find Crosses"]
        C4["try_auction_fill / try_swift_fill"]
    end

    subgraph Transaction_Worker
        D1["TxWorker.send_tx"]
        D2["TxWorker.confirm_tx"]
    end

    %% Event flow
    A1 -- "on_slot_update_fn" --> B2
    A1 -- "on_slot_update_fn" --> C1
    A2 -- "on_account_update_fn" --> B2
    A3 -- "on_transaction_update_fn" --> D2
    A4 -- "New Swift Order" --> C2

    B2 -- "DLOBEvent::SlotOrPriceUpdate / Order" --> B1
    B1 -- "Orderbook State" --> C3
    C1 -- "New Slot" --> C3
    C2 -- "New Swift Order" --> C3
    C3 -- "Crosses Found?" --> C4
    C4 -- "Build Transaction" --> D1
    D1 -- "Send Transaction" --> Solana["Solana Network"]
    Solana -- "Transaction Update" --> A3
    D2 -- "Confirm & Metrics" --> FillerBot_MainLoop
    D1 -- "PendingTxs" --> D2

    %% Feedback
    D2 -- "Update Metrics, PendingTxs" --> FillerBot_MainLoop
```