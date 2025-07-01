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

## Transaction Lifecycle Diagram

The following diagram shows the complete lifecycle of a transaction from creation to confirmation, including error handling and competition scenarios:

```mermaid
flowchart TD
    subgraph "Transaction Creation"
        A1["Cross Detection"]
        A2["Build Transaction"]
        A3["Calculate Priority Fee"]
        A4["Set CU Limit"]
    end

    subgraph "Transaction Sending"
        B1["TxWorker.send_tx"]
        B2["Sign & Send to RPC"]
        B3["Add to PendingTxs"]
        B4["Increment Metrics"]
    end

    subgraph "Transaction Confirmation"
        C1["gRPC Transaction Update"]
        C2["TxWorker.confirm_tx"]
        C3["Get Transaction Details"]
        C4["Parse Transaction Logs"]
        C5["Update Metrics"]
    end

    subgraph "Success Path"
        D1["Transaction Success"]
        D2["Parse Fill Events"]
        D3["Compare Expected vs Actual Fills"]
        D4["Record Performance Metrics"]
    end

    subgraph "Error Paths"
        E1["Send Error"]
        E2["Confirmation Error"]
        E3["Transaction Failed"]
        E4["Insufficient Funds"]
        E5["Compute Unit Exceeded"]
    end

    subgraph "Competition Scenarios"
        F1["AMM Fill Beats Us"]
        F2["Higher Priority Fee"]
        F3["Order Already Filled"]
        F4["Partial Fill"]
    end

    %% Main flow
    A1 --> A2 --> A3 --> A4 --> B1 --> B2 --> B3 --> B4
    B2 --> C1 --> C2 --> C3 --> C4 --> C5

    %% Success path
    C4 --> D1 --> D2 --> D3 --> D4

    %% Error paths
    B2 --> E1
    C3 --> E2
    C4 --> E3
    E3 --> E4
    E3 --> E5

    %% Competition scenarios
    D2 --> F1
    D2 --> F2
    D2 --> F3
    D3 --> F4

    %% Styling
    classDef success fill:#d4edda,stroke:#155724,color:#155724
    classDef error fill:#f8d7da,stroke:#721c24,color:#721c24
    classDef competition fill:#fff3cd,stroke:#856404,color:#856404
    classDef process fill:#d1ecf1,stroke:#0c5460,color:#0c5460

    class D1,D2,D3,D4 success
    class E1,E2,E3,E4,E5 error
    class F1,F2,F3,F4 competition
    class A1,A2,A3,A4,B1,B2,B3,B4,C1,C2,C3,C4,C5 process
```

## TODO:
- [x] increase memory for rust-filler pod

- [x] increase CU limit for place swift order
- [x] add athena queries to dashboard to track volume, maybe metrics. fills by market?
- [x] add vamm price to DLOB, prevent some wasted orders
- [x] add top makers to cross info
- [ ] improve metrics so its presentable
- [ ] swift tx retry on error
- [ ] consider VAMM order size?
- [ ] safe trigger orders
- [ ] check PMM orders (MT filler)
- [ ] use internal endpoint for swift order stream
- [ ] add swift orders to auctions immediately? (MT filler)

- [ ] rayon: parallelize the orderbook updates on new slot

## Tx Summary
print some recent tx stats
```bash
 RUST_LOG=info cargo run --release --bin=tx_history F1RsRqBjuLdGeKtQK2LEjVJHJqVbhBYtfUzaUCi8PcFv --rpc-url <RPC_URL>
 ```

## order does not exist examples
example: beaten by AMM fill ~5 slots earlier
 - https://solscan.io/tx/2jme1XL7WqTTVcKgjydozCw5gX441pSba1ptTr6DxtddrR9hPLX6SF4MUzd7VG7uTRs4uL2VavF8MRg8ptzS4By5
 - https://solscan.io/tx/43hfKyZXBvxRL4tcPdhCDUwBxLbriqEjRCUGMeSgNto7oiWyMuaJXdpqZU5qCJNcjvzzr7j9YUpjEDdavyHXAnks

example: beaten by AMM fill 1 slot earlier
toid: 3402
slot: 349335921
- us: https://solscan.io/tx/2An2tBXauLebwJbtg5a57W7BxbnkWyQBNAw4b4NcGy1fFyosV1JvoN6WDh472xHkRDUdE65C8aZvb3dkoPRbWDPq
- them: https://solscan.io/tx/5ry63m59ouJykrZBFmJHhvgrgdishEZFSUyK6gBd1M7AXDi4WUmjNqm7nD2jxQ2pr5Cb5p92rr7gsuFZKapP8sgJ

example: beaten by AMM fill 2 slots earlier
toid: 487
slot: 349336219
them slot: 349336217
us: https://solscan.io/tx/61sdUnbQTQk4KFzFn4Bt4gZ1XmYZErJdkex1RhRam34EoaKXb6D1uLSGcpheh96JSzGEsjDxk26oyYwVujfv1z7K
them: https://solscan.io/tx/wpzZePXNeTXaAFck1812S9GoFMN6Xr5kzi95GUfTLfqFjfxFofS8rrxRBWBijautcaE8u8r3TmpHLe1DdsnA5zL

seems in this case the order still seems live even though it filled.

toid: 259204
us: https://solscan.io/tx/2CbAQexzrHMZeE6pf9LbgAVNMi1m72Ys3A4NT7tdyLjoi1SQ2yWq5BfkA7FNUXnDsm3JVjS5yNqGDSFrLX73fnbt
349337367

AMM fill same slot
- https://solscan.io/tx/GfdeFh5n1vKdBdVtWqZzhAR1uL33kL6tP3Weju3R3Kwy2LVngJSABEtv9c7PkKjrhsF1rB7nPYqcdN49vatbBFQ
- https://solscan.io/tx/3jsjb25fPrENzFfGMFBps9VGNX2XKecxWLBSP7vPLjWXqkSfzPDjHtdy6T2om1mqkpFGvTfbm8VXRDo2LS6Qsd98
they have slightly higher fee:
- ours:   0.000005
- theirs: 0.000005882
- signer: 5r27HEM1Q9aXmy7MSD3HYJTGVKGmLTDa2q3c7hgJgj9b

toid: 1532
AMM fill same slot
- us: https://solscan.io/tx/5a6r6upr9Lm1N5AsxCpb5ZuNrM6YqoyHapHhPAM53WDRey1RpeEJ1QXqKCpS6eSV7fAnex1tpAvfQNf9kiRZUmSy
- them: https://solscan.io/tx/MguGhkkJHQEMAdLriHmPhkVCNEqVeJDvFXtx34xGQ1wU6pfnUViLaEUk8Sj6RSr7Pyy93YL9V1Q5PLJJSHvPB1e

they have slightly higher fee:
- ours:   0.000005
- theirs: 0.000005979
- signer: 5r27HEM1Q9aXmy7MSD3HYJTGVKGmLTDa2q3c7hgJgj9b
