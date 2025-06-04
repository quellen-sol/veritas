# Rust Project Documentation

## Token Pricing Derivation

The pricing engine in this Rust program derives token prices using a graph-based approach. The core component is the `MintPricingGraph`, which is a directed graph where nodes represent tokens (mints) and edges represent relationships between these tokens.

### Key Components

- **MintNode:** Each node in the graph represents a token and contains information about the token's mint and its USD price. The price can be sourced directly from an oracle or derived through relationships with other nodes in the graph.

- **MintEdge:** Each edge represents a relationship between two tokens, capturing details such as the relationship ID, whether the relationship data is up-to-date, and the last update time. The edge also holds a `LiqRelation`, which likely contains liquidity-related data.

- **USDPriceWithSource:** This enum indicates the source of the USD price for a token. It can either be directly from an oracle or derived through relationships with other nodes in the graph.

### Pricing Mechanism

1. **Oracle Pricing:** If available, the price of a token is directly obtained from an oracle, providing a reliable and up-to-date price.

2. **Relation-Based Pricing:** If oracle data is not available, the price is derived through relationships with other tokens in the graph. This involves traversing the graph and using the `LiqRelation` data to calculate the price based on liquidity and other factors.

This graph-based approach allows the program to efficiently manage and update token prices, leveraging both direct oracle data and complex inter-token relationships to ensure accurate pricing.

### BFS Recalculation

The `bfs_recalculate` function is a key component of the pricing engine, responsible for traversing the `MintPricingGraph` to update token prices. It uses a breadth-first search (BFS) approach to visit each node (token) in the graph, recalculating prices based on relationships with other tokens.

- **Oracle Check:** The function first checks if a token's price is directly available from an oracle. If so, it skips recalculating the price but continues to traverse the graph.

- **Price Calculation:** For tokens without direct oracle prices, the function calculates a new price using the `get_total_weighted_price` function, which aggregates prices from related tokens.

- **Price Update:** If the new price differs significantly from the current price, the function updates the token's price and emits a notification (dooot) for further processing.

This mechanism ensures that token prices are consistently updated, leveraging both direct oracle data and derived prices from the graph's relationships.

## Crates

### SDK Crate

The `sdk` crate is a library that provides core functionalities and utilities for the project. It is structured as follows:

- **Modules:**
  - `constants`: Contains constant values used throughout the SDK.
  - `ppl_graph`: Manages graph-related functionalities.
    - `graph.rs`: Implements graph algorithms and data structures.
    - `structs.rs`: Defines the structures used in graph operations.
  - `utils`: Provides utility functions and helpers.

### Engine Crate

The `engine` crate is another component of the project, though its specific functionalities are not detailed in this document.

#### API Endpoints

The engine crate provides several HTTP endpoints through an Axum server running on port 3000:

- **Health Check**
  - Endpoint: `/healthcheck`
  - Method: GET
  - Returns: 200 OK if service is ready, 503 Service Unavailable during bootstrap

- **Debug Endpoints**
  - `/debug-node`: Get detailed information about a specific node in the pricing graph
  - `/lp-cache`: Query liquidity pool cache information
  - `/decimal-cache`: Retrieve decimal precision information for tokens
  - `/balance-cache`: Get token balance information from the cache

Each endpoint provides access to internal state and debugging information about the pricing engine's operation.

## Dependencies

The project relies on several external crates, including but not limited to:

- `tokio`: For asynchronous programming.
- `serde` and `serde_json`: For serialization and deserialization.
- `lapin`: For AMQP (Advanced Message Queuing Protocol) client functionalities.
- `solana-sdk`: For interacting with the Solana blockchain.

## Building and Running

To build the project, use the following command:

```bash
cargo build --release
```

To run the project, execute:

```bash
cargo run --release
```
