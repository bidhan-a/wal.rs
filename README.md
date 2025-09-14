# wal.rs

A high-performance, distributed write-ahead log inspired by Apache Kafka.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   gRPC Client   │    │   gRPC Client   │    │   gRPC Client   │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌────────────▼──────────────┐
                    │     gRPC Server           │
                    │   (TLS + Authentication)  │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │        Log Service        │
                    │   (Append/Read/Stream)    │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │      Segment Manager      │
                    │   (Rotation & Cleanup)    │
                    └─────────────┬─────────────┘
                                  │
          ┌───────────────────────┼────────────────────────┐
          │                       │                        │
┌─────────▼─────────┐    ┌────────▼─────────┐     ┌────────▼──────────┐
│     Segment 1     │    │     Segment 2     │    │     Segment N     │
│  ┌─────────────┐  │    │  ┌─────────────┐  │    │  ┌─────────────┐  │
│  │    Store    │  │    │  │    Store    │  │    │  │    Store    │  │
│  │  (Data)     │  │    │  │  (Data)     │  │    │  │  (Data)     │  │
│  └─────────────┘  │    │  └─────────────┘  │    │  └─────────────┘  │
│  ┌─────────────┐  │    │  ┌─────────────┐  │    │  ┌─────────────┐  │
│  │    Index    │  │    │  │    Index    │  │    │  │    Index    │  │
│  │ (Offsets)   │  │    │  │ (Offsets)   │  │    │  │ (Offsets)   │  │
│  └─────────────┘  │    │  └─────────────┘  │    │  └─────────────┘  │
└───────────────────┘    └───────────────────┘    └───────────────────┘
```

## Quick Start

### Prerequisites

- Rust 1.70+ (2024 edition)
- Go 1.21+ (for certificate generation)
- cfssl (for TLS certificate generation)


### Project Structure

```
wal.rs/
├── api/           # Protocol Buffers and gRPC service definitions
├── cfg/           # Configuration management
├── log/           # Core log implementation
├── server/        # gRPC server implementation
├── test/          # TLS certificates and test data
```

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/bidhan-a/wal.rs
   cd walrs
   ```

2. **Generate TLS certificates:**
   ```bash
   make init
   make gencert
   ```

3. **Build the project:**
   ```bash
   cargo build
   ```

4. **Run tests:**
   ```bash
   cargo test
   ```

### TODO
- [ ] Service Discovery (memberlist/gossip)
- [ ] Distributed Consensus (raft)
- [ ] Logging and Instrumentation
