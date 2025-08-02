# NATS + RxDB Real-time User Versioning System

## Project Overview

This project implements a real-time user management system using NATS messaging with RxDB frontend replication. The system enables:

- Frontend clients to publish user updates directly to NATS
- Backend processes messages, saves to PostgreSQL with versioning
- Real-time synchronization between multiple frontend clients
- Complete audit trail of all user changes

## Architecture

```
┌─────────────────┐    users.update    ┌──────────────────┐
│   RxDB Client   │ ──────────────────► │                  │
│   (Frontend)    │                     │   NATS Server    │
└─────────────────┘                     │   (JetStream)    │
         ▲                               │                  │
         │                               └──────────────────┘
         │                                         │
         │ users.broadcast                         │ users.update
         │                                         ▼
         │                               ┌──────────────────┐
         └─────────────────────────────── │  Go Backend      │
                                         │  (User Handler)  │
                                         └──────────────────┘
                                                   │
                                                   ▼
                                         ┌──────────────────┐
                                         │   PostgreSQL     │
                                         │ (Users+Versions) │
                                         └──────────────────┘
```

## Components

### 1. NATS JetStream Server
- **Purpose**: Message broker with persistent streams
- **Streams**: 
  - `USERS_UPDATE`: Receives user changes from frontend
  - `USERS_BROADCAST`: Broadcasts changes to all clients
- **Port**: 4222

### 2. Go Backend (`internal/`)
- **User Handler**: Processes NATS messages and manages PostgreSQL
- **Repository Pattern**: Clean data access layer
- **Versioning**: Every user change creates a new version record
- **API**: Optional REST endpoints for debugging
- **Port**: 8080

### 3. RxDB Frontend (`frontend/`)
- **RxDB**: IndexedDB-based reactive database
- **NATS Replication**: Direct connection to NATS server
- **Real-time Sync**: Automatic synchronization across clients
- **Port**: 3000
- install dependencies with `pnpm install` at the root of the workspace
- start the server with `pnpm run dev` at the root of the workspace

### 4. PostgreSQL Database
- **Users Table**: Core user data (id, email, status, role)
- **Versions Table**: Complete audit trail of changes
- **Port**: 5432

### 5. js types
for convenients, there's a shared js types package used by both the server & the front end

### 6. js server
the js server uses a sqlite database, and exposes endpoints & websockets for the custom replication

## Prerequisites

1. **Docker** (for NATS and PostgreSQL)
2. **Go 1.21+** (for backend)
3. **Node.js 22+** (for frontend server)

## Quick Start

### 0. Replicate with JS Server

```bash
pnpm install
# starts everything
pnpm -r --parallel dev
```

go to http://localhost:3000 and play !
replication is now started directly after rxDB initialization

### 1. Start Infrastructure Services

**NATS with JetStream (RxDB Compatible):**
```bash
docker run --rm -d --name rxdb-nats-ws -p 4222:4222 -p 9222:9222 -v $(pwd)/nats-server.conf:/etc/nats/nats-server.conf nats:2.9.17 -js -c /etc/nats/nats-server.conf
# to be removed if we're happy with the new command
# docker run -d --name nats-jetstream -p 4222:4222 -p 8222:8222 nats:latest --jetstream
```

**PostgreSQL:**
```bash
docker run -d --name postgres \
  -e POSTGRES_DB=cognyx \
  -e POSTGRES_USER=cognyx \
  -e POSTGRES_PASSWORD=cognyx \
  -p 5432:5432 postgres:15
```

**Create Database Tables:**
```bash
# Connect to PostgreSQL
psql postgres://cognyx:cognyx@localhost:5432/cognyx

# Create tables
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    email VARCHAR NOT NULL UNIQUE,
    status VARCHAR NOT NULL,
    role VARCHAR,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE version (
    id BIGSERIAL PRIMARY KEY,
    object_type VARCHAR NOT NULL,
    object_id BIGINT NOT NULL,
    version INTEGER NOT NULL,
    json JSONB NOT NULL,
    action VARCHAR NOT NULL,
    actor VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

# Exit psql
\q
```

### 2. Setup NATS Streams

```bash
cd internal
go run setup_nats_streams.go
```

Expected output:
```
Created USERS_UPDATE stream
Created USERS_BROADCAST stream
✅ NATS JetStream streams setup complete!
```

### 3. Start Backend

```bash
cd internal
go run cmd/users_api/main.go
```

Expected output:
```
Server starting on :8080
```

### 4. Start Frontend

```bash
pnpm install
pnpm run dev
```

Expected output:
```
Frontend server running on http://localhost:3000
```

## Testing the Complete Flow

### 1. Open Multiple Clients

Open 2+ browser tabs to `http://localhost:3000`

### 2. Start NATS Replication

In each browser tab:
1. Click **"🔄 Start NATS Replication"**
2. Verify status shows "✅ NATS replication is active!"

### 3. Test Real-time Sync

**In Client 1:**
1. Enter email: `test@example.com`
2. Set status: `active`
3. Click **"➕ Create User"**

**Expected Results:**
- Client 1: User appears in local list immediately
- Backend: Processes message, saves to PostgreSQL
- Client 2: User appears automatically after backend processing
- Both clients show same user data

### 4. Test Updates

**In Client 2:**
1. Click **"🔄 Update Random User"**

**Expected Results:**
- Client 2: Shows updated user data
- Backend: Creates new version record
- Client 1: Automatically receives and displays update

## Monitoring & Debugging

### NATS Message Monitoring

```bash
# Monitor all messages
./monitor_nats.sh

# Decode message payloads
./decode_nats_messages.sh
```

### Backend Logs

Backend logs show:
- Message received from NATS
- Database operations
- Version creation
- Broadcast to clients

### Frontend Logs

Browser console shows:
- RxDB initialization
- NATS connection status
- Document synchronization
- Replication events

## Database Schema

### Users Table
```sql
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    email VARCHAR NOT NULL UNIQUE,
    status VARCHAR NOT NULL,
    role VARCHAR,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Versions Table
```sql
CREATE TABLE version (
    id BIGSERIAL PRIMARY KEY,
    object_type VARCHAR NOT NULL,
    object_id BIGINT NOT NULL,
    version INTEGER NOT NULL,
    json JSONB NOT NULL,
    action VARCHAR NOT NULL,
    actor VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Key Features

### 1. Event-Driven Architecture
- Frontend publishes to `users.update`
- Backend processes and broadcasts to `users.broadcast`
- No direct HTTP API calls needed

### 2. Complete Versioning
- Every user change creates a version record
- Full JSON snapshot of user data
- Action tracking (create, update, delete)
- Actor identification

### 3. Real-time Synchronization
- RxDB NATS replication provides automatic sync
- Changes propagate to all connected clients
- IndexedDB provides offline capabilities

### 4. Resilient Design
- NATS JetStream provides message persistence
- PostgreSQL ensures data durability
- RxDB handles offline/online scenarios

## Troubleshooting

### NATS Connection Issues
```bash
# Check NATS server is running
docker ps | grep rxdb-nats

# Verify JetStream is enabled
curl http://localhost:8222/jsz
```

### Database Connection Issues
```bash
# Test PostgreSQL connection
psql postgres://cognyx:cognyx@localhost:5432/cognyx

# Check tables exist
\dt
```

### Frontend Import Issues
- RxDB modules are loaded via CDN
- Check browser console for import errors
- Verify network connectivity to unpkg.com

### Backend Issues
```bash
# Check Go module dependencies
go mod tidy

# Verify database migrations
go run app.go migrate status
```

## Message Flow Details

### 1. User Creation Flow
```
Frontend → users.update → Backend → PostgreSQL → users.broadcast → All Frontends
```

### 2. Message Format
```json
{
  "id": "uuid-string",
  "email": "user@example.com",
  "status": "active",
  "role": "user",
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T00:00:00Z",
  "_deleted": false
}
```

### 3. Version Record Format
```json
{
  "object_type": "user",
  "object_id": 123,
  "version": 1,
  "action": "create",
  "actor": "frontend-client",
  "json": { "complete": "user data" }
}
```

## Security Considerations

- NATS server should use authentication in production
- PostgreSQL connections should use SSL
- Frontend should validate user permissions
- Version records provide complete audit trail

## Performance Notes

- RxDB provides efficient IndexedDB operations
- NATS JetStream handles high message throughput
- PostgreSQL versioning grows over time (consider archiving)
- Frontend batching reduces network overhead

## Next Steps

1. Add user authentication
2. Implement conflict resolution
3. Add data validation
4. Set up monitoring and alerts
5. Deploy to production environment