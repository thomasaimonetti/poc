<script setup lang="ts">
import type { RxCollection, RxDatabase } from "rxdb";
import { createRxDatabase } from "rxdb";
import { addRxPlugin } from 'rxdb/plugins/core';
// import { RxDBDevModePlugin } from "rxdb/plugins/dev-mode";
import { RxDBQueryBuilderPlugin } from "rxdb/plugins/query-builder";
import { getRxStorageDexie } from "rxdb/plugins/storage-dexie";
import { RxDBUpdatePlugin } from 'rxdb/plugins/update';
import { wrappedValidateAjvStorage } from "rxdb/plugins/validate-ajv";
import { onMounted, reactive, ref } from "vue";
import { replicateNats, RxNatsReplicationState } from "./replication-nats";


const userSchema = {
      version: 0,
      primaryKey: "id",
      type: "object",
      properties: {
        id: { type: "string", maxLength: 255 },
        email: { type: "string" },
        status: { type: "string" },
        role: { type: "string" },
        created_at: { type: "string" },
        updated_at: { type: "string" },
        _deleted: { type: "boolean" },
      },
      required: ["id", "email", "status"],
    };

interface User {
    id: string
    email: string
    status: string
    role?: string
    created_at: string
    updated_at: string
    _deleted: boolean
}

// Global variables
const database = ref<RxDatabase | null>(null);
const usersCollection = ref<{users: RxCollection<User, {}, unknown, unknown>} | null>(null);
const replicationState = ref<RxNatsReplicationState<User> | null>(null);
const syncedCount = ref(0);
const errorCount = ref(0);
const replicationStarted = ref(false);
const replicationStatus = ref("Initializing RxDB...");
const replicationStatusType = ref("info");

const logs = ref<{ id: number; message: string }[]>([]);

const userState = reactive<Pick<User, "email" | "status" | "role">>({
    email: "",
    status: "active",
    role: "user"
});

const usersStore = ref<User[]>([]);

// Initialize RxDB
async function initRxDB() {
  try {
    logMessage("🔧 Creating RxDB database...");
    // addRxPlugin(RxDBDevModePlugin);
    addRxPlugin(RxDBQueryBuilderPlugin)
    addRxPlugin(RxDBUpdatePlugin);
    
    const storage = wrappedValidateAjvStorage({ storage: getRxStorageDexie() })

    database.value = await createRxDatabase({
      name: "usersdb_nats",
      storage,
      multiInstance: true,
    });

    

    usersCollection.value = await database.value?.addCollections({
      users: { schema: userSchema },
    });

    // Set up reactive UI updates
    usersCollection.value?.users.find().$.subscribe((users) => {
      console.log("users", { users })
        usersStore.value = users.map(doc => doc.toJSON() as User);
      });

    updateReplicationStatus("✅ RxDB initialized successfully", "success");
    logMessage("✅ RxDB database and collection created");
  } catch (error) {
    console.error("RxDB initialization failed:", error);
    logMessage("❌ RxDB initialization failed: " + (error as Error).message);
    updateReplicationStatus(
      "Failed to initialize RxDB: " + (error as Error).message,
      "error"
    );
    errorCount.value++;
  }
}

// Start NATS replication
async function startReplication() {
  try {
    updateReplicationStatus("Starting NATS replication...", "info");
    logMessage("🔄 Starting RxDB NATS replication...");

    replicationState.value = replicateNats({
      collection: usersCollection.value?.users,
      replicationIdentifier: "users-replication",
      streams: {
        pull: "USERS_BROADCAST",
        push: "USERS_UPDATE"
      },
      subjectPrefix: "users",
      connection: { servers: "ws://127.0.0.1:9222" },
      live: true,
      pull: { batchSize: 30 },
      push: { batchSize: 30 },
    });

    // Set up event listeners

    replicationState.value.error$.subscribe((error) => {
      console.error("Replication error:", error);
      errorCount.value++;
      logMessage("❌ Replication error: " + (error as Error).message);
    });

    replicationState.value.active$.subscribe((active) => {
      logMessage(
        active ? "🟢 Replication is active" : "🔴 Replication is inactive"
      );
    });

    replicationState.value.sent$.subscribe((sent) => {
      syncedCount.value++;
      logMessage("📤 Document sent to NATS: " + sent.id);

    });

    replicationState.value.received$.subscribe((received) => {
      syncedCount.value++;
      logMessage("📥 Document received from NATS: " + received.id);

    });

    // Wait for initial replication
    await replicationState.value?.awaitInitialReplication();

    updateReplicationStatus("✅ NATS replication is active!", "success");
    logMessage("✅ NATS replication started successfully");

    replicationStarted.value = true;
    
  } catch (error) {
    console.error("Failed to start replication:", error);
    logMessage("❌ Failed to start NATS replication: " + (error as Error).message);
    updateReplicationStatus(
      "Failed to start replication: " + (error as Error).message,
      "error"
    );
    errorCount.value++;

  }
}

// Stop NATS replication
async function stopReplication() {
  try {
    if (replicationState.value) {
      await replicationState.value.cancel();
      replicationState.value = null;
    }

    updateReplicationStatus("Replication stopped", "info");
    logMessage("⏹️ NATS replication stopped");

    replicationStarted.value = false;
    
  } catch (error) {
    console.error("Failed to stop replication:", error);
    logMessage("❌ Failed to stop replication: " + (error as Error).message);
    errorCount.value++;
  }
}

// Create a new user
async function createUser() {
  try {

    const { email, status, role } = userState;

    if (!email || !status) {
      alert("Email and status are required!");
      return;
    }

    logMessage("👤 Creating user in RxDB...");

    const userDoc = {
      id: generateUUID(),
      email: email,
      status: status,
      role: role || "user",
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      _deleted: false,
    };

    await usersCollection.value?.users.insert(userDoc);

    logMessage("✅ User created: " + email);

    // Clear form
    userState.email = "";
    userState.status = "active";
    userState.role = "user";
  } catch (error) {
    console.error("Failed to create user:", error);
    logMessage("❌ Failed to create user: " + (error as Error).message);
    errorCount.value++;
  }
}

// Update a random user
async function updateRandomUser() {
  try {
    const users = await usersCollection.value?.users
      .find()
      .where("_deleted")
      .ne(true)
      .exec();

    if (!users || users.length === 0) {
      alert("No users to update. Create a user first!");
      return;
    }

    const randomUser = users[Math.floor(Math.random() * users.length)];
    const newEmail = `updated-${Date.now()}@example.com`;
    const newStatus = Math.random() > 0.5 ? "active" : "inactive";
    const newRole = Math.random() > 0.5 ? "admin" : "user";

    logMessage(`🔄 Updating user ${randomUser.email}...`);

    await randomUser.update({
      $set: {
        email: newEmail,
        status: newStatus,
        role: newRole,
        updated_at: new Date().toISOString(),
      },
    });

    logMessage("✅ User updated: " + newEmail);
  } catch (error) {
    console.error("Failed to update user:", error);
    logMessage("❌ Failed to update user: " + (error as Error).message);
    errorCount.value++;
  }
}

// Delete a random user (soft delete)
async function deleteRandomUser() {
  try {
    const users = await usersCollection.value?.users
      .find()
      .where("_deleted")
      .ne(true)
      .exec();

    if (!users || users.length === 0) {
      alert("No users to delete. Create a user first!");
      return;
    }

    const randomUser = users[Math.floor(Math.random() * users.length)];

    logMessage(`🗑️ Deleting user ${randomUser.email}...`);

    await randomUser.update({
      $set: {
        _deleted: true,
        updated_at: new Date().toISOString(),
      },
    });

    logMessage("✅ User deleted: " + randomUser.email);
  } catch (error) {
    console.error("Failed to delete user:", error);
    logMessage("❌ Failed to delete user: " + (error as Error).message);
    errorCount.value++;
  }
}

// Clear all local users
async function clearLocalUsers() {
  try {
    if (confirm("Are you sure you want to delete all local users?")) {
      const users = await usersCollection.value?.users
        .find()
        .where("_deleted")
        .ne(true)
        .exec();

      for (const user of users || []) {
        await user.update({
          $set: {
            _deleted: true,
            updated_at: new Date().toISOString(),
          },
        });
      }

      logMessage("🧹 All local users marked as deleted");
    }
  } catch (error) {
    console.error("Failed to clear users:", error);
    logMessage("❌ Failed to clear users: " + (error as Error).message);
    errorCount.value++;
  }
}

// Update replication status
function updateReplicationStatus(message: string, type: string) {
  replicationStatus.value = message;
  replicationStatusType.value = type;
}

// Utility functions
function logMessage(message: string) {

  const timestamp = new Date().toLocaleTimeString();
  logs.value.push({ id: logs.value.length + 1, message: `[${timestamp}] ${message}` });
}

function clearLogs() {
  logs.value = [{ id: 1, message: "🧹 Logs cleared" }];
}

function generateUUID() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c == "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

onMounted(() => {
  window.addEventListener("load", async () => {
    logMessage("🎬 Initializing RxDB NATS Replication Frontend...");
    await initRxDB();
    logMessage('✅ Ready! Click "Start NATS Replication" to begin syncing.');
  });
});
</script>

<template>
  <h1>🚀 RxDB NATS Replication Frontend</h1>

  <div class="container">
    <h3>🔄 RxDB NATS Replication Status</h3>
    <div id="replicationStatus" :class="['status', replicationStatusType]">{{  replicationStatus }}</div>
    <div class="stats">
      <div class="stat-item">
        <div class="stat-value" id="localCount">{{ usersStore.length }}</div>
        <div class="stat-label">Local Users</div>
      </div>
      <div class="stat-item">
        <div class="stat-value" id="syncedCount">{{ syncedCount }}</div>
        <div class="stat-label">Synced Docs</div>
      </div>
      <div class="stat-item">
        <div class="stat-value" id="errorCount">{{ errorCount }}</div>
        <div class="stat-label">Errors</div>
      </div>
    </div>
    <button id="startReplication" @click="startReplication" :disabled="replicationStarted">
      🔄 Start NATS Replication
    </button>
    <button id="stopReplication" @click="stopReplication" :disabled="!replicationStarted">
      ⏹️ Stop Replication
    </button>
  </div>

  <div class="container">
    <h3>👤 User Management</h3>
    <div class="form-group">
      <label>Email:</label>
      <input type="email" id="userEmail" placeholder="user@example.com" v-model="userState.email" />
    </div>
    <div class="form-group">
      <label>Status:</label>
      <input type="text" id="userStatus" placeholder="active" v-model="userState.status" />
    </div>
    <div class="form-group">
      <label>Role:</label>
      <input type="text" id="userRole" placeholder="user" v-model="userState.role" />
    </div>
    <button @click="createUser">➕ Create User</button>
    <button @click="updateRandomUser">🔄 Update Random User</button>
    <button @click="deleteRandomUser">🗑️ Delete Random User</button>
  </div>

  <div class="container">
    <h3>💾 Local Users (RxDB + NATS)</h3>
    <div id="localUsers" class="user-list">
      <div v-if="usersStore.length === 0" class="user-item">No users yet...</div>
      <div v-else v-for="user in usersStore" :key="user.id" class="user-item">
        <div class="user-info">
            <div class="user-email">{{ user.email }}</div>
            <div class="user-meta">ID: {{ user.id.substring(0, 8) }}... | Status: {{ user.status }} | Role: {{ user.role }}</div>
        </div>
    </div>
    </div>
    <button @click="clearLocalUsers">🧹 Clear All Local Users</button>

  </div>

  <div class="container">
    <h3>📡 Replication Logs</h3>
    <div id="logs" class="logs">
      <div v-for="log in logs" :key="log.id" class="log-entry">{{  log.message }}</div>
    </div>
    <button @click="clearLogs">🧹 Clear Logs</button>
  </div>
</template>

<style scoped>
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  max-width: 1000px;
  margin: 0 auto;
  padding: 20px;
  line-height: 1.6;
}
.container {
  background: #f8f9fa;
  padding: 20px;
  border-radius: 8px;
  margin-bottom: 20px;
}
button {
  background: #007bff;
  color: white;
  border: none;
  padding: 10px 20px;
  border-radius: 4px;
  cursor: pointer;
  margin: 5px;
  font-size: 14px;
}
button:hover {
  background: #0056b3;
}
button:disabled {
  background: #6c757d;
  cursor: not-allowed;
}
.status {
  padding: 10px;
  border-radius: 4px;
  margin: 10px 0;
}
.status.success {
  background: #d4edda;
  color: #155724;
  border: 1px solid #c3e6cb;
}
.status.error {
  background: #f8d7da;
  color: #721c24;
  border: 1px solid #f5c6cb;
}
.status.info {
  background: #d1ecf1;
  color: #0c5460;
  border: 1px solid #bee5eb;
}
.user-list {
  max-height: 300px;
  overflow-y: auto;
  border: 1px solid #dee2e6;
  border-radius: 4px;
  padding: 10px;
  background: white;
}
.user-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px;
  border-bottom: 1px solid #eee;
}
.user-item:last-child {
  border-bottom: none;
}
.user-info {
  flex: 1;
}
.user-email {
  font-weight: bold;
  color: #333;
}
.user-meta {
  font-size: 12px;
  color: #666;
  margin-top: 4px;
}
.form-group {
  margin: 10px 0;
}
.form-group label {
  display: block;
  margin-bottom: 5px;
  font-weight: bold;
}
.form-group input {
  width: 100%;
  padding: 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 14px;
  box-sizing: border-box;
}
.logs {
  background: #1e1e1e;
  color: #f8f8f2;
  padding: 15px;
  border-radius: 4px;
  max-height: 300px;
  overflow-y: auto;
  font-family: "Monaco", "Consolas", monospace;
  font-size: 12px;
  line-height: 1.4;
}
.log-entry {
  margin: 2px 0;
  word-wrap: break-word;
}
h1 {
  color: #007bff;
  text-align: center;
}
h3 {
  margin-top: 0;
  color: #495057;
}
.stats {
  display: flex;
  gap: 20px;
  margin-top: 10px;
}
.stat-item {
  background: white;
  padding: 10px;
  border-radius: 4px;
  border: 1px solid #dee2e6;
  text-align: center;
}
.stat-value {
  font-size: 24px;
  font-weight: bold;
  color: #007bff;
}
.stat-label {
  font-size: 12px;
  color: #666;
}
</style>
