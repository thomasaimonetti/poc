import Database from 'better-sqlite3';
import { userSchema, type User } from 'types';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

// Define database row type
interface UserRow {
  id: string;
  email: string;
  status: string;
  role: string | null;
  created_at: string;
  updated_at: string;
  _deleted: number;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Initialize SQLite database
const db = new Database(join(__dirname, '../data/users.db'));

// Create users table based on the schema from types package
const createUsersTable = () => {
  const createTableSQL = `
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL,
      role TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      _deleted BOOLEAN NOT NULL DEFAULT 0
    )
  `;
  
  db.exec(createTableSQL);
  
  // Create indexes for better performance
  db.exec('CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_users_deleted ON users(_deleted)');
};

// Initialize database
createUsersTable();

// Prepared statements for better performance
const insertUser = db.prepare(`
  INSERT INTO users (id, email, status, role, created_at, updated_at, _deleted)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const selectAllUsers = db.prepare('SELECT * FROM users WHERE _deleted = 0');
const selectUserById = db.prepare('SELECT * FROM users WHERE id = ? AND _deleted = 0');
const selectUserByEmail = db.prepare('SELECT * FROM users WHERE email = ? AND _deleted = 0');

const updateUser = db.prepare(`
  UPDATE users 
  SET email = ?, status = ?, role = ?, updated_at = ?
  WHERE id = ? AND _deleted = 0
`);

const softDeleteUser = db.prepare(`
  UPDATE users 
  SET _deleted = 1, updated_at = ?
  WHERE id = ?
`);

// Database operations with Zod validation
export const userDb = {
  // Create a new user
  create: (userData: Omit<User, 'id' | 'created_at' | 'updated_at' | '_deleted'>): User => {
    const now = new Date().toISOString();
    const id = crypto.randomUUID();
    
    const user: User = {
      id,
      ...userData,
      created_at: now,
      updated_at: now,
      _deleted: false,
    };
    
    // Validate with Zod schema
    const validatedUser = userSchema.parse(user);
    
    insertUser.run(
      validatedUser.id,
      validatedUser.email,
      validatedUser.status,
      validatedUser.role || null,
      validatedUser.created_at,
      validatedUser.updated_at,
      validatedUser._deleted ? 1 : 0
    );
    
    return validatedUser;
  },

  // Get all users
  getAll: (): User[] => {
    const rows = selectAllUsers.all() as UserRow[];
    return rows.map(row => userSchema.parse({
      ...row,
      _deleted: Boolean(row._deleted)
    }));
  },

  // Get user by ID
  getById: (id: string): User | null => {
    const row = selectUserById.get(id) as UserRow | undefined;
    if (!row) return null;
    
    return userSchema.parse({
      ...row,
      _deleted: Boolean(row._deleted)
    });
  },

  // Get user by email
  getByEmail: (email: string): User | null => {
    const row = selectUserByEmail.get(email) as UserRow | undefined;
    if (!row) return null;
    
    return userSchema.parse({
      ...row,
      _deleted: Boolean(row._deleted)
    });
  },

  // Update user
  update: (id: string, userData: Partial<Omit<User, 'id' | 'created_at' | 'updated_at' | '_deleted'>>): User | null => {
    const existingUser = userDb.getById(id);
    if (!existingUser) return null;

    const updatedUser = {
      ...existingUser,
      ...userData,
      updated_at: new Date().toISOString(),
    };

    // Validate with Zod schema
    const validatedUser = userSchema.parse(updatedUser);

    updateUser.run(
      validatedUser.email,
      validatedUser.status,
      validatedUser.role || null,
      validatedUser.updated_at,
      validatedUser.id
    );

    return validatedUser;
  },

  // Soft delete user
  delete: (id: string): boolean => {
    const result = softDeleteUser.run(new Date().toISOString(), id);
    return result.changes > 0;
  },

  // Close database connection
  close: () => {
    db.close();
  }
};

// Graceful shutdown
process.on('exit', () => userDb.close());
process.on('SIGINT', () => {
  userDb.close();
  process.exit(0);
});
process.on('SIGTERM', () => {
  userDb.close();
  process.exit(0);
});
