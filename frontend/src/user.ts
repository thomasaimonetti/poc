import type { RxCollection } from "rxdb"

export interface User {
    id: string
    email: string
    status: string
    role?: string
    created_at: string
    updated_at: string
    _deleted: boolean
}

export type UsersCollectionType = RxCollection<User, {}, unknown, unknown>