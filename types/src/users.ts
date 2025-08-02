import { z } from "zod";
import { CollectionStreamEvent, GetCollectionParams, GetCollectionResponse, PostCollectionBody, PostCollectionResponse } from "./replication";

export const userSchema = z.object({
  id: z.string(),
  email: z.string().email(),
  status: z.string(),
  role: z.string().optional(),
  created_at: z.string(),
  updated_at: z.string(),
  _deleted: z.boolean(),
});

export type User = z.infer<typeof userSchema>;

export type GetUsersResponse = GetCollectionResponse<User>;

export type GetUsersParams = GetCollectionParams;

export type PostUsersResponse = PostCollectionResponse<User>;

export type PostUsersBody = PostCollectionBody<User>;

export type UsersStreamEvent = CollectionStreamEvent<User>;