import { createHash, randomBytes, scrypt as scryptCallback, timingSafeEqual } from 'node:crypto';
import { promisify } from 'node:util';
import { Router } from 'express';
import { z } from 'zod';
import { env, getConfiguredReportingApiToken } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
const scrypt = promisify(scryptCallback);
const SESSION_TOKEN_PREFIX = 'rrs_';
class AuthHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'AuthHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
const loginSchema = z.object({
    email: z.string().trim().email().transform((value) => value.toLowerCase()),
    password: z.string().min(1)
});
const createUserSchema = z.object({
    email: z.string().trim().email().transform((value) => value.toLowerCase()),
    password: z.string().min(12),
    displayName: z.string().trim().min(1).max(120),
    isAdmin: z.boolean().optional().default(false)
});
function parseInput(schema, input) {
    try {
        return schema.parse(input);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new AuthHttpError(400, 'invalid_request', 'Invalid authentication request', error.flatten());
        }
        throw error;
    }
}
function parseBearerToken(authHeader) {
    if (!authHeader) {
        return null;
    }
    const [scheme, token] = authHeader.split(/\s+/, 2);
    if (!scheme || !token || scheme.toLowerCase() !== 'bearer') {
        return null;
    }
    return token.trim() || null;
}
function computeSessionDigest(token) {
    return createHash('sha256').update(token).digest('hex');
}
async function hashPassword(password) {
    const salt = randomBytes(16).toString('base64url');
    const derivedKey = (await scrypt(password, salt, 64));
    return `scrypt$${salt}$${derivedKey.toString('base64url')}`;
}
async function verifyPassword(password, passwordHash) {
    const [algorithm, salt, encodedHash] = passwordHash.split('$');
    if (algorithm !== 'scrypt' || !salt || !encodedHash) {
        return false;
    }
    const expected = Buffer.from(encodedHash, 'base64url');
    const actual = (await scrypt(password, salt, expected.length));
    return expected.length === actual.length && timingSafeEqual(expected, actual);
}
function mapUser(row) {
    return {
        id: row.id,
        email: row.email,
        displayName: row.display_name,
        isAdmin: row.is_admin,
        status: row.status,
        lastLoginAt: row.last_login_at?.toISOString() ?? null,
        createdAt: row.created_at.toISOString()
    };
}
async function findUserByEmail(email) {
    const result = await query(`
      SELECT
        id,
        email,
        password_hash,
        display_name,
        is_admin,
        status,
        last_login_at,
        created_at
      FROM app_users
      WHERE email = $1
      LIMIT 1
    `, [email]);
    return result.rows[0] ?? null;
}
async function createUserSession(user) {
    const token = `${SESSION_TOKEN_PREFIX}${randomBytes(32).toString('hex')}`;
    const digest = computeSessionDigest(token);
    await withTransaction(async (client) => {
        await client.query(`
        INSERT INTO app_sessions (
          user_id,
          token_digest,
          expires_at
        )
        VALUES (
          $1,
          $2,
          now() + ($3::text || ' hours')::interval
        )
      `, [user.id, digest, String(env.APP_SESSION_TTL_HOURS)]);
        await client.query(`
        UPDATE app_users
        SET
          last_login_at = now(),
          updated_at = now()
        WHERE id = $1
      `, [user.id]);
    });
    return {
        token,
        user: {
            ...mapUser(user),
            lastLoginAt: new Date().toISOString()
        }
    };
}
export function isInternalServiceToken(authHeader) {
    const configuredToken = getConfiguredReportingApiToken();
    if (!configuredToken) {
        return false;
    }
    return authHeader === `Bearer ${configuredToken}`;
}
export async function resolveAuthContext(authHeader) {
    if (isInternalServiceToken(authHeader)) {
        return { kind: 'internal' };
    }
    const token = parseBearerToken(authHeader);
    if (!token) {
        return null;
    }
    const result = await query(`
      SELECT
        s.id AS session_id,
        u.id AS user_id,
        u.email,
        u.display_name,
        u.is_admin,
        u.status,
        u.last_login_at,
        u.created_at,
        s.expires_at
      FROM app_sessions s
      INNER JOIN app_users u ON u.id = s.user_id
      WHERE s.token_digest = $1
        AND s.revoked_at IS NULL
        AND s.expires_at > now()
      LIMIT 1
    `, [computeSessionDigest(token)]);
    const row = result.rows[0];
    if (!row || row.status !== 'active') {
        return null;
    }
    return {
        kind: 'user',
        sessionId: row.session_id,
        user: {
            id: row.user_id,
            email: row.email,
            displayName: row.display_name,
            isAdmin: row.is_admin,
            status: row.status,
            lastLoginAt: row.last_login_at?.toISOString() ?? null,
            createdAt: row.created_at.toISOString()
        }
    };
}
export async function attachAuthContext(req, res, next) {
    try {
        const auth = await resolveAuthContext(req.header('authorization') ?? undefined);
        res.locals.auth = auth;
        next();
    }
    catch (error) {
        next(error);
    }
}
export function requireAuthenticated(req, res, next) {
    const auth = res.locals.auth;
    if (!auth) {
        res.status(401).json({
            error: 'unauthorized',
            message: 'Authentication required'
        });
        return;
    }
    next();
}
export function requireAdmin(req, res, next) {
    const auth = res.locals.auth;
    if (!auth) {
        res.status(401).json({
            error: 'unauthorized',
            message: 'Authentication required'
        });
        return;
    }
    if (auth.kind === 'internal') {
        next();
        return;
    }
    if (!auth.user.isAdmin) {
        res.status(403).json({
            error: 'forbidden',
            message: 'Admin access required'
        });
        return;
    }
    next();
}
export function requireInternalService(req, res, next) {
    const auth = res.locals.auth;
    if (!auth) {
        res.status(401).json({
            error: 'unauthorized',
            message: 'Authentication required'
        });
        return;
    }
    if (auth.kind !== 'internal') {
        res.status(403).json({
            error: 'forbidden',
            message: 'Internal service token required'
        });
        return;
    }
    next();
}
export function createAuthRouter() {
    const router = Router();
    router.use(attachAuthContext);
    router.post('/login', async (req, res, next) => {
        try {
            const payload = parseInput(loginSchema, req.body);
            const user = await findUserByEmail(payload.email);
            if (!user || user.status !== 'active' || !(await verifyPassword(payload.password, user.password_hash))) {
                throw new AuthHttpError(401, 'invalid_credentials', 'Invalid email or password');
            }
            const session = await createUserSession(user);
            res.status(200).json({
                token: session.token,
                user: session.user
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/me', requireAuthenticated, async (_req, res) => {
        const auth = res.locals.auth;
        if (auth.kind === 'internal') {
            res.status(200).json({
                user: {
                    id: 0,
                    email: 'internal@system',
                    displayName: 'Internal service token',
                    isAdmin: true,
                    status: 'active',
                    lastLoginAt: null,
                    createdAt: new Date(0).toISOString()
                }
            });
            return;
        }
        res.status(200).json({ user: auth.user });
    });
    router.post('/logout', requireAuthenticated, async (_req, res, next) => {
        try {
            const auth = res.locals.auth;
            if (auth.kind === 'user') {
                await query(`
            UPDATE app_sessions
            SET
              revoked_at = now(),
              last_seen_at = now()
            WHERE id = $1
          `, [auth.sessionId]);
            }
            res.status(200).json({ ok: true });
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
export function createUserAdminRouter() {
    const router = Router();
    router.use(attachAuthContext);
    router.use(requireAdmin);
    router.get('/', async (_req, res, next) => {
        try {
            const result = await query(`
          SELECT
            id,
            email,
            password_hash,
            display_name,
            is_admin,
            status,
            last_login_at,
            created_at
          FROM app_users
          ORDER BY email ASC
        `);
            res.status(200).json({
                users: result.rows.map((row) => mapUser(row))
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.post('/', async (req, res, next) => {
        try {
            const payload = parseInput(createUserSchema, req.body);
            const passwordHash = await hashPassword(payload.password);
            const result = await query(`
          INSERT INTO app_users (
            email,
            password_hash,
            display_name,
            is_admin,
            status
          )
          VALUES ($1, $2, $3, $4, 'active')
          RETURNING
            id,
            email,
            password_hash,
            display_name,
            is_admin,
            status,
            last_login_at,
            created_at
        `, [payload.email, passwordHash, payload.displayName, payload.isAdmin]);
            res.status(201).json({
                user: mapUser(result.rows[0])
            });
        }
        catch (error) {
            if (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505') {
                next(new AuthHttpError(409, 'user_exists', 'A user with that email already exists'));
                return;
            }
            next(error);
        }
    });
    return router;
}
