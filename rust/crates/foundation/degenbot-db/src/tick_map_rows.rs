//! Low-level concentrated-liquidity map row storage for database fixtures and inspection.

use hashbrown::HashMap;

use alloy::primitives::{U128, U256};
use degenbot_math::cl::liquidity_mapping::{BitmapAtWord, LiquidityAtTick};

use crate::connection::DegenbotDb;
use crate::error::DbError;
use crate::rows::decode::encode_u256;
use crate::schema::table::v2_v3_subclass_table;
use crate::schema::table::{
    INITIALIZATION_MAPS, LIQUIDITY_POSITIONS, MANAGED_POOL_INITIALIZATION_MAPS,
    MANAGED_POOL_LIQUIDITY_POSITIONS, POOLS, UNISWAP_V4_POOLS,
};

const SQLITE_MAX_VARIABLES: usize = 32_766;
const POSITION_KEYS_PER_ROW: usize = 4;
const INIT_MAP_KEYS_PER_ROW: usize = 3;

impl DegenbotDb {
    /// Set the V3 pool's `liquidity_update_block`/`liquidity_update_log_index`
    /// stamp to `(block, log_index)` — mirrors the Python
    /// `pool_in_db.liquidity_update_block = ...`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on an UPDATE failure.
    pub fn set_v3_liquidity_update_marker(
        &self,
        pool_id: i64,
        block: u64,
        log_index: u64,
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::set_v3_liquidity_update_marker_on_conn(&conn, pool_id, block, log_index)
    }

    /// The single-transaction-bound variant of
    /// [`Self::set_v3_liquidity_update_marker`].
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn set_v3_liquidity_update_marker_on_conn(
        conn: &rusqlite::Connection,
        pool_id: i64,
        block: u64,
        log_index: u64,
    ) -> Result<(), DbError> {
        // Forks store these columns in different subclass tables.
        let kind: String = conn.query_row(
            &format!("SELECT kind FROM {POOLS} WHERE id = ?1"),
            rusqlite::params![pool_id],
            |r| r.get(0),
        )?;
        let subclass_table = v2_v3_subclass_table(&kind).ok_or_else(|| {
            DbError::Decode(format!(
                "pool {pool_id} has kind {kind:?}, not a V3 family discriminator"
            ))
        })?;
        conn.execute(
            &format!(
                "UPDATE {subclass_table} SET liquidity_update_block = ?1, \
                 liquidity_update_log_index = ?2 WHERE pool_id = ?3"
            ),
            rusqlite::params![
                i64::try_from(block).unwrap_or(i64::MAX),
                i64::try_from(log_index).unwrap_or(i64::MAX),
                pool_id
            ],
        )?;
        Ok(())
    }

    /// Set the V4 pool's `liquidity_update_block`/`liquidity_update_log_index`
    /// stamp (on the `uniswap_v4_pools` row, keyed by `managed_pool_id`).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on an UPDATE failure.
    pub fn set_v4_liquidity_update_marker(
        &self,
        managed_pool_id: i64,
        block: u64,
        log_index: u64,
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::set_v4_liquidity_update_marker_on_conn(&conn, managed_pool_id, block, log_index)
    }

    /// The single-transaction-bound variant of
    /// [`Self::set_v4_liquidity_update_marker`].
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn set_v4_liquidity_update_marker_on_conn(
        conn: &rusqlite::Connection,
        managed_pool_id: i64,
        block: u64,
        log_index: u64,
    ) -> Result<(), DbError> {
        conn.execute(
            &format!(
                "UPDATE {UNISWAP_V4_POOLS} SET liquidity_update_block = ?1, \
                 liquidity_update_log_index = ?2 WHERE managed_pool_id = ?3"
            ),
            rusqlite::params![
                i64::try_from(block).unwrap_or(i64::MAX),
                i64::try_from(log_index).unwrap_or(i64::MAX),
                managed_pool_id
            ],
        )?;
        Ok(())
    }

    /// Delete liquidity positions for ticks NOT in `live_ticks` (the V3
    /// `liquidity_positions` table, keyed by `pool_id`). Mirrors the Python
    /// `delete(...).where(pool_id==?, tick.in_(ticks_to_drop))`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on a DELETE failure.
    pub fn delete_stale_v3_positions(
        &self,
        pool_id: i64,
        live_ticks: &[i32],
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::delete_stale_v3_positions_on_conn(&conn, pool_id, live_ticks)
    }

    /// The single-transaction-bound variant of [`Self::delete_stale_v3_positions`]
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn delete_stale_v3_positions_on_conn(
        conn: &rusqlite::Connection,
        pool_id: i64,
        live_ticks: &[i32],
    ) -> Result<(), DbError> {
        delete_stale_rows(
            conn,
            LIQUIDITY_POSITIONS,
            "pool_id",
            "tick",
            pool_id,
            live_ticks,
        )
    }

    /// Delete init maps for words NOT in `live_words` (the V3
    /// `initialization_maps` table, keyed by `pool_id`).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on a DELETE failure.
    pub fn delete_stale_v3_init_maps(
        &self,
        pool_id: i64,
        live_words: &[i32],
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::delete_stale_v3_init_maps_on_conn(&conn, pool_id, live_words)
    }

    /// The single-transaction-bound variant of [`Self::delete_stale_v3_init_maps`]
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn delete_stale_v3_init_maps_on_conn(
        conn: &rusqlite::Connection,
        pool_id: i64,
        live_words: &[i32],
    ) -> Result<(), DbError> {
        delete_stale_rows(
            conn,
            INITIALIZATION_MAPS,
            "pool_id",
            "word",
            pool_id,
            live_words,
        )
    }

    /// Delete V4 liquidity positions for ticks NOT in `live_ticks`
    /// (`managed_pool_liquidity_positions`, keyed by `managed_pool_id`).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on a DELETE failure.
    pub fn delete_stale_v4_positions(
        &self,
        managed_pool_id: i64,
        live_ticks: &[i32],
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::delete_stale_v4_positions_on_conn(&conn, managed_pool_id, live_ticks)
    }

    /// The single-transaction-bound variant of [`Self::delete_stale_v4_positions`]
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn delete_stale_v4_positions_on_conn(
        conn: &rusqlite::Connection,
        managed_pool_id: i64,
        live_ticks: &[i32],
    ) -> Result<(), DbError> {
        delete_stale_rows(
            conn,
            MANAGED_POOL_LIQUIDITY_POSITIONS,
            "managed_pool_id",
            "tick",
            managed_pool_id,
            live_ticks,
        )
    }

    /// Delete V4 init maps for words NOT in `live_words`
    /// (`managed_pool_initialization_maps`, keyed by `managed_pool_id`).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on a DELETE failure.
    pub fn delete_stale_v4_init_maps(
        &self,
        managed_pool_id: i64,
        live_words: &[i32],
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::delete_stale_v4_init_maps_on_conn(&conn, managed_pool_id, live_words)
    }

    /// The single-transaction-bound variant of [`Self::delete_stale_v4_init_maps`]
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn delete_stale_v4_init_maps_on_conn(
        conn: &rusqlite::Connection,
        managed_pool_id: i64,
        live_words: &[i32],
    ) -> Result<(), DbError> {
        delete_stale_rows(
            conn,
            MANAGED_POOL_INITIALIZATION_MAPS,
            "managed_pool_id",
            "word",
            managed_pool_id,
            live_words,
        )
    }

    /// Upsert the V3 `liquidity_positions` rows for `pool_id` from the live
    /// `tick_data` map. Chunked to respect `SQLite`'s 32,766-variable limit
    /// (4 vars/row → ≤ 7,500 rows/chunk). Mirrors the Python
    /// `sqlite_upsert(LiquidityPositionTable).values([...]).on_conflict_do_update(...)`.
    ///
    /// The `on_conflict_do_update`'s `where != excl.net || != excl.gross`
    /// guard is implicit: `SQLite`'s `ON CONFLICT DO UPDATE` writes the new
    /// values unconditionally on conflict (the Python `where` skips no-op
    /// writes; the Rust path writes every time, which is observationally
    /// identical — the resulting row's net/gross equals the conflict-excluded
    /// value either way).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on any upsert failure.
    pub fn upsert_v3_liquidity_positions(
        &self,
        pool_id: i64,
        tick_data: &HashMap<i32, LiquidityAtTick>,
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::upsert_v3_liquidity_positions_on_conn(&conn, pool_id, tick_data)
    }

    /// The single-transaction-bound variant of
    /// [`Self::upsert_v3_liquidity_positions`].
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn upsert_v3_liquidity_positions_on_conn(
        conn: &rusqlite::Connection,
        pool_id: i64,
        tick_data: &HashMap<i32, LiquidityAtTick>,
    ) -> Result<(), DbError> {
        if tick_data.is_empty() {
            return Ok(());
        }
        let chunk_cap = SQLITE_MAX_VARIABLES / POSITION_KEYS_PER_ROW;
        let mut entries: Vec<(i32, &LiquidityAtTick)> =
            tick_data.iter().map(|(t, v)| (*t, v)).collect();
        // Deterministic write order: the chunk apply is replayed against
        // golden captures (ADR-068 D3/D6), so both the statement's VALUES
        // order and the autoincrement row ids it assigns must not depend on
        // hashbrown iteration order.
        entries.sort_by_key(|(t, _)| *t);
        for chunk in entries.chunks(chunk_cap) {
            let placeholders = (0..chunk.len())
                .map(|i| {
                    format!(
                        "(?{}, ?{}, ?{}, ?{})",
                        i * 4 + 1,
                        i * 4 + 2,
                        i * 4 + 3,
                        i * 4 + 4
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO {LIQUIDITY_POSITIONS} (pool_id, tick, liquidity_net, \
                 liquidity_gross) VALUES {placeholders} \
                 ON CONFLICT (pool_id, tick) DO UPDATE SET \
                 liquidity_net = excluded.liquidity_net, \
                 liquidity_gross = excluded.liquidity_gross \
                 WHERE liquidity_positions.liquidity_net != excluded.liquidity_net \
                   OR liquidity_positions.liquidity_gross != excluded.liquidity_gross"
            );
            // Build owned row-value buffers first (the &dyn ToSql params outlive the loop body).
            let row_bufs: Vec<(i64, i32, String, String)> = chunk
                .iter()
                .map(|(tick, lat)| {
                    (
                        pool_id,
                        *tick,
                        lat.liquidity_net.to_string(),
                        encode_u256(&u128_to_u256(lat.liquidity_gross)),
                    )
                })
                .collect();
            let params: Vec<&dyn rusqlite::ToSql> = row_bufs
                .iter()
                .flat_map(|(pid, tick, net, gross)| [pid as &dyn rusqlite::ToSql, tick, net, gross])
                .collect();
            let rows = conn.execute(&sql, rusqlite::params_from_iter(params))?;
            debug_assert!(
                rows <= chunk.len(),
                "upsert wrote {rows} for {} rows",
                chunk.len()
            );
        }
        Ok(())
    }

    /// Upsert the V4 `managed_pool_liquidity_positions` rows for
    /// `managed_pool_id` from `tick_data`. See
    /// [`Self::upsert_v3_liquidity_positions`] for the chunking + conflict
    /// semantics (V4 differs only in the table + key column name).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on any upsert failure.
    pub fn upsert_v4_liquidity_positions(
        &self,
        managed_pool_id: i64,
        tick_data: &HashMap<i32, LiquidityAtTick>,
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::upsert_v4_liquidity_positions_on_conn(&conn, managed_pool_id, tick_data)
    }

    /// The single-transaction-bound variant of
    /// [`Self::upsert_v4_liquidity_positions`].
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn upsert_v4_liquidity_positions_on_conn(
        conn: &rusqlite::Connection,
        managed_pool_id: i64,
        tick_data: &HashMap<i32, LiquidityAtTick>,
    ) -> Result<(), DbError> {
        if tick_data.is_empty() {
            return Ok(());
        }
        let chunk_cap = SQLITE_MAX_VARIABLES / POSITION_KEYS_PER_ROW;
        // Deterministic write order (see the V3 positions upsert — golden
        // captures replay this statement, so VALUES order + row ids must be
        // stable across runs).
        let mut entries: Vec<(i32, &LiquidityAtTick)> =
            tick_data.iter().map(|(t, v)| (*t, v)).collect();
        entries.sort_by_key(|(t, _)| *t);
        for chunk in entries.chunks(chunk_cap) {
            let placeholders = (0..chunk.len())
                .map(|i| {
                    format!(
                        "(?{}, ?{}, ?{}, ?{})",
                        i * 4 + 1,
                        i * 4 + 2,
                        i * 4 + 3,
                        i * 4 + 4
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO {MANAGED_POOL_LIQUIDITY_POSITIONS} (managed_pool_id, tick, \
                 liquidity_net, liquidity_gross) VALUES {placeholders} \
                 ON CONFLICT (managed_pool_id, tick) DO UPDATE SET \
                 liquidity_net = excluded.liquidity_net, \
                 liquidity_gross = excluded.liquidity_gross \
                 WHERE managed_pool_liquidity_positions.liquidity_net != excluded.liquidity_net \
                   OR managed_pool_liquidity_positions.liquidity_gross != excluded.liquidity_gross"
            );
            let row_bufs: Vec<(i64, i32, String, String)> = chunk
                .iter()
                .map(|(tick, lat)| {
                    (
                        managed_pool_id,
                        *tick,
                        lat.liquidity_net.to_string(),
                        encode_u256(&u128_to_u256(lat.liquidity_gross)),
                    )
                })
                .collect();
            let params: Vec<&dyn rusqlite::ToSql> = row_bufs
                .iter()
                .flat_map(|(pid, tick, net, gross)| [pid as &dyn rusqlite::ToSql, tick, net, gross])
                .collect();
            conn.execute(&sql, rusqlite::params_from_iter(params))?;
        }
        Ok(())
    }

    /// Upsert the V3 `initialization_maps` rows for `pool_id` from the live
    /// `tick_bitmap` (skips entries whose `bitmap == U256::ZERO` — the Python
    /// `if map_.bitmap != 0` filter). Chunked (3 vars/row → ≤ 10,000 rows/chunk).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on any upsert failure.
    pub fn upsert_v3_initialization_maps(
        &self,
        pool_id: i64,
        tick_bitmap: &HashMap<i32, BitmapAtWord>,
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::upsert_v3_initialization_maps_on_conn(&conn, pool_id, tick_bitmap)
    }

    /// The single-transaction-bound variant of
    /// [`Self::upsert_v3_initialization_maps`].
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn upsert_v3_initialization_maps_on_conn(
        conn: &rusqlite::Connection,
        pool_id: i64,
        tick_bitmap: &HashMap<i32, BitmapAtWord>,
    ) -> Result<(), DbError> {
        upsert_init_maps_impl(conn, pool_id, tick_bitmap, INITIALIZATION_MAPS, "pool_id")
    }

    /// Upsert the V4 `managed_pool_initialization_maps` rows for
    /// `managed_pool_id` from the live `tick_bitmap` (skips zero-bitmap
    /// entries). See [`Self::upsert_v3_initialization_maps`].
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlite`] on any upsert failure.
    pub fn upsert_v4_initialization_maps(
        &self,
        managed_pool_id: i64,
        tick_bitmap: &HashMap<i32, BitmapAtWord>,
    ) -> Result<(), DbError> {
        let conn = self.lock();
        Self::upsert_v4_initialization_maps_on_conn(&conn, managed_pool_id, tick_bitmap)
    }

    /// The single-transaction-bound variant of
    /// [`Self::upsert_v4_initialization_maps`].
    /// # Errors
    ///
    /// Same error conditions as the `&self` wrapper variant.
    pub fn upsert_v4_initialization_maps_on_conn(
        conn: &rusqlite::Connection,
        managed_pool_id: i64,
        tick_bitmap: &HashMap<i32, BitmapAtWord>,
    ) -> Result<(), DbError> {
        upsert_init_maps_impl(
            conn,
            managed_pool_id,
            tick_bitmap,
            MANAGED_POOL_INITIALIZATION_MAPS,
            "managed_pool_id",
        )
    }
}

/// Shared stale-row deleter for the V3/V4 liquidity positions + init-maps.
/// Deletes every row for `id_value` whose `key_col` is NOT in `live_keys`
/// (the ticks/words still present in the freshly-applied `tick_data`/
/// `tick_bitmap`). Binds `id_value` then the live keys. Accepts a
/// borrowed [`rusqlite::Connection`] (the chunk-loop `Transaction` derefs to
/// one) so the delete runs on the chunk's single owned connection — no re-lock.
///
/// Builds the SQL internally from the table + column identifiers so the
/// `live_keys.is_empty()` branch (a fully-drained pool — every tick pruned)
/// can issue the bare `DELETE FROM {table} WHERE {id_col} = ?1` (drop ALL rows),
/// matching the Python `db_keys - helper_keys` complement-delete-all semantics
/// (helper empty → drop all). The bare form avoids the SQL syntax error of an
/// empty `NOT IN ()` placeholder list.
fn delete_stale_rows(
    conn: &rusqlite::Connection,
    table: &str,
    id_col: &str,
    key_col: &str,
    id_value: i64,
    live_keys: &[i32],
) -> Result<(), DbError> {
    if live_keys.is_empty() {
        // Fully-drained pool → no live keys → delete ALL rows for this pool.
        conn.execute(
            &format!("DELETE FROM {table} WHERE {id_col} = ?1"),
            rusqlite::params![id_value],
        )?;
        return Ok(());
    }
    // Chunk the NOT IN list across SQLite's 32,766-var limit.
    let chunk_cap = SQLITE_MAX_VARIABLES.saturating_sub(1);
    for chunk in live_keys.chunks(chunk_cap) {
        let placeholders = sql_placeholders_for(chunk.len());
        let chunk_sql = format!(
            "DELETE FROM {table} WHERE {id_col} = ?1 AND {key_col} NOT IN ({placeholders})"
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() + 1);
        params.push(&id_value);
        for k in chunk {
            params.push(k);
        }
        conn.execute(&chunk_sql, rusqlite::params_from_iter(params))?;
    }
    Ok(())
}

/// Shared `upsert_init_maps` impl — V3/V4 differ only in the table name + the
/// id column name. Accepts a borrowed [`rusqlite::Connection`].
fn upsert_init_maps_impl(
    conn: &rusqlite::Connection,
    id_value: i64,
    tick_bitmap: &HashMap<i32, BitmapAtWord>,
    table: &str,
    id_col: &str,
) -> Result<(), DbError> {
    let mut entries: Vec<(i32, &BitmapAtWord)> = tick_bitmap
        .iter()
        .filter(|(_, bw)| bw.bitmap != U256::ZERO)
        .map(|(w, bw)| (*w, bw))
        .collect();
    // Deterministic write order (see the positions upserts — golden captures
    // replay this statement, so VALUES order + row ids must be stable).
    entries.sort_by_key(|(w, _)| *w);
    if entries.is_empty() {
        return Ok(());
    }
    let chunk_cap = SQLITE_MAX_VARIABLES / INIT_MAP_KEYS_PER_ROW;
    for chunk in entries.chunks(chunk_cap) {
        let placeholders = (0..chunk.len())
            .map(|i| format!("(?{}, ?{}, ?{})", i * 3 + 1, i * 3 + 2, i * 3 + 3))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {table} ({id_col}, word, bitmap) VALUES {placeholders} \
             ON CONFLICT ({id_col}, word) DO UPDATE SET bitmap = excluded.bitmap \
             WHERE {table}.bitmap != excluded.bitmap"
        );
        let row_bufs: Vec<(i64, i32, String)> = chunk
            .iter()
            .map(|(word, bw)| (id_value, *word, encode_u256(&bw.bitmap)))
            .collect();
        let params: Vec<&dyn rusqlite::ToSql> = row_bufs
            .iter()
            .flat_map(|(pid, word, bitmap)| [pid as &dyn rusqlite::ToSql, word, bitmap])
            .collect();
        conn.execute(&sql, rusqlite::params_from_iter(params))?;
    }
    Ok(())
}

/// Build `?, ?, ...` (n placeholders) for a SQL `IN (...)` clause.
fn sql_placeholders_for(n: usize) -> String {
    vec!["?"; n].join(", ")
}

/// Widen a [`U128`] back to [`U256`] (the DB write form).
#[must_use]
fn u128_to_u256(v: U128) -> U256 {
    // U128 is 16 bytes; widen to U256's 32-byte big-endian by zero-padding
    // the high 16 bytes (matches the Python `int`-round-trip: the low-128
    // gross value is stored as its decimal `str(int)`, which `decode_u256`
    // parses back to the same U256).
    let mut arr = [0u8; 32];
    arr[16..32].copy_from_slice(&v.to_be_bytes::<16>());
    U256::from_be_bytes(arr)
}
