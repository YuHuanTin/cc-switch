//! 数据库模块 - SQLite 数据持久化
//!
//! 此模块提供应用的核心数据存储功能，包括：
//! - 供应商配置管理
//! - MCP 服务器配置
//! - 提示词管理
//! - Skills 管理
//! - 通用设置存储
//!
//! ## 架构设计
//!
//! ```text
//! database/
//! ├── mod.rs        - Database 结构体 + 初始化
//! ├── schema.rs     - 表结构定义 + Schema 迁移
//! ├── backup.rs     - SQL 导入导出 + 快照备份
//! ├── migration.rs  - JSON → SQLite 数据迁移
//! └── dao/          - 数据访问对象
//!     ├── providers.rs
//!     ├── mcp.rs
//!     ├── prompts.rs
//!     ├── skills.rs
//!     └── settings.rs
//! ```

pub(crate) mod backup;
mod dao;
mod migration;
mod schema;

#[cfg(test)]
mod tests;

// DAO 类型导出供外部使用
pub(crate) use dao::providers_seed::{
    is_official_seed_id, CLAUDE_DESKTOP_OFFICIAL_PROVIDER_ID, CODEX_OFFICIAL_PROVIDER_ID,
    GROKBUILD_OFFICIAL_PROVIDER_ID,
};
pub(crate) use dao::proxy::{
    validate_cost_multiplier, validate_pricing_source, PRICING_SOURCE_REQUEST,
    PRICING_SOURCE_RESPONSE,
};
pub use dao::FailoverQueueItem;
pub use dao::Profile;

use crate::config::get_app_config_dir;
use crate::error::AppError;
use rusqlite::{hooks::Action, Connection};
use serde::Serialize;
use std::path::Path;
use std::sync::Mutex;

// DAO 方法通过 impl Database 提供，无需额外导出

/// 当前 Schema 版本号
/// 每次修改表结构时递增，并在 schema.rs 中添加相应的迁移逻辑
pub(crate) const SCHEMA_VERSION: i32 = 19;

/// 独立日志数据库的版本号。
///
/// 日志库从当前完整结构开始创建，不复用配置库的历史迁移版本，避免
/// 配置库的 user_version 因日志结构变化再次增长。
pub(crate) const LOG_SCHEMA_VERSION: i32 = 1;

/// 安全地序列化 JSON，避免 unwrap panic
pub(crate) fn to_json_string<T: Serialize>(value: &T) -> Result<String, AppError> {
    serde_json::to_string(value)
        .map_err(|e| AppError::Config(format!("JSON serialization failed: {e}")))
}

/// 安全地获取 Mutex 锁，避免 unwrap panic
macro_rules! lock_conn {
    ($mutex:expr) => {
        $mutex
            .lock()
            .map_err(|e| AppError::Database(format!("Mutex lock failed: {}", e)))?
    };
}

// 导出宏供子模块使用
pub(crate) use lock_conn;

/// 安全地获取日志数据库连接锁。
macro_rules! lock_logs_conn {
    ($mutex:expr) => {
        $mutex
            .lock()
            .map_err(|e| AppError::Database(format!("日志数据库 Mutex lock failed: {}", e)))?
    };
}

pub(crate) use lock_logs_conn;

/// 数据库连接封装
///
/// 使用 Mutex 包装 Connection 以支持在多线程环境（如 Tauri State）中共享。
/// rusqlite::Connection 本身不是 Sync 的，因此需要这层包装。
pub struct Database {
    pub(crate) conn: Mutex<Connection>,
    pub(crate) logs_conn: Mutex<Connection>,
}

fn register_db_change_hook(conn: &Connection) {
    conn.update_hook(Some(
        |action: Action, _database: &str, table: &str, _row_id: i64| match action {
            Action::SQLITE_INSERT | Action::SQLITE_UPDATE | Action::SQLITE_DELETE => {
                crate::services::webdav_auto_sync::notify_db_changed(table);
                crate::services::s3_auto_sync::notify_db_changed(table);
            }
            _ => {}
        },
    ));
}

impl Database {
    /// 初始化数据库连接并创建表
    ///
    /// 数据库文件位于 `~/.cc-switch/cc-switch.db`
    pub fn init() -> Result<Self, AppError> {
        let db_path = get_app_config_dir().join("cc-switch.db");
        let db_exists = db_path.exists();
        let logs_path = get_app_config_dir().join("cc-switch-logs.db");
        let logs_exists = logs_path.exists();

        // 确保父目录存在
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| AppError::io(parent, e))?;
        }

        let conn = Connection::open(&db_path).map_err(|e| AppError::Database(e.to_string()))?;

        // 启用外键约束
        conn.execute("PRAGMA foreign_keys = ON;", [])
            .map_err(|e| AppError::Database(e.to_string()))?;
        if !db_exists {
            // For a brand-new database, configure incremental auto-vacuum
            // before creating any tables so no rebuild is needed later.
            conn.execute("PRAGMA auto_vacuum = INCREMENTAL;", [])
                .map_err(|e| AppError::Database(e.to_string()))?;
        }

        let logs_conn = Connection::open(&logs_path)
            .map_err(|e| AppError::Database(format!("打开日志数据库失败: {e}")))?;
        logs_conn
            .execute("PRAGMA foreign_keys = ON;", [])
            .map_err(|e| AppError::Database(format!("启用日志库外键约束失败: {e}")))?;
        if !logs_exists {
            logs_conn
                .execute("PRAGMA auto_vacuum = INCREMENTAL;", [])
                .map_err(|e| AppError::Database(format!("配置日志库 auto_vacuum 失败: {e}")))?;
        }

        let db = Self {
            conn: Mutex::new(conn),
            logs_conn: Mutex::new(logs_conn),
        };

        let config_version = {
            let conn = lock_conn!(db.conn);
            Self::get_user_version(&conn)?
        };
        let has_legacy_logs = {
            let conn = lock_conn!(db.conn);
            Self::has_legacy_log_tables(&conn)?
        };

        if config_version > SCHEMA_VERSION {
            return Err(AppError::Database(format!(
                "数据库版本过新（{config_version}），当前应用仅支持 {SCHEMA_VERSION}，请升级应用后再尝试。"
            )));
        }

        if has_legacy_logs {
            log::info!(
                "Creating pre-split database backup (v{config_version} → split log database)"
            );
            let backup_path = db
                .backup_database_file()?
                .ok_or_else(|| AppError::Database("拆分日志表前未能创建数据库备份".to_string()))?;
            log::info!(
                "Pre-split database backup created at {}",
                backup_path.display()
            );
        }

        if has_legacy_logs {
            // 先用原有完整 schema 迁移逻辑把旧库升级到当前结构，再拆出日志表。
            // 这样历史 v1-v19 数据迁移不会被新库的独立版本号打断。
            let conn = lock_conn!(db.conn);
            Self::create_tables_on_conn(&conn)?;
            Self::apply_schema_migrations_on_conn(&conn)?;
        } else {
            let conn = lock_conn!(db.conn);
            Self::create_config_tables_on_conn(&conn)?;
            if config_version < SCHEMA_VERSION {
                // 仅存在配置表但版本较旧的极端旧库仍复用历史迁移逻辑；临时补上日志表
                // 是为了兼容其中引用旧日志表的迁移步骤，迁移完成后立即删除。
                Self::create_log_tables_on_conn(&conn)?;
                Self::apply_schema_migrations_on_conn(&conn)?;
                Self::drop_legacy_log_tables(&conn)?;
            }
            if config_version == 0 {
                Self::set_user_version(&conn, SCHEMA_VERSION)?;
            }
        }

        {
            let logs_conn = lock_logs_conn!(db.logs_conn);
            Self::apply_log_schema_migrations_on_conn(&logs_conn)?;
        }

        if has_legacy_logs {
            db.migrate_legacy_log_tables(&db_path)?;
        }
        db.refresh_provider_catalog_cache()?;

        {
            let conn = lock_conn!(db.conn);
            register_db_change_hook(&conn);
        }

        if let Err(e) = db.ensure_incremental_auto_vacuum() {
            log::warn!("Failed to ensure incremental auto-vacuum: {e}");
        }
        db.ensure_model_pricing_seeded()?;
        if let Err(e) = crate::services::model_pricing::sync_local_model_pricing(&db) {
            log::warn!("Failed to sync local model pricing file: {e}");
        }

        // Startup cleanup: prune old logs and reclaim space in the independent log database.
        if let Err(e) = db.cleanup_old_stream_check_logs(7) {
            log::warn!("Startup stream_check_logs cleanup failed: {e}");
        }
        if let Err(e) = db.rollup_and_prune(30) {
            log::warn!("Startup rollup_and_prune failed: {e}");
        }
        // Reclaim disk space after cleanup
        {
            let logs_conn = lock_logs_conn!(db.logs_conn);
            if let Err(e) = logs_conn.execute_batch("PRAGMA incremental_vacuum;") {
                log::warn!("Startup incremental vacuum failed: {e}");
            }
        }

        Ok(db)
    }

    /// 读取磁盘上数据库的 `user_version`；仅当它比应用支持的 [`SCHEMA_VERSION`]
    /// 更新时返回 `Some(version)`。
    ///
    /// 用于初始化失败后判断是否为「数据库版本过新（应用过旧，需升级应用）」的可恢复
    /// 场景——此时不应反复弹出无效的重试对话框，而应引导用户在应用内升级。
    pub fn stored_user_version_exceeds_supported(
        db_path: &std::path::Path,
    ) -> Result<Option<i32>, AppError> {
        if !db_path.exists() {
            return Ok(None);
        }
        let conn = Connection::open(db_path).map_err(|e| AppError::Database(e.to_string()))?;
        let version = Self::get_user_version(&conn)?;
        Ok((version > SCHEMA_VERSION).then_some(version))
    }

    /// 创建内存数据库（用于测试）
    pub fn memory() -> Result<Self, AppError> {
        let conn = Connection::open_in_memory().map_err(|e| AppError::Database(e.to_string()))?;
        let logs_conn =
            Connection::open_in_memory().map_err(|e| AppError::Database(e.to_string()))?;

        // 启用外键约束
        conn.execute("PRAGMA foreign_keys = ON;", [])
            .map_err(|e| AppError::Database(e.to_string()))?;
        conn.execute("PRAGMA auto_vacuum = INCREMENTAL;", [])
            .map_err(|e| AppError::Database(e.to_string()))?;
        logs_conn
            .execute("PRAGMA foreign_keys = ON;", [])
            .map_err(|e| AppError::Database(e.to_string()))?;

        let db = Self {
            conn: Mutex::new(conn),
            logs_conn: Mutex::new(logs_conn),
        };
        db.create_tables()?;
        {
            let conn = lock_conn!(db.conn);
            Self::set_user_version(&conn, SCHEMA_VERSION)?;
            register_db_change_hook(&conn);
        }
        {
            let logs_conn = lock_logs_conn!(db.logs_conn);
            Self::set_user_version(&logs_conn, LOG_SCHEMA_VERSION)?;
        }
        db.refresh_provider_catalog_cache()?;
        db.ensure_model_pricing_seeded()?;

        Ok(db)
    }

    fn has_legacy_log_tables(conn: &Connection) -> Result<bool, AppError> {
        for table in [
            "proxy_request_logs",
            "usage_daily_rollups",
            "stream_check_logs",
            "provider_health",
            "session_log_sync",
            "session_usage_dedup",
        ] {
            if Self::table_exists(conn, table)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn drop_legacy_log_tables(conn: &Connection) -> Result<(), AppError> {
        for table in [
            "provider_health",
            "proxy_request_logs",
            "stream_check_logs",
            "usage_daily_rollups",
            "session_log_sync",
            "session_usage_dedup",
            "provider_catalog_cache",
        ] {
            conn.execute(&format!("DROP TABLE IF EXISTS {table}"), [])
                .map_err(|e| AppError::Database(format!("删除旧日志表 {table} 失败: {e}")))?;
        }
        Ok(())
    }

    fn migrate_legacy_log_tables(&self, legacy_path: &Path) -> Result<(), AppError> {
        let conn = lock_conn!(self.conn);
        let logs_conn = lock_logs_conn!(self.logs_conn);

        logs_conn
            .execute(
                "ATTACH DATABASE ?1 AS legacy",
                [legacy_path.to_string_lossy().as_ref()],
            )
            .map_err(|e| AppError::Database(format!("挂载旧配置数据库失败: {e}")))?;

        let tables = [
            (
                "provider_health",
                "provider_id, app_type, is_healthy, consecutive_failures, last_success_at, last_failure_at, last_error, updated_at",
            ),
            (
                "proxy_request_logs",
                "request_id, provider_id, app_type, model, request_model, pricing_model, input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, input_token_semantics, input_cost_usd, output_cost_usd, cache_read_cost_usd, cache_creation_cost_usd, total_cost_usd, latency_ms, first_token_ms, duration_ms, status_code, error_message, session_id, provider_type, is_streaming, cost_multiplier, created_at, data_source",
            ),
            (
                "stream_check_logs",
                "id, provider_id, provider_name, app_type, status, success, message, response_time_ms, http_status, model_used, retry_count, tested_at",
            ),
            (
                "usage_daily_rollups",
                "date, app_type, provider_id, model, request_model, pricing_model, request_count, success_count, input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, input_token_semantics, total_cost_usd, avg_latency_ms",
            ),
            (
                "session_log_sync",
                "file_path, last_modified, last_line_offset, last_synced_at, last_byte_offset, last_tail_fingerprint",
            ),
            (
                "session_usage_dedup",
                "data_source, request_id, semantic_id, has_entry_id",
            ),
        ];

        for (table, columns) in tables {
            if !Self::table_exists(&conn, table)? {
                continue;
            }
            let sql = format!(
                "INSERT OR REPLACE INTO {table} ({columns}) SELECT {columns} FROM legacy.{table}"
            );
            logs_conn
                .execute(&sql, [])
                .map_err(|e| AppError::Database(format!("迁移日志表 {table} 失败: {e}")))?;

            let source_count: i64 =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            let target_count: i64 =
                logs_conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
            if target_count < source_count {
                return Err(AppError::Database(format!(
                    "迁移日志表 {table} 校验失败：源 {source_count} 行，目标 {target_count} 行"
                )));
            }
        }

        let integrity: String = logs_conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .map_err(|e| AppError::Database(format!("校验日志数据库失败: {e}")))?;
        if integrity != "ok" {
            return Err(AppError::Database(format!(
                "日志数据库完整性校验失败: {integrity}"
            )));
        }

        logs_conn
            .execute_batch("DETACH DATABASE legacy;")
            .map_err(|e| AppError::Database(format!("卸载旧配置数据库失败: {e}")))?;
        Self::drop_legacy_log_tables(&conn)?;
        conn.execute_batch("VACUUM;")
            .map_err(|e| AppError::Database(format!("压缩配置数据库失败: {e}")))?;
        Ok(())
    }

    /// Refresh the tiny provider-name projection used by log-side statistics.
    /// The request/usage tables remain exclusively in `logs_conn`; this cache
    /// only avoids cross-connection joins between SQLite connections.
    pub(crate) fn refresh_provider_catalog_cache(&self) -> Result<(), AppError> {
        let providers = {
            let conn = lock_conn!(self.conn);
            let mut stmt = conn
                .prepare("SELECT id, app_type, name FROM providers")
                .map_err(|e| AppError::Database(format!("读取供应商目录失败: {e}")))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| AppError::Database(format!("读取供应商目录失败: {e}")))?;
            rows
        };

        let logs_conn = lock_logs_conn!(self.logs_conn);
        let tx = logs_conn
            .unchecked_transaction()
            .map_err(|e| AppError::Database(format!("刷新供应商目录失败: {e}")))?;
        tx.execute("DELETE FROM provider_catalog_cache", [])
            .map_err(|e| AppError::Database(format!("清理供应商目录缓存失败: {e}")))?;
        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO provider_catalog_cache (provider_id, app_type, name)
                     VALUES (?1, ?2, ?3)",
                )
                .map_err(|e| AppError::Database(format!("准备供应商目录缓存失败: {e}")))?;
            for (id, app_type, name) in providers {
                stmt.execute(rusqlite::params![id, app_type, name])
                    .map_err(|e| AppError::Database(format!("写入供应商目录缓存失败: {e}")))?;
            }
        }
        tx.commit()
            .map_err(|e| AppError::Database(format!("提交供应商目录缓存失败: {e}")))?;
        Ok(())
    }

    pub(crate) fn get_auto_vacuum_mode(conn: &Connection) -> Result<i32, AppError> {
        conn.query_row("PRAGMA auto_vacuum;", [], |row| row.get(0))
            .map_err(|e| AppError::Database(format!("读取 auto_vacuum 失败: {e}")))
    }

    fn has_user_tables(conn: &Connection) -> Result<bool, AppError> {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| AppError::Database(format!("读取表数量失败: {e}")))?;
        Ok(count > 0)
    }

    pub(crate) fn ensure_incremental_auto_vacuum_on_conn(
        conn: &Connection,
    ) -> Result<bool, AppError> {
        let mode = Self::get_auto_vacuum_mode(conn)?;
        if mode == 2 {
            return Ok(false);
        }

        let has_tables = Self::has_user_tables(conn)?;
        conn.execute("PRAGMA auto_vacuum = INCREMENTAL;", [])
            .map_err(|e| AppError::Database(format!("设置 auto_vacuum 失败: {e}")))?;

        if !has_tables {
            return Ok(false);
        }

        conn.execute("VACUUM;", [])
            .map_err(|e| AppError::Database(format!("执行 VACUUM 失败: {e}")))?;
        conn.execute("PRAGMA foreign_keys = ON;", [])
            .map_err(|e| AppError::Database(format!("恢复 foreign_keys 失败: {e}")))?;
        Ok(true)
    }

    pub(crate) fn ensure_incremental_auto_vacuum(&self) -> Result<bool, AppError> {
        let mode = {
            let conn = lock_conn!(self.conn);
            Self::get_auto_vacuum_mode(&conn)?
        };
        if mode == 2 {
            return Ok(false);
        }

        let has_tables = {
            let conn = lock_conn!(self.conn);
            Self::has_user_tables(&conn)?
        };
        if has_tables {
            log::info!(
                "Detected auto_vacuum={mode}, rebuilding database to enable incremental vacuum"
            );
            self.backup_database_file()?;
        }

        let rebuilt = {
            let conn = lock_conn!(self.conn);
            Self::ensure_incremental_auto_vacuum_on_conn(&conn)?
        };

        if rebuilt {
            log::info!("Incremental auto-vacuum enabled after database rebuild");
        } else {
            log::info!("Incremental auto-vacuum configured for new database");
        }

        Ok(rebuilt)
    }

    /// 检查 MCP 服务器表是否为空
    pub fn is_mcp_table_empty(&self) -> Result<bool, AppError> {
        let conn = lock_conn!(self.conn);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM mcp_servers", [], |row| row.get(0))
            .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(count == 0)
    }

    /// 检查提示词表是否为空
    pub fn is_prompts_table_empty(&self) -> Result<bool, AppError> {
        let conn = lock_conn!(self.conn);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM prompts", [], |row| row.get(0))
            .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(count == 0)
    }
}
