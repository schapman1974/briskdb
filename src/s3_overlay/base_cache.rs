//! Handle-local LRU of immutable SQLite bases. Never caches an S3 head or a
//! merged result; each statement still resolves the current published base ID.
use super::{ReadStats, Result};
use rusqlite::Connection;
use std::collections::VecDeque;

const CAPACITY: usize = 8;

pub(super) struct BaseCache {
    entries: VecDeque<Entry>,
    capacity: usize,
}

struct Entry {
    table: usize,
    partition: u16,
    base: String,
    connection: Connection,
}

impl Default for BaseCache {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            capacity: CAPACITY,
        }
    }
}

impl BaseCache {
    /// The registry's mutex protects all access, including statement execution.
    /// The cache is scoped to a single database root/configuration and is
    /// dropped with that handle, closing every retained connection.
    pub fn get_or_open(
        &mut self,
        table: usize,
        partition: u16,
        base: &str,
        stats: &mut ReadStats,
        open: impl FnOnce() -> Result<Connection>,
    ) -> Result<&Connection> {
        if let Some(index) = self.entries.iter().position(|entry| {
            entry.table == table && entry.partition == partition && entry.base == base
        }) {
            let entry = self.entries.remove(index).expect("located cache entry");
            self.entries.push_back(entry);
            stats.sqlite_base_cache_hits += 1;
        } else {
            if self.entries.len() == self.capacity {
                // Close before opening the replacement, bounding live handles.
                self.entries.pop_front();
                stats.sqlite_base_cache_evictions += 1;
            }
            let connection = open()?;
            connection.set_prepared_statement_cache_capacity(16);
            stats.sqlite_base_opens += 1;
            self.entries.push_back(Entry {
                table,
                partition,
                base: base.to_owned(),
                connection,
            });
        }
        Ok(&self
            .entries
            .back()
            .expect("admitted cache entry")
            .connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection() -> Result<Connection> {
        Connection::open_in_memory().map_err(super::super::storage_error)
    }

    #[test]
    fn cache_is_lru_bounded_and_uses_the_full_snapshot_identity() {
        let mut cache = BaseCache {
            capacity: 2,
            ..BaseCache::default()
        };
        let mut stats = ReadStats::default();
        cache
            .get_or_open(0, 0, "one", &mut stats, connection)
            .unwrap();
        cache
            .get_or_open(0, 0, "two", &mut stats, connection)
            .unwrap();
        cache
            .get_or_open(0, 0, "one", &mut stats, || panic!("expected cache hit"))
            .unwrap();
        cache
            .get_or_open(0, 0, "three", &mut stats, connection)
            .unwrap();
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.entries[0].base, "one");
        assert_eq!(cache.entries[1].base, "three");
        assert_eq!(
            (
                stats.sqlite_base_opens,
                stats.sqlite_base_cache_hits,
                stats.sqlite_base_cache_evictions
            ),
            (3, 1, 1)
        );
        // Same ID in another partition or table must never reuse a connection.
        cache
            .get_or_open(0, 1, "three", &mut stats, connection)
            .unwrap();
        cache
            .get_or_open(1, 1, "three", &mut stats, connection)
            .unwrap();
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(stats.sqlite_base_opens, 5);
        assert_eq!(stats.sqlite_base_cache_hits, 1);
        assert_eq!(stats.sqlite_base_cache_evictions, 3);
    }

    #[test]
    fn failed_opens_are_not_cached() {
        let mut cache = BaseCache::default();
        let mut stats = ReadStats::default();
        assert!(
            cache
                .get_or_open(0, 0, "one", &mut stats, || Err(super::super::invalid(
                    "test failure"
                )))
                .is_err()
        );
        assert!(cache.entries.is_empty());
        assert_eq!(stats.sqlite_base_opens, 0);
        cache
            .get_or_open(0, 0, "one", &mut stats, connection)
            .unwrap();
        assert_eq!(stats.sqlite_base_opens, 1);
        assert_eq!(stats.sqlite_base_cache_hits, 0);
    }
}
