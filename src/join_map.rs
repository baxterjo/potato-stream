use std::{collections::HashMap, future::Future};

use tokio::task::{AbortHandle, Id, JoinError, JoinSet};
use tracing::warn;

/// Naive join map implementation. Official JoinMap by Tokio is actively being stabilized.
///
/// https://github.com/tokio-rs/tokio/pull/7075
pub struct JoinMap<T> {
    name_map: HashMap<Id, String>,
    handle_map: HashMap<String, AbortHandle>,
    set: JoinSet<T>,
}

impl<T> JoinMap<T> {
    /// Create a new `JoinMap`
    pub fn new() -> Self {
        Self {
            name_map: HashMap::new(),
            handle_map: HashMap::new(),
            set: JoinSet::new(),
        }
    }
    /// Returns the number of tasks currently in the `JoinMap`
    pub fn len(&self) -> usize {
        self.set.len()
    }
    /// Returns whether the `JoinMap` is empty.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

impl<T: 'static> JoinMap<T> {
    pub fn spawn<F>(&mut self, name: &str, task: F)
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        let handle = self.set.spawn(task);
        if let Some(old_val) = self.name_map.insert(handle.id(), name.to_owned()) {
            warn!(?old_val, "Name map was out of sync.");
        }
        if let Some(old_val) = self.handle_map.insert(name.to_string(), handle) {
            warn!(?old_val, "Handle map was out of sync!")
        }
    }

    pub async fn join_next(&mut self) -> Option<(String, Result<T, JoinError>)> {
        match self.set.join_next_with_id().await {
            None => {
                return None;
            }
            Some(Ok((id, output))) => {
                let name = self
                    .name_map
                    .remove(&id)
                    .expect("Name map was out of sync!");
                self.handle_map.remove(&name);
                Some((name, Ok(output)))
            }
            Some(Err(error)) => {
                let id = error.id();
                let name = self
                    .name_map
                    .remove(&id)
                    .expect("Name map was out of sync!");
                self.handle_map.remove(&name);
                Some((name, Err(error)))
            }
        }
    }

    pub fn abort_all(&mut self) {
        self.set.abort_all();
    }
}
