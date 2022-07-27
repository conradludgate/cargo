use crate::core::{PackageId, SourceId};
use crate::sources::registry::download;
use crate::sources::registry::MaybeLock;
use crate::sources::registry::{LoadResponse, RegistryConfig, RegistryData};
use crate::util::errors::CargoResult;
use crate::util::{Config, Filesystem};
use log::{debug, trace};
use std::fs::File;
use std::path::Path;
use std::str;
use std::task::Poll;

use super::git_index::GitIndex;

/// A remote registry is a registry that lives at a remote URL (such as
/// crates.io). The git index is cloned locally, and `.crate` files are
/// downloaded as needed and cached locally.
pub struct RemoteRegistry<'cfg> {
    index: GitIndex<'cfg>,
    /// Path to the cache of `.crate` files (`$CARGO_HOME/registry/path/$REG-HASH`).
    cache_path: Filesystem,
}

impl<'cfg> RemoteRegistry<'cfg> {
    pub fn new(source_id: SourceId, config: &'cfg Config, name: &str) -> RemoteRegistry<'cfg> {
        let index_path = config.registry_index_path().join(name);
        let cache_path = config.registry_cache_path().join(name);
        RemoteRegistry {
            // remote registry stores the index at the root of the repo
            index: GitIndex::new(source_id, config, index_path.clone(), index_path),
            cache_path,
        }
    }
}

impl<'cfg> RegistryData for RemoteRegistry<'cfg> {
    fn prepare(&self) -> CargoResult<()> {
        self.index.prepare()
    }

    fn index_path(&self) -> &Filesystem {
        &self.index.index_path
    }

    fn assert_index_locked<'a>(&self, path: &'a Filesystem) -> &'a Path {
        self.index.config.assert_package_cache_locked(path)
    }

    fn load(
        &mut self,
        _root: &Path,
        path: &Path,
        index_version: Option<&str>,
    ) -> Poll<CargoResult<LoadResponse>> {
        self.index.load(path, index_version)
    }

    fn config(&mut self) -> Poll<CargoResult<Option<RegistryConfig>>> {
        debug!("loading config");
        self.prepare()?;
        self.index
            .config
            .assert_package_cache_locked(&self.index.index_path);
        match self.load(Path::new(""), Path::new("config.json"), None)? {
            Poll::Ready(LoadResponse::Data { raw_data, .. }) => {
                trace!("config loaded");
                Poll::Ready(Ok(Some(serde_json::from_slice(&raw_data)?)))
            }
            Poll::Ready(_) => Poll::Ready(Ok(None)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn block_until_ready(&mut self) -> CargoResult<()> {
        self.index.block_until_ready(true)
    }

    fn invalidate_cache(&mut self) {
        if !self.index.updated {
            self.index.needs_update = true;
        }
    }

    fn is_updated(&self) -> bool {
        self.index.updated
    }

    fn download(&mut self, pkg: PackageId, checksum: &str) -> CargoResult<MaybeLock> {
        let registry_config = loop {
            match self.config()? {
                Poll::Pending => self.block_until_ready()?,
                Poll::Ready(cfg) => break cfg.unwrap(),
            }
        };

        download::download(
            &self.cache_path,
            &self.index.config,
            pkg,
            checksum,
            registry_config,
        )
    }

    fn finish_download(
        &mut self,
        pkg: PackageId,
        checksum: &str,
        data: &[u8],
    ) -> CargoResult<File> {
        download::finish_download(&self.cache_path, &self.index.config, pkg, checksum, data)
    }

    fn is_crate_downloaded(&self, pkg: PackageId) -> bool {
        download::is_crate_downloaded(&self.cache_path, &self.index.config, pkg)
    }
}
