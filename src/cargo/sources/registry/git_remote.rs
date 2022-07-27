use crate::core::{PackageId, SourceId};
use crate::sources::registry::download;
use crate::sources::registry::MaybeLock;
use crate::sources::registry::{LoadResponse, RegistryConfig, RegistryData};
use crate::util::errors::CargoResult;
use crate::util::{Config, Filesystem};
use cargo_util::{paths, Sha256};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::str;
use std::task::Poll;

use super::git_index::GitIndex;

/// A remote registry is a registry that stores the index AND the crate package tarballs
/// in a git repository
pub struct GitRemoteRegistry<'cfg> {
    index: GitIndex<'cfg>,
    pkg_path: Filesystem,
    src_path: Filesystem,
}

impl<'cfg> GitRemoteRegistry<'cfg> {
    pub fn new(source_id: SourceId, config: &'cfg Config, name: &str) -> GitRemoteRegistry<'cfg> {
        let repo_path = config.registry_index_path().join(name);
        let index_path = repo_path.join("index");
        let pkg_path = repo_path.join("pkg");
        let src_path = config.registry_source_path().join(name);
        GitRemoteRegistry {
            index: GitIndex::new(source_id, config, repo_path, index_path),
            pkg_path,
            src_path,
        }
    }
}

impl<'cfg> RegistryData for GitRemoteRegistry<'cfg> {
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
        // remote git registries don't have configuration for remote APIs or anything
        // like that (similar to a local registry)
        Poll::Ready(Ok(None))
    }

    fn block_until_ready(&mut self) -> CargoResult<()> {
        self.index.block_until_ready(false)
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
        let dst = download::filename(pkg);

        // Note that the usage of `into_path_unlocked` here is because the local
        // crate files here never change in that we're not the one writing them,
        // so it's not our responsibility to synchronize access to them.
        let path = self.pkg_path.join(&dst).into_path_unlocked();
        let mut crate_file = paths::open(&path)?;

        // If we've already got an unpacked version of this crate, then skip the
        // checksum below as it is in theory already verified.
        if self.src_path.join(dst).into_path_unlocked().exists() {
            return Ok(MaybeLock::Ready(crate_file));
        }

        self.index.config.shell().status("Unpacking", pkg)?;

        // We don't actually need to download anything per-se, we just need to
        // verify the checksum matches the .crate file itself.
        let actual = Sha256::new().update_file(&crate_file)?.finish_hex();
        if actual != checksum {
            anyhow::bail!("failed to verify the checksum of `{}`", pkg)
        }

        crate_file.seek(SeekFrom::Start(0))?;

        Ok(MaybeLock::Ready(crate_file))
    }

    fn finish_download(
        &mut self,
        _pkg: PackageId,
        _checksum: &str,
        _data: &[u8],
    ) -> CargoResult<File> {
        panic!("this source doesn't download")
    }
}
