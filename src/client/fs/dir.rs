use std::{collections::VecDeque, sync::Arc};

use super::Metadata;
use crate::protocol::FileType;

/// Entries returned by the [`ReadDir`] iterator.
#[derive(Debug)]
pub struct DirEntry {
    parent: Arc<[u8]>,
    file: Vec<u8>,
    metadata: Metadata,
}

impl DirEntry {
    /// Returns the file name for the file that this entry points at.
    pub fn file_name(&self) -> String {
        String::from_utf8_lossy(&self.file).into_owned()
    }

    pub fn file_name_bytes(&self) -> &[u8] {
        &self.file
    }

    /// Returns the file type for the file that this entry points at.
    pub fn file_type(&self) -> FileType {
        self.metadata.file_type()
    }

    /// Returns the metadata for the file that this entry points at.
    pub fn metadata(&self) -> Metadata {
        self.metadata.to_owned()
    }

    /// Returns the full path of the file that this entry points at.
    ///
    /// The returned path is built by joining the path originally passed to
    /// [`SftpSession::read_dir`](crate::client::SftpSession::read_dir) with
    /// [`DirEntry::file_name`] using `/` as the separator (SFTP always uses
    /// POSIX-style paths on the wire). No canonicalization is performed, so a
    /// relative input yields a relative result — mirroring the behaviour of
    /// [`std::fs::DirEntry::path`].
    pub fn path(&self) -> String {
        String::from_utf8_lossy(&self.path_bytes()).into_owned()
    }

    pub fn path_bytes(&self) -> Vec<u8> {
        if self.parent.is_empty() {
            self.file.clone()
        } else if self.parent.ends_with(b"/") {
            let mut path = self.parent.to_vec();
            path.extend_from_slice(&self.file);
            path
        } else {
            let mut path = self.parent.to_vec();
            path.push(b'/');
            path.extend_from_slice(&self.file);
            path
        }
    }
}

/// Iterator over the entries in a remote directory.
pub struct ReadDir {
    pub(crate) parent: Arc<[u8]>,
    pub(crate) entries: VecDeque<(Vec<u8>, Metadata)>,
}

impl Iterator for ReadDir {
    type Item = DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match self.entries.pop_front() {
            None => None,
            Some(entry) if entry.0 == b"." || entry.0 == b".." => self.next(),
            Some(entry) => Some(DirEntry {
                parent: self.parent.clone(),
                file: entry.0,
                metadata: entry.1,
            }),
        }
    }
}
