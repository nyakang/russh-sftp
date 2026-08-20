use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{
    error::Error,
    fs::{File, Metadata, ReadDir},
    rawsession::{Limits, SftpResult},
    RawSftpSession,
};
use crate::{
    client::Config,
    extensions::{self, Statvfs},
    protocol::{FileAttributes, OpenFlags, StatusCode},
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Features {
    pub hardlink: bool,
    pub fsync: bool,
    pub statvfs: bool,
    pub expand_path: bool,
    pub limits: Option<Limits>,
    pub max_concurrent_writes: usize,
    pub max_packet_len: u32,
}

/// High-level SFTP implementation for easy interaction with a remote file system.
/// Contains most methods similar to the native [filesystem](std::fs)
pub struct SftpSession {
    session: Arc<RawSftpSession>,
    features: Features,
}

impl SftpSession {
    /// Creates a new session by initializing the protocol and extensions
    pub async fn new<S>(stream: S) -> SftpResult<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::new_with_config(stream, Config::default()).await
    }

    /// Creates a new session with custom configuration
    pub async fn new_with_config<S>(stream: S, cfg: Config) -> SftpResult<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let max_concurrent_writes = cfg.max_concurrent_writes;
        let max_packet_len = cfg.max_packet_len;
        let mut session = RawSftpSession::new_with_config(stream, cfg);

        let version = session.init().await?;
        let has_extension = |name, ver| version.extensions.get(name).is_some_and(|v| v == ver);

        let mut features = Features {
            hardlink: has_extension(extensions::HARDLINK, "1"),
            fsync: has_extension(extensions::FSYNC, "1"),
            statvfs: has_extension(extensions::STATVFS, "2"),
            expand_path: has_extension(extensions::EXPAND_PATH, "1"),
            limits: None,
            max_concurrent_writes,
            max_packet_len,
        };

        if has_extension(extensions::LIMITS, "1") {
            let limits = Limits::from(session.limits().await?);
            session.set_limits(limits);
            features.limits = Some(limits);
            if let Some(plen) = limits.packet_len {
                features.max_packet_len = (plen as u32).min(max_packet_len);
            }
        }

        Ok(Self {
            session: Arc::new(session),
            features,
        })
    }

    /// Set the maximum response time in seconds.
    /// Default: 10 seconds
    pub fn set_timeout(&self, secs: u64) {
        self.session.set_timeout(secs);
    }

    /// Returns limits advertised by the server via the `limits@openssh.com`
    /// extension, when available.
    pub fn limits(&self) -> Option<Limits> {
        self.features.limits
    }

    /// Returns the effective maximum packet length after applying server limits.
    pub fn effective_max_packet_len(&self) -> u32 {
        self.features.max_packet_len
    }

    /// Returns the maximum number of open handles advertised by the server.
    pub fn max_open_handles(&self) -> Option<u64> {
        self.features.limits.and_then(|limits| limits.open_handles)
    }

    /// Closes the inner channel stream.
    pub async fn close(&self) -> SftpResult<()> {
        self.session.close_session()
    }

    /// Attempts to open a file in read-only mode.
    pub async fn open<T: Into<String>>(&self, filename: T) -> SftpResult<File> {
        self.open_bytes(filename.into().into_bytes()).await
    }

    /// Attempts to open a file in read-only mode using raw SFTP path bytes.
    pub async fn open_bytes(&self, filename: Vec<u8>) -> SftpResult<File> {
        self.open_with_flags_bytes(filename, OpenFlags::READ).await
    }

    /// Opens a file in write-only mode.
    ///
    /// This function will create a file if it does not exist, and will truncate it if it does.
    pub async fn create<T: Into<String>>(&self, filename: T) -> SftpResult<File> {
        self.create_bytes(filename.into().into_bytes()).await
    }

    /// Opens a file in write-only mode using raw SFTP path bytes.
    ///
    /// This function will create a file if it does not exist, and will truncate it if it does.
    pub async fn create_bytes(&self, filename: Vec<u8>) -> SftpResult<File> {
        self.open_with_flags_bytes(
            filename,
            OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
        )
        .await
    }

    /// Attempts to open or create the file in the specified mode
    pub async fn open_with_flags<T: Into<String>>(
        &self,
        filename: T,
        flags: OpenFlags,
    ) -> SftpResult<File> {
        self.open_with_flags_bytes(filename.into().into_bytes(), flags)
            .await
    }

    /// Attempts to open or create the file in the specified mode using raw SFTP path bytes.
    pub async fn open_with_flags_bytes(
        &self,
        filename: Vec<u8>,
        flags: OpenFlags,
    ) -> SftpResult<File> {
        self.open_with_flags_and_attributes_bytes(filename, flags, FileAttributes::empty())
            .await
    }

    /// Attempts to open or create the file in the specified mode and with specified file attributes
    pub async fn open_with_flags_and_attributes<T: Into<String>>(
        &self,
        filename: T,
        flags: OpenFlags,
        attributes: FileAttributes,
    ) -> SftpResult<File> {
        self.open_with_flags_and_attributes_bytes(filename.into().into_bytes(), flags, attributes)
            .await
    }

    /// Attempts to open or create the file in the specified mode and with specified file
    /// attributes using raw SFTP path bytes.
    pub async fn open_with_flags_and_attributes_bytes(
        &self,
        filename: Vec<u8>,
        flags: OpenFlags,
        attributes: FileAttributes,
    ) -> SftpResult<File> {
        let handle = self
            .session
            .open_bytes(filename, flags, attributes)
            .await?
            .handle;
        Ok(File::new(self.session.clone(), handle, self.features))
    }

    /// Requests the remote party for the absolute from the relative path.
    pub async fn canonicalize<T: Into<String>>(&self, path: T) -> SftpResult<String> {
        let path = self.canonicalize_bytes(path.into().into_bytes()).await?;
        Ok(String::from_utf8_lossy(&path).into_owned())
    }

    /// Requests the remote party for the absolute from the relative path using raw SFTP path
    /// bytes, returning raw bytes from the server.
    pub async fn canonicalize_bytes<T: Into<Vec<u8>>>(&self, path: T) -> SftpResult<Vec<u8>> {
        let name = self.session.realpath_bytes(path.into()).await?;
        match name.files.first() {
            Some(file) => Ok(file.filename.to_owned()),
            None => Err(Error::UnexpectedBehavior("no file".to_owned())),
        }
    }

    /// Creates a new empty directory.
    pub async fn create_dir<T: Into<String>>(&self, path: T) -> SftpResult<()> {
        self.create_dir_bytes(path.into().into_bytes()).await
    }

    /// Creates a new empty directory using raw SFTP path bytes.
    pub async fn create_dir_bytes<T: Into<Vec<u8>>>(&self, path: T) -> SftpResult<()> {
        self.session
            .mkdir_bytes(path.into(), FileAttributes::empty())
            .await
            .map(|_| ())
    }

    /// Reads the contents of a file located at the specified path to the end.
    pub async fn read<P: Into<String>>(&self, path: P) -> SftpResult<Vec<u8>> {
        self.read_bytes(path.into().into_bytes()).await
    }

    /// Reads the contents of a file located at the specified raw SFTP path bytes to the end.
    pub async fn read_bytes<P: Into<Vec<u8>>>(&self, path: P) -> SftpResult<Vec<u8>> {
        let mut file = self.open_bytes(path.into()).await?;
        let mut buffer = Vec::new();

        file.read_to_end(&mut buffer).await?;
        file.shutdown().await?;

        Ok(buffer)
    }

    /// Writes the contents to a file whose path is specified.
    pub async fn write<P: Into<String>>(&self, path: P, data: &[u8]) -> SftpResult<()> {
        self.write_bytes(path.into().into_bytes(), data).await
    }

    /// Writes the contents to a file whose raw SFTP path bytes are specified.
    pub async fn write_bytes<P: Into<Vec<u8>>>(&self, path: P, data: &[u8]) -> SftpResult<()> {
        let mut file = self
            .open_with_flags_bytes(path.into(), OpenFlags::WRITE)
            .await?;
        file.write_all(data).await?;
        file.flush().await?;
        file.shutdown().await?;
        Ok(())
    }

    /// Checks a file or folder exists at the specified path
    pub async fn try_exists<P: Into<String>>(&self, path: P) -> SftpResult<bool> {
        self.try_exists_bytes(path.into().into_bytes()).await
    }

    /// Checks a file or folder exists at the specified raw SFTP path bytes.
    pub async fn try_exists_bytes<P: Into<Vec<u8>>>(&self, path: P) -> SftpResult<bool> {
        match self.metadata_bytes(path).await {
            Ok(_) => Ok(true),
            Err(Error::Status(status)) if status.status_code == StatusCode::NoSuchFile => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Returns an iterator over the entries within a directory.
    pub async fn read_dir<P: Into<String>>(&self, path: P) -> SftpResult<ReadDir> {
        self.read_dir_bytes(path.into().into_bytes()).await
    }

    /// Returns an iterator over the entries within a directory using raw SFTP path bytes.
    pub async fn read_dir_bytes<P: Into<Vec<u8>>>(&self, path: P) -> SftpResult<ReadDir> {
        let path: Vec<u8> = path.into();
        let parent = Arc::from(path.as_slice());

        let handle = self.session.opendir_bytes(path).await?.handle;
        let mut files = vec![];

        loop {
            match self.session.readdir(handle.as_str()).await {
                Ok(name) => {
                    files = name
                        .files
                        .into_iter()
                        .map(|f| (f.filename, f.attrs))
                        .chain(files)
                        .collect();
                }
                Err(Error::Status(status)) if status.status_code == StatusCode::Eof => break,
                Err(err) => return Err(err),
            }
        }

        self.session.close(handle).await?;

        Ok(ReadDir {
            parent,
            entries: files.into(),
        })
    }

    /// Reads a symbolic link, returning the file that the link points to.
    pub async fn read_link<P: Into<String>>(&self, path: P) -> SftpResult<String> {
        let path = self.read_link_bytes(path.into().into_bytes()).await?;
        Ok(String::from_utf8_lossy(&path).into_owned())
    }

    /// Reads a symbolic link using raw SFTP path bytes, returning raw bytes from the server.
    pub async fn read_link_bytes<P: Into<Vec<u8>>>(&self, path: P) -> SftpResult<Vec<u8>> {
        let name = self.session.readlink_bytes(path.into()).await?;
        match name.files.first() {
            Some(file) => Ok(file.filename.to_owned()),
            None => Err(Error::UnexpectedBehavior("no file".to_owned())),
        }
    }

    /// Removes the specified folder.
    pub async fn remove_dir<P: Into<String>>(&self, path: P) -> SftpResult<()> {
        self.remove_dir_bytes(path.into().into_bytes()).await
    }

    /// Removes the specified folder using raw SFTP path bytes.
    pub async fn remove_dir_bytes<P: Into<Vec<u8>>>(&self, path: P) -> SftpResult<()> {
        self.session.rmdir_bytes(path.into()).await.map(|_| ())
    }

    /// Removes the specified file.
    pub async fn remove_file<T: Into<String>>(&self, filename: T) -> SftpResult<()> {
        self.remove_file_bytes(filename.into().into_bytes()).await
    }

    /// Removes the specified file using raw SFTP path bytes.
    pub async fn remove_file_bytes<T: Into<Vec<u8>>>(&self, filename: T) -> SftpResult<()> {
        self.session.remove_bytes(filename.into()).await.map(|_| ())
    }

    /// Rename a file or directory to a new name.
    pub async fn rename<O, N>(&self, oldpath: O, newpath: N) -> SftpResult<()>
    where
        O: Into<String>,
        N: Into<String>,
    {
        self.rename_bytes(oldpath.into().into_bytes(), newpath.into().into_bytes())
            .await
    }

    /// Rename a file or directory to a new name using raw SFTP path bytes.
    pub async fn rename_bytes<O, N>(&self, oldpath: O, newpath: N) -> SftpResult<()>
    where
        O: Into<Vec<u8>>,
        N: Into<Vec<u8>>,
    {
        self.session
            .rename_bytes(oldpath.into(), newpath.into())
            .await
            .map(|_| ())
    }

    /// Creates a symlink of the specified target.
    pub async fn symlink<P, T>(&self, path: P, target: T) -> SftpResult<()>
    where
        P: Into<String>,
        T: Into<String>,
    {
        self.symlink_bytes(path.into().into_bytes(), target.into().into_bytes())
            .await
    }

    /// Creates a symlink of the specified target using raw SFTP path bytes.
    pub async fn symlink_bytes<P, T>(&self, path: P, target: T) -> SftpResult<()>
    where
        P: Into<Vec<u8>>,
        T: Into<Vec<u8>>,
    {
        self.session
            .symlink_bytes(path.into(), target.into())
            .await
            .map(|_| ())
    }

    /// Creates a symlink using OpenSSH SFTP argument ordering.
    ///
    /// OpenSSH's `SSH_FXP_SYMLINK` implementation expects `(target, link)`,
    /// which is the reverse of the order documented by the old draft protocol.
    pub async fn symlink_openssh<T, L>(&self, target: T, link: L) -> SftpResult<()>
    where
        T: Into<String>,
        L: Into<String>,
    {
        self.symlink_openssh_bytes(target.into().into_bytes(), link.into().into_bytes())
            .await
    }

    /// Creates an OpenSSH-ordered symlink using raw SFTP path bytes.
    pub async fn symlink_openssh_bytes<T, L>(&self, target: T, link: L) -> SftpResult<()>
    where
        T: Into<Vec<u8>>,
        L: Into<Vec<u8>>,
    {
        self.session
            .symlink_openssh_bytes(target.into(), link.into())
            .await
            .map(|_| ())
    }

    /// Queries metadata about the remote file.
    pub async fn metadata<P: Into<String>>(&self, path: P) -> SftpResult<Metadata> {
        self.metadata_bytes(path.into().into_bytes()).await
    }

    /// Queries metadata about the remote file using raw SFTP path bytes.
    pub async fn metadata_bytes<P: Into<Vec<u8>>>(&self, path: P) -> SftpResult<Metadata> {
        Ok(self.session.stat_bytes(path.into()).await?.attrs)
    }

    /// Sets metadata for a remote file.
    pub async fn set_metadata<P: Into<String>>(
        &self,
        path: P,
        metadata: Metadata,
    ) -> Result<(), Error> {
        self.set_metadata_bytes(path.into().into_bytes(), metadata)
            .await
    }

    /// Sets metadata for a remote file using raw SFTP path bytes.
    pub async fn set_metadata_bytes<P: Into<Vec<u8>>>(
        &self,
        path: P,
        metadata: Metadata,
    ) -> Result<(), Error> {
        self.session
            .setstat_bytes(path.into(), metadata)
            .await
            .map(|_| ())
    }

    pub async fn symlink_metadata<P: Into<String>>(&self, path: P) -> SftpResult<Metadata> {
        self.symlink_metadata_bytes(path.into().into_bytes()).await
    }

    pub async fn symlink_metadata_bytes<P: Into<Vec<u8>>>(&self, path: P) -> SftpResult<Metadata> {
        Ok(self.session.lstat_bytes(path.into()).await?.attrs)
    }

    pub async fn hardlink<O, N>(&self, oldpath: O, newpath: N) -> SftpResult<bool>
    where
        O: Into<String>,
        N: Into<String>,
    {
        if !self.features.hardlink {
            return Ok(false);
        }

        self.session.hardlink(oldpath, newpath).await.map(|_| true)
    }

    /// Performs a statvfs on the remote file system path.
    /// Returns `Ok(None)` if the remote SFTP server does not support `statvfs@openssh.com` extension v2.
    pub async fn fs_info<P: Into<String>>(&self, path: P) -> SftpResult<Option<Statvfs>> {
        if !self.features.statvfs {
            return Ok(None);
        }

        self.session.statvfs(path).await.map(Some)
    }

    /// Expands a `~`/`~user`-prefixed or relative path and returns its canonicalized absolute form.
    /// Returns `Ok(None)` if the remote SFTP server does not support `expand-path@openssh.com` extension v1.
    pub async fn expand_path<P: Into<String>>(&self, path: P) -> SftpResult<Option<String>> {
        let expanded = self.expand_path_bytes(path.into().into_bytes()).await?;
        Ok(expanded.map(|path| String::from_utf8_lossy(&path).into_owned()))
    }

    /// Expands a `~`/`~user`-prefixed or relative path using raw SFTP path bytes and returns raw
    /// bytes from the server.
    pub async fn expand_path_bytes<P: Into<Vec<u8>>>(
        &self,
        path: P,
    ) -> SftpResult<Option<Vec<u8>>> {
        if !self.features.expand_path {
            return Ok(None);
        }

        let name = self
            .session
            .expand_path(String::from_utf8_lossy(&path.into()).into_owned())
            .await?;
        match name.files.first() {
            Some(file) => Ok(Some(file.filename.to_owned())),
            None => Err(Error::UnexpectedBehavior("no file".to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl SftpSession {
        fn for_test_with_limits(limits: Option<Limits>, max_packet_len: u32) -> Self {
            let stream = tokio::io::duplex(64).0;
            Self {
                session: Arc::new(RawSftpSession::new(stream)),
                features: Features {
                    hardlink: false,
                    fsync: false,
                    statvfs: false,
                    expand_path: false,
                    limits,
                    max_concurrent_writes: 8,
                    max_packet_len,
                },
            }
        }
    }

    #[tokio::test]
    async fn exposes_server_limits_and_effective_packet_len() {
        let limits = Limits {
            packet_len: Some(65_536),
            read_len: Some(32_768),
            write_len: Some(32_768),
            open_handles: Some(128),
        };
        let session = SftpSession::for_test_with_limits(Some(limits), 65_536);

        assert_eq!(session.limits(), Some(limits));
        assert_eq!(session.effective_max_packet_len(), 65_536);
        assert_eq!(session.max_open_handles(), Some(128));
    }

    #[tokio::test]
    async fn max_open_handles_is_none_without_limits_extension() {
        let session = SftpSession::for_test_with_limits(None, 262_144);

        assert_eq!(session.limits(), None);
        assert_eq!(session.effective_max_packet_len(), 262_144);
        assert_eq!(session.max_open_handles(), None);
    }
}
