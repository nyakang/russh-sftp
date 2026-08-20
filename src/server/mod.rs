mod handler;
mod reply;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub use self::handler::Handler;
pub use self::reply::StatusReply;

use crate::{
    error::Error,
    protocol::{Packet, Status, StatusCode},
    utils::read_packet,
};

macro_rules! into_wrap {
    ($id:expr, $handler:expr, $var:ident; $($arg:ident),*) => {
        match $handler.$var($($var.$arg),*).await {
            Err(err) => {
                let StatusReply { status_code, error_message, language_tag } = err.into();
                Packet::Status(Status {
                    id: $id,
                    status_code,
                    error_message: error_message.unwrap_or_else(|| status_code.to_string()),
                    language_tag: language_tag.unwrap_or_else(|| "en-US".to_string()),
                })
            },
            Ok(packet) => packet.into(),
        }
    };
}

macro_rules! into_wrap_path {
    ($id:expr, $handler:expr, $method:ident; $($arg:expr),*) => {
        match $handler.$method($($arg),*).await {
            Err(err) => {
                let StatusReply { status_code, error_message, language_tag } = err.into();
                Packet::Status(Status {
                    id: $id,
                    status_code,
                    error_message: error_message.unwrap_or_else(|| status_code.to_string()),
                    language_tag: language_tag.unwrap_or_else(|| "en-US".to_string()),
                })
            },
            Ok(packet) => packet.into(),
        }
    };
}

fn path_string(path: Vec<u8>) -> String {
    String::from_utf8_lossy(&path).into_owned()
}

/// Configuration for the SFTP server.
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum allowed size of SFTP packets sent by clients. Default: 256 KiB.
    pub max_client_packet_len: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_client_packet_len: 262144,
        }
    }
}

async fn process_request<H>(packet: Packet, handler: &mut H) -> Packet
where
    H: Handler + Send,
{
    let id = packet.get_request_id();

    match packet {
        Packet::Init(init) => into_wrap!(id, handler, init; version, extensions),
        Packet::Open(open) => into_wrap_path!(
            id,
            handler,
            open;
            open.id,
            path_string(open.filename),
            open.pflags,
            open.attrs
        ),
        Packet::Close(close) => into_wrap!(id, handler, close; id, handle),
        Packet::Read(read) => into_wrap!(id, handler, read; id, handle, offset, len),
        Packet::Write(write) => into_wrap!(id, handler, write; id, handle, offset, data),
        Packet::Lstat(lstat) => {
            into_wrap_path!(id, handler, lstat; lstat.id, path_string(lstat.path))
        }
        Packet::Fstat(fstat) => into_wrap!(id, handler, fstat; id, handle),
        Packet::SetStat(setstat) => into_wrap_path!(
            id,
            handler,
            setstat;
            setstat.id,
            path_string(setstat.path),
            setstat.attrs
        ),
        Packet::FSetStat(fsetstat) => into_wrap!(id, handler, fsetstat; id, handle, attrs),
        Packet::OpenDir(opendir) => {
            into_wrap_path!(id, handler, opendir; opendir.id, path_string(opendir.path))
        }
        Packet::ReadDir(readdir) => into_wrap!(id, handler, readdir; id, handle),
        Packet::Remove(remove) => {
            into_wrap_path!(id, handler, remove; remove.id, path_string(remove.filename))
        }
        Packet::MkDir(mkdir) => into_wrap_path!(
            id,
            handler,
            mkdir;
            mkdir.id,
            path_string(mkdir.path),
            mkdir.attrs
        ),
        Packet::RmDir(rmdir) => {
            into_wrap_path!(id, handler, rmdir; rmdir.id, path_string(rmdir.path))
        }
        Packet::RealPath(realpath) => {
            into_wrap_path!(id, handler, realpath; realpath.id, path_string(realpath.path))
        }
        Packet::Stat(stat) => {
            into_wrap_path!(id, handler, stat; stat.id, path_string(stat.path))
        }
        Packet::Rename(rename) => into_wrap_path!(
            id,
            handler,
            rename;
            rename.id,
            path_string(rename.oldpath),
            path_string(rename.newpath)
        ),
        Packet::ReadLink(readlink) => {
            into_wrap_path!(id, handler, readlink; readlink.id, path_string(readlink.path))
        }
        Packet::Symlink(symlink) => into_wrap_path!(
            id,
            handler,
            symlink;
            symlink.id,
            path_string(symlink.linkpath),
            path_string(symlink.targetpath)
        ),
        Packet::Extended(extended) => into_wrap!(id, handler, extended; id, request, data),
        _ => Packet::error(0, StatusCode::BadMessage),
    }
}

async fn process_handler<H, S>(stream: &mut S, handler: &mut H, cfg: &Config) -> Result<(), Error>
where
    H: Handler + Send,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut bytes = read_packet(stream, cfg.max_client_packet_len).await?;

    let response = match Packet::try_from(&mut bytes) {
        Ok(request) => process_request(request, handler).await,
        Err(_) => Packet::error(0, StatusCode::BadMessage),
    };

    let packet = Bytes::try_from(response)?;
    stream.write_all(&packet).await?;
    stream.flush().await?;

    Ok(())
}

/// Run processing stream as SFTP
pub async fn run<S, H>(stream: S, handler: H)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Handler + Send + 'static,
{
    run_with_config(stream, handler, Config::default()).await
}

/// Run processing stream as SFTP with custom configuration
pub async fn run_with_config<S, H>(mut stream: S, mut handler: H, cfg: Config)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Handler + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            match process_handler(&mut stream, &mut handler, &cfg).await {
                Err(Error::UnexpectedEof) => break,
                Err(err) => warn!("{}", err),
                Ok(_) => (),
            }
        }

        debug!("sftp stream ended");
    });
}
