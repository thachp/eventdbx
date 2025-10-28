use std::io;

use anyhow::{Context, Result, anyhow, bail};
use curve25519_dalek::{edwards::CompressedEdwardsY, montgomery::MontgomeryPoint};
use ed25519_dalek::SigningKey;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use snow::{TransportState, params::NoiseParams};

const NOISE_PROTOCOL_NAME: &str = "Noise_NK_25519_ChaChaPoly_SHA256";
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
const AEAD_TAG_LEN: usize = 16;
const HANDSHAKE_MESSAGE_MAX: usize = 1024;

pub fn ed25519_public_to_x25519(public_key: &[u8]) -> Result<[u8; 32]> {
    if public_key.len() != 32 {
        bail!("ed25519 public key must be 32 bytes");
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(public_key);
    let compressed = CompressedEdwardsY(bytes);
    let point = compressed
        .decompress()
        .ok_or_else(|| anyhow!("failed to decompress ed25519 public key"))?;
    let montgomery: MontgomeryPoint = point.to_montgomery();
    Ok(montgomery.to_bytes())
}

pub fn ed25519_secret_to_x25519(secret_key: &[u8]) -> Result<[u8; 32]> {
    if secret_key.len() != 32 {
        bail!("ed25519 secret key must be 32 bytes");
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(secret_key);
    let signing = SigningKey::from_bytes(&seed);
    let mut scalar_bytes = signing.to_scalar_bytes();
    // Clamp per RFC 7748.
    scalar_bytes[0] &= 248;
    scalar_bytes[31] &= 127;
    scalar_bytes[31] |= 64;
    Ok(scalar_bytes)
}

pub async fn perform_client_handshake<R, W>(
    reader: &mut R,
    writer: &mut W,
    remote_ed25519: &[u8],
) -> Result<TransportState>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let remote_static = ed25519_public_to_x25519(remote_ed25519)?;
    let params: NoiseParams = NOISE_PROTOCOL_NAME
        .parse()
        .context("failed to parse Noise protocol definition")?;
    let builder = snow::Builder::new(params).remote_public_key(&remote_static);
    let mut state = builder
        .build_initiator()
        .context("failed to build Noise initiator")?;
    let mut buffer = vec![0u8; HANDSHAKE_MESSAGE_MAX];
    let len = state
        .write_message(&[], &mut buffer)
        .context("failed to write Noise handshake message")?;
    send_frame(writer, &buffer[..len]).await?;
    writer
        .flush()
        .await
        .context("failed to flush Noise handshake message")?;

    let message = read_frame(reader).await?;
    let frame = message.ok_or_else(|| anyhow!("peer closed connection during Noise handshake"))?;
    state
        .read_message(&frame, &mut [])
        .context("failed to read Noise handshake response")?;

    state
        .into_transport_mode()
        .context("failed to construct Noise transport state")
}

pub async fn perform_server_handshake<R, W>(
    reader: &mut R,
    writer: &mut W,
    local_ed25519_secret: &[u8],
) -> Result<TransportState>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let local_static = ed25519_secret_to_x25519(local_ed25519_secret)?;
    let params: NoiseParams = NOISE_PROTOCOL_NAME
        .parse()
        .context("failed to parse Noise protocol definition")?;
    let builder = snow::Builder::new(params).local_private_key(&local_static);
    let mut state = builder
        .build_responder()
        .context("failed to build Noise responder")?;

    let message = read_frame(reader).await?;
    let frame = message.ok_or_else(|| anyhow!("peer closed connection during Noise handshake"))?;
    state
        .read_message(&frame, &mut [])
        .context("failed to process Noise handshake message")?;

    let mut buffer = vec![0u8; HANDSHAKE_MESSAGE_MAX];
    let len = state
        .write_message(&[], &mut buffer)
        .context("failed to write Noise handshake response")?;
    send_frame(writer, &buffer[..len]).await?;
    writer
        .flush()
        .await
        .context("failed to flush Noise handshake response")?;

    state
        .into_transport_mode()
        .context("failed to construct Noise transport state")
}

pub async fn write_encrypted_frame<W>(
    writer: &mut W,
    state: &mut TransportState,
    plaintext: &[u8],
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if plaintext.len() > MAX_FRAME_LEN {
        bail!(
            "plaintext message exceeds maximum Noise frame length ({} bytes)",
            MAX_FRAME_LEN
        );
    }
    let mut buffer = vec![0u8; plaintext.len() + AEAD_TAG_LEN];
    let len = state
        .write_message(plaintext, &mut buffer)
        .context("failed to encrypt Noise frame")?;
    send_frame(writer, &buffer[..len]).await?;
    writer
        .flush()
        .await
        .context("failed to flush encrypted Noise frame")?;
    Ok(())
}

pub async fn read_encrypted_frame<R>(
    reader: &mut R,
    state: &mut TransportState,
) -> Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let frame = match read_frame(reader).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    if frame.len() > MAX_FRAME_LEN + AEAD_TAG_LEN {
        bail!(
            "encrypted Noise frame exceeds maximum length ({} bytes)",
            MAX_FRAME_LEN + AEAD_TAG_LEN
        );
    }
    let mut buffer = vec![0u8; frame.len()];
    let len = state
        .read_message(&frame, &mut buffer)
        .context("failed to decrypt Noise frame")?;
    buffer.truncate(len);
    Ok(Some(buffer))
}

async fn send_frame<W>(writer: &mut W, payload: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len = payload.len();
    if len > u32::MAX as usize {
        bail!("frame payload exceeds u32 length");
    }
    let mut header = [0u8; 4];
    header.copy_from_slice(&(len as u32).to_be_bytes());
    writer
        .write_all(&header)
        .await
        .context("failed to write Noise frame header")?;
    writer
        .write_all(payload)
        .await
        .context("failed to write Noise frame payload")?;
    Ok(())
}

async fn read_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 4];
    match reader.read_exact(&mut header).await {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => {
            return Err(anyhow!("failed to read Noise frame header: {}", err));
        }
    }
    let len = u32::from_be_bytes(header) as usize;
    if len > MAX_FRAME_LEN + AEAD_TAG_LEN {
        bail!(
            "frame length {} exceeds allowed maximum {}",
            len,
            MAX_FRAME_LEN + AEAD_TAG_LEN
        );
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        if let Err(err) = reader.read_exact(&mut payload).await {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(anyhow!("failed to read Noise frame payload: {}", err));
        }
    }
    Ok(Some(payload))
}
