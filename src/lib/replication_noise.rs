use std::io::{self, Read, Write};

use anyhow::{Context, Result, anyhow, bail};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use sha2::{Digest, Sha256};
use snow::{HandshakeState, TransportState, params::NoiseParams};

const NOISE_PROTOCOL_NAME: &str = "Noise_NNpsk0_25519_ChaChaPoly_SHA256";
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
const AEAD_TAG_LEN: usize = 16;
const HANDSHAKE_MESSAGE_MAX: usize = 1024;

fn derive_psk(token: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(token);
    let digest = hasher.finalize();
    let mut psk = [0u8; 32];
    psk.copy_from_slice(&digest);
    psk
}

fn noise_params() -> Result<NoiseParams> {
    NOISE_PROTOCOL_NAME
        .parse()
        .context("failed to parse Noise protocol definition")
}

fn build_initiator_state(token: &[u8]) -> Result<HandshakeState> {
    let params = noise_params()?;
    let psk = derive_psk(token);
    snow::Builder::new(params)
        .psk(0, &psk)
        .build_initiator()
        .context("failed to build Noise initiator")
}

fn build_responder_state(token: &[u8]) -> Result<HandshakeState> {
    let params = noise_params()?;
    let psk = derive_psk(token);
    snow::Builder::new(params)
        .psk(0, &psk)
        .build_responder()
        .context("failed to build Noise responder")
}

fn encrypt_payload(state: &mut TransportState, plaintext: &[u8]) -> Result<Vec<u8>> {
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
    buffer.truncate(len);
    Ok(buffer)
}

fn decrypt_payload(state: &mut TransportState, ciphertext: &[u8]) -> Result<Vec<u8>> {
    if ciphertext.len() > MAX_FRAME_LEN + AEAD_TAG_LEN {
        bail!(
            "encrypted Noise frame exceeds maximum length ({} bytes)",
            MAX_FRAME_LEN + AEAD_TAG_LEN
        );
    }
    let mut buffer = vec![0u8; ciphertext.len()];
    let len = state
        .read_message(ciphertext, &mut buffer)
        .context("failed to decrypt Noise frame")?;
    buffer.truncate(len);
    Ok(buffer)
}

fn ensure_frame_size(len: usize) -> Result<()> {
    if len > MAX_FRAME_LEN + AEAD_TAG_LEN {
        bail!(
            "frame length {} exceeds allowed maximum {}",
            len,
            MAX_FRAME_LEN + AEAD_TAG_LEN
        );
    }
    Ok(())
}

pub async fn perform_client_handshake<R, W>(
    reader: &mut R,
    writer: &mut W,
    token: &[u8],
) -> Result<TransportState>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut state = build_initiator_state(token)?;
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
    token: &[u8],
) -> Result<TransportState>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut state = build_responder_state(token)?;

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
    let buffer = encrypt_payload(state, plaintext)?;
    send_frame(writer, &buffer).await?;
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
    let plaintext = decrypt_payload(state, &frame)?;
    Ok(Some(plaintext))
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
    ensure_frame_size(len)?;
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

pub fn perform_client_handshake_blocking<S>(stream: &mut S, token: &[u8]) -> Result<TransportState>
where
    S: Read + Write,
{
    let mut state = build_initiator_state(token)?;
    let mut buffer = vec![0u8; HANDSHAKE_MESSAGE_MAX];
    let len = state
        .write_message(&[], &mut buffer)
        .context("failed to write Noise handshake message")?;
    send_frame_blocking(stream, &buffer[..len])?;
    stream.flush()?;

    let frame = read_frame_blocking(stream)?
        .ok_or_else(|| anyhow!("peer closed connection during Noise handshake"))?;
    state
        .read_message(&frame, &mut [])
        .context("failed to read Noise handshake response")?;

    state
        .into_transport_mode()
        .context("failed to construct Noise transport state")
}

pub fn write_encrypted_frame_blocking<W: Write>(
    writer: &mut W,
    state: &mut TransportState,
    plaintext: &[u8],
) -> Result<()> {
    let buffer = encrypt_payload(state, plaintext)?;
    send_frame_blocking(writer, &buffer)?;
    writer.flush()?;
    Ok(())
}

pub fn read_encrypted_frame_blocking<R: Read>(
    reader: &mut R,
    state: &mut TransportState,
) -> Result<Option<Vec<u8>>> {
    let frame = match read_frame_blocking(reader)? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    let plaintext = decrypt_payload(state, &frame)?;
    Ok(Some(plaintext))
}

fn send_frame_blocking<W: Write>(writer: &mut W, payload: &[u8]) -> Result<()> {
    let len = payload.len();
    if len > u32::MAX as usize {
        bail!("frame payload exceeds u32 length");
    }
    let mut header = [0u8; 4];
    header.copy_from_slice(&(len as u32).to_be_bytes());
    writer
        .write_all(&header)
        .context("failed to write Noise frame header")?;
    writer
        .write_all(payload)
        .context("failed to write Noise frame payload")?;
    Ok(())
}

fn read_frame_blocking<R: Read>(reader: &mut R) -> Result<Option<Vec<u8>>> {
    let mut header = [0u8; 4];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => {
            return Err(anyhow!("failed to read Noise frame header: {}", err));
        }
    }
    let len = u32::from_be_bytes(header) as usize;
    ensure_frame_size(len)?;
    let mut payload = vec![0u8; len];
    if len > 0 {
        if let Err(err) = reader.read_exact(&mut payload) {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(anyhow!("failed to read Noise frame payload: {}", err));
        }
    }
    Ok(Some(payload))
}
