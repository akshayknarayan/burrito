//! Read from and write to a Reader/Writer, given a serialized slice.
//!
//! Will wrap the buffer with its length.

pub async fn write_msg(
    mut cl: impl tokio::io::AsyncWrite + Unpin,
    msg_type: Option<u32>,
    msg: &[u8],
) -> Result<(), failure::Error> {
    use tokio::io::AsyncWriteExt;
    cl.write_all(&msg.len().to_le_bytes()).await?;
    if let Some(t) = msg_type {
        cl.write_all(&t.to_le_bytes()).await?;
    }

    cl.write_all(msg).await?;
    Ok(())
}

async fn read_u32(
    mut cl: impl tokio::io::AsyncRead + Unpin,
    buf: &mut [u8],
) -> Result<u32, failure::Error> {
    use tokio::io::AsyncReadExt;
    cl.read_exact(&mut buf[0..4]).await?;

    // unsafe transmute ok because endianness is guaranteed to be the
    // same on either side of a UDS
    // will read only the first sizeof(u32) = 4 bytes
    Ok(unsafe { std::mem::transmute_copy(&buf) })
}

async fn read_len(
    mut cl: impl tokio::io::AsyncRead + Unpin,
    buf: &mut [u8],
) -> Result<usize, failure::Error> {
    Ok(loop {
        let len_to_read = read_u32(&mut cl, &mut *buf).await? as usize;
        if len_to_read == 0 {
            tokio::task::yield_now().await;
            continue;
        } else if len_to_read > buf.len() {
            failure::bail!("message size too large: {:?} > 128", len_to_read);
        } else {
            break len_to_read;
        }
    })
}

pub async fn read_msg(
    mut cl: impl tokio::io::AsyncRead + Unpin,
    buf: &mut [u8],
) -> Result<usize, failure::Error> {
    use tokio::io::AsyncReadExt;

    let len_to_read = read_len(&mut cl, &mut *buf).await?;
    cl.read_exact(&mut buf[0..len_to_read]).await?;
    Ok(len_to_read)
}

pub async fn read_msg_with_type(
    mut cl: impl tokio::io::AsyncRead + Unpin,
    buf: &mut [u8],
) -> Result<(usize, u32), failure::Error> {
    use tokio::io::AsyncReadExt;

    let len_to_read = read_len(&mut cl, &mut *buf).await?;
    let msg_type = read_u32(&mut cl, &mut *buf).await?;
    cl.read_exact(&mut buf[0..len_to_read as usize]).await?;
    Ok((len_to_read as usize, msg_type))
}

#[macro_export]
macro_rules! assign_const_vals {
    ($($tags:ident),+) => (
        $crate::assign_const_vals!(0; $($tags),+);
    );
    ($x: expr; $tag:ident) => (
        pub const $tag : usize = $x;
    );
    ($x: expr; $tag:ident, $($tags:ident),+) => (
        pub const $tag : usize = $x;
        $crate::assign_const_vals!($x + 1; $($tags),+);
    );
}

#[cfg(test)]
mod test {
    assign_const_vals!(FOO, BAR, BAZ, QUX);

    #[test]
    fn consts() {
        assert_eq!(FOO, 0);
        assert_eq!(BAR, 1);
        assert_eq!(BAZ, 2);
        assert_eq!(QUX, 3);
    }
}
