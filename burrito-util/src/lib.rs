//! Read from and write to a Reader/Writer, given a serialized slice.
//!
//! Will wrap the buffer with its length.

use byteorder::{ByteOrder, LittleEndian};

pub async fn write_msg(
    mut cl: impl tokio::io::AsyncWrite + Unpin,
    msg_type: Option<u32>,
    msg: &[u8],
) -> Result<(), failure::Error> {
    use tokio::io::AsyncWriteExt;
    cl.write_all(&(msg.len() as u32).to_le_bytes()).await?;
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
    Ok(LittleEndian::read_u32(buf))
}

async fn read_len(
    mut cl: impl tokio::io::AsyncRead + Unpin,
    buf: &mut [u8],
) -> Result<usize, failure::Error> {
    Ok(loop {
        let len_to_read = read_u32(&mut cl, buf).await? as usize;
        if len_to_read == 0 {
            tokio::task::yield_now().await;
            continue;
        } else if len_to_read > buf.len() {
            failure::bail!(
                "message size too large: {:?} > {:?}",
                len_to_read,
                buf.len()
            );
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

    let len_to_read = read_len(&mut cl, buf).await?;
    cl.read_exact(&mut buf[0..len_to_read]).await?;
    Ok(len_to_read)
}

pub async fn read_msg_with_type(
    mut cl: impl tokio::io::AsyncRead + Unpin,
    buf: &mut [u8],
) -> Result<(usize, u32), failure::Error> {
    use tokio::io::AsyncReadExt;

    let len_to_read = read_len(&mut cl, buf).await?;
    let msg_type = read_u32(&mut cl, buf).await?;
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

    #[test]
    fn read_write() -> Result<(), Box<dyn std::error::Error>> {
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_io()
            .build()?;

        let r: Result<(), Box<dyn std::error::Error>> = rt.block_on(async move {
            let mut buf = vec![];
            super::write_msg(&mut buf, Some(1), b"foo").await?;

            println!("wrote: {:?}", buf);

            let mut read_buf = [0u8; 16];
            let cur = std::io::Cursor::new(buf);
            let (len, typ) = super::read_msg_with_type(cur, &mut read_buf).await?;

            assert_eq!(typ, 1);
            assert_eq!(&read_buf[0..len as usize], b"foo");
            Ok(())
        });
        r?;

        Ok(())
    }

    #[test]
    fn read_write_notype() -> Result<(), Box<dyn std::error::Error>> {
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_io()
            .build()?;

        let r: Result<(), Box<dyn std::error::Error>> = rt.block_on(async move {
            let mut buf = vec![];
            super::write_msg(&mut buf, None, b"foo").await?;

            println!("wrote: {:?}", buf);

            let mut read_buf = [0u8; 16];
            let cur = std::io::Cursor::new(buf);
            let len = super::read_msg(cur, &mut read_buf).await?;

            assert_eq!(&read_buf[0..len as usize], b"foo");
            Ok(())
        });
        r?;

        Ok(())
    }
}
