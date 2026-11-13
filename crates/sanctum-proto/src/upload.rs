use std::io;

use sanctum_core::UPLOAD_FRAME_BYTES;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};

use crate::v1::{UploadTextEnd, UploadTextFrame, UploadTextHeader, upload_text_frame::Frame};

/// Sends a framed upload with bounded memory and an explicit end after EOF.
/// On read failure the sender is dropped before a complete upload can be accepted.
pub async fn send_upload(
    sender: mpsc::Sender<UploadTextFrame>,
    header: UploadTextHeader,
    mut source: impl AsyncRead + Unpin,
) -> io::Result<()> {
    let send = |frame| async {
        sender
            .send(UploadTextFrame { frame: Some(frame) })
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "server closed the upload stream")
            })
    };
    send(Frame::Header(header)).await?;
    let mut buffer = vec![0; UPLOAD_FRAME_BYTES];
    loop {
        let read = source.read(&mut buffer).await?;
        if read == 0 {
            return send(Frame::End(UploadTextEnd {})).await;
        }
        send(Frame::Data(buffer[..read].to_vec())).await?;
    }
}
