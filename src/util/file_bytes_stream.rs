use futures_util::stream::Stream;
use http_range::HttpRange;
use hyper::body::{Body, Bytes};
use std::io::{Error as IoError, Write, SeekFrom};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

const BUF_SIZE: usize = 8 * 1024;

/// Wraps a `tokio::fs::File`, and implements a stream of `Bytes`s.
pub struct FileBytesStream {
    file: File,
    buf: Box<[MaybeUninit<u8>; BUF_SIZE]>,
    range_bytes: Option<RangeBytes>,
}

struct RangeBytes {
    seek_completed: bool,
    range_remaining: u64,
    file_length: u64,
    ranges: vec::IntoIter<HttpRange>,
    // Only used if there is more than one range
    boundary: String,
    // Only used if there is more than one range
    is_first_boundary: bool,
}

impl FileBytesStream {
    /// Create a new stream from the given file.
    pub fn new(file: File) -> FileBytesStream {
        let buf = Box::new([MaybeUninit::uninit(); BUF_SIZE]);
        FileBytesStream { file, buf, range_bytes: None }
    }

    /// Set ranges if the client requested it.  Boundary must be the empty string if there is only one
    /// range.  It is invalid to pass a list of zero ranges.
    pub fn set_ranges(&mut self, ranges: Vec<HttpRange>, boundary: String, file_length: u64) -> &mut Self {
        assert_eq!(ranges.len() > 1, boundary.len() > 0);
        let is_first_boundary = ranges.len() > 1;
        self.range_bytes = Some(RangeBytes {
            seek_completed: true,
            range_remaining: 0,
            file_length,
            ranges: ranges.into_iter(),
            boundary,
            is_first_boundary,
        });
        self
    }
}

impl RangeBytes {

    pub fn handle(&mut self, file: &mut File, buf: &mut [MaybeUninit<u8>], cx: &mut Context) -> Poll<Option<Result<Bytes, IoError>>> {
        loop {
            if !self.seek_completed {
                match Pin::new(&mut *file).poll_complete(cx) {
                    Poll::Ready(Ok(..)) => (),
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Pending => return Poll::Pending,
                }
            }

            // we've finished the previous range (or we're just starting), we need to seek
            // and repopulate the remaining field if we're not completely finished.
            if self.range_remaining == 0 {
                let range = match self.ranges.next() {
                    Some(r) => r,
                    None => return Poll::Ready(None),
                };
                if let Err(e) = Pin::new(&mut *file).start_seek(SeekFrom::Start(range.start)) {
                    return Poll::Ready(Some(Err(e)));
                }
                self.seek_completed = false;
                self.range_remaining = range.length;
                if self.boundary.len() > 0 {
                    let mut mp_header = Vec::new();
                    if self.is_first_boundary {
                        self.is_first_boundary = false;
                    } else {
                        write!(&mut mp_header, "\r\n").unwrap();
                    }
                    write!(&mut mp_header, "--{}\r\n", self.boundary).unwrap();
                    write!(
                        &mut mp_header,
                        "Content-Range: bytes {}-{}/{}\r\n",
                        range.start,
                        range.start + range.length - 1,
                        self.file_length,
                    )
                    .unwrap();
                    // write!(&mut mp_header, "Content-Type: {}\r\n", file_mime_type).unwrap();
                    write!(&mut mp_header, "\r\n").unwrap();

                    return Poll::Ready(Some(Ok(mp_header.into())));
                }

                // back to the top of the loop so we can poll on the seek's completion
            } else {
                let rr = std::cmp::min(self.range_remaining, usize::max_value() as u64) as usize;
                let max_read_length = std::cmp::min(rr, buf.len());

                let mut read_buf = ReadBuf::uninit(&mut buf[..max_read_length]);
                return match Pin::new(&mut *file).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let filled = read_buf.filled();
                        self.range_remaining -= filled.len() as u64;
                        if filled.is_empty() {
                            Poll::Ready(None)
                        } else {
                            Poll::Ready(Some(Ok(Bytes::copy_from_slice(filled))))
                        }
                    }
                    Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                    Poll::Pending => Poll::Pending,
                }
            }
        }

    }
}

impl Stream for FileBytesStream {
    type Item = Result<Bytes, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let Self {
            ref mut file,
            ref mut buf,
            ref mut range_bytes,
        } = *self;

        if let Some(rb) = range_bytes {
            rb.handle(file, &mut buf[..], cx)
        } else {
            let mut read_buf = ReadBuf::uninit(&mut buf[..]);
            match Pin::new(file).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) => {
                    let filled = read_buf.filled();
                    if filled.is_empty() {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(Ok(Bytes::copy_from_slice(filled))))
                    }
                }
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

impl FileBytesStream {
    /// Create a Hyper `Body` from this stream.
    pub fn into_body(self) -> Body {
        Body::wrap_stream(self)
    }
}
