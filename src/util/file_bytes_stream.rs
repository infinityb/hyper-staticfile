use futures_util::stream::Stream;
use http_range::HttpRange;
use hyper::body::{Body, Bytes};
use std::io::{Cursor, Error as IoError, SeekFrom, Write};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec;
use std::cmp::min;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

const BUF_SIZE: usize = 8 * 1024;


/// Wraps a `tokio::fs::File`, and implements a stream of `Bytes`s.
pub struct FileBytesStream {
    file: File,
    buf: Box<[MaybeUninit<u8>; BUF_SIZE]>,
    range_remaining: u64,
}


impl FileBytesStream {
    /// Create a new stream from the given file.
    pub fn new(file: File) -> FileBytesStream {
        let buf = Box::new([MaybeUninit::uninit(); BUF_SIZE]);
        FileBytesStream { file, buf, range_remaining: u64::MAX }
    }

    fn new_with_limit(file: File, range_remaining: u64) -> FileBytesStream {
        let buf = Box::new([MaybeUninit::uninit(); BUF_SIZE]);
        FileBytesStream { file, buf, range_remaining }
    }
}

impl Stream for FileBytesStream {
    type Item = Result<Bytes, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let Self {
            ref mut file,
            ref mut buf,
            range_remaining,
        } = *self;

        let max_read_length = min(range_remaining, buf.len() as u64) as usize;
        let mut read_buf = ReadBuf::uninit(&mut buf[..max_read_length]);
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

impl FileBytesStream {
    /// Create a Hyper `Body` from this stream.
    pub fn into_body(self) -> Body {
        Body::wrap_stream(self)
    }
}

pub struct FileBytesStreamRange {
    file_stream: FileBytesStream,
    seek_state: FileSeekState,
    start_offset: u64,
}

impl FileBytesStreamRange {
    pub fn new(file: File, range: HttpRange) -> FileBytesStreamRange {
        FileBytesStreamRange {
            file_stream: FileBytesStream::new_with_limit(file, range.length),
            seek_state: FileSeekState::NeedSeek,
            start_offset: range.start,
        }
    }
}

#[derive(PartialEq, Eq)]
enum FileSeekState {
    NeedSeek,
    Seeking,
    Reading,
}

impl Stream for FileBytesStreamRange {
    type Item = Result<Bytes, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let Self {
            ref mut file_stream,
            ref mut seek_state,
            start_offset,
        } = *self;
        if *seek_state == FileSeekState::NeedSeek {
            *seek_state = FileSeekState::Seeking;
            if let Err(e) = Pin::new(&mut file_stream.file).start_seek(SeekFrom::Start(start_offset)) {
                return Poll::Ready(Some(Err(e)));
            }
        }
        if *seek_state == FileSeekState::Seeking {
            match Pin::new(&mut file_stream.file).poll_complete(cx) {
                Poll::Ready(Ok(..)) => *seek_state = FileSeekState::Reading,
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
        Pin::new(file_stream).poll_next(cx)
    }
}


impl FileBytesStreamRange {
    /// Create a Hyper `Body` from this stream.
    pub fn into_body(self) -> Body {
        Body::wrap_stream(self)
    }
}

pub struct FileBytesStreamMultiRange {
    file_range: FileBytesStreamRange,
    range_iter: vec::IntoIter<HttpRange>,
    is_first_boundary: bool,
    boundary: String,
    file_length: u64,
}

impl FileBytesStreamMultiRange {
    pub fn new(file: File, ranges: Vec<HttpRange>, boundary: String, file_length: u64) -> FileBytesStreamMultiRange {
        let mut range_iter = ranges.into_iter();
        let first_range = range_iter.next().expect("ranges must not be empty");
        FileBytesStreamMultiRange {
            file_range: FileBytesStreamRange::new(file, first_range),
            range_iter,
            boundary,
            is_first_boundary: true,
            file_length,
        }
    }
}

impl Stream for FileBytesStreamMultiRange {
    type Item = Result<Bytes, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let Self {
            ref mut file_range,
            ref mut range_iter,
            ref mut is_first_boundary,
            ref boundary,
            file_length,
        } = *self;

        if file_range.file_stream.range_remaining == 0 {
            let range = match range_iter.next() {
                Some(r) => r,
                None => return Poll::Ready(None),
            };

            file_range.seek_state = FileSeekState::NeedSeek;
            file_range.file_stream.range_remaining = range.length;

            let mut read_buf = ReadBuf::uninit(&mut file_range.file_stream.buf[..]);
            if *is_first_boundary {
                *is_first_boundary = false;
            } else {
                read_buf.put_slice(b"\r\n");
            }
            read_buf.put_slice(b"--");
            read_buf.put_slice(boundary.as_bytes());
            read_buf.put_slice(b"\r\nContent-Range: bytes ");

            let mut tmp_buffer = [0; 66];
            let mut tmp_storage = Cursor::new(&mut tmp_buffer[..]);
            write!(&mut tmp_storage, "{}-{}/{}\r\n\r\n",
                range.start,
                range.start + range.length - 1,
                file_length,
            ).expect("buffer unexpectedly too small");

            let end_position = tmp_storage.position() as usize;
            let tmp_storage = tmp_storage.into_inner();
            read_buf.put_slice(&tmp_storage[..end_position]);

            return Poll::Ready(Some(Ok(Bytes::copy_from_slice(read_buf.filled()))))
        }
        
        match Pin::new(file_range).poll_next(cx) {
            Poll::Ready(None) => unreachable!(),
            other => return other,
        }
    }
}

impl FileBytesStreamMultiRange {
    /// Create a Hyper `Body` from this stream.
    pub fn into_body(self) -> Body {
        Body::wrap_stream(self)
    }
}
