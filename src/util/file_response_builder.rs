use super::{FileBytesStream, FileBytesStreamRange, FileBytesStreamMultiRange};
use crate::util::DateTimeHttp;
use chrono::{offset::Local as LocalTz, DateTime, SubsecRound};
use http::response::Builder as ResponseBuilder;
use http::{header, HeaderMap, Method, Request, Response, Result, StatusCode};
use hyper::Body;
use std::fs::Metadata;
use std::time::{Duration, UNIX_EPOCH};
use tokio::fs::File;
use http_range::HttpRange;
use rand::prelude::{thread_rng, SliceRandom};


/// Minimum duration since Unix epoch we accept for file modification time.
///
/// This is intended to discard invalid times, specifically:
///  - Zero values on any Unix system.
///  - 'Epoch + 1' on NixOS.
const MIN_VALID_MTIME: Duration = Duration::from_secs(2);

const BOUNDARY_LENGTH: usize = 60;
const BOUNDARY_CHARS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Utility to build responses for serving a `tokio::fs::File`.
///
/// This struct allows direct access to its fields, but these fields are typically initialized by
/// the accessors, using the builder pattern. The fields are basically a bunch of settings that
/// determine the response details.
#[derive(Clone, Debug, Default)]
pub struct FileResponseBuilder {
    /// Whether to send cache headers, and what lifespan to indicate.
    pub cache_headers: Option<u32>,
    /// Whether this is a `HEAD` request, with no response body.
    pub is_head: bool,
    /// The parsed value of the `If-Modified-Since` request header.
    pub if_modified_since: Option<DateTime<LocalTz>>,
    /// The file ranges to read, if any, otherwise we read from the beginning.
    pub range: Option<String>,
    /// The unparsed value of the `If-Range` request header. May match etag or last-modified.
    pub if_range: Option<String>,
}

impl FileResponseBuilder {
    /// Create a new builder with a default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply parameters based on a request.
    pub fn request<B>(&mut self, req: &Request<B>) -> &mut Self {
        self.request_parts(req.method(), req.headers())
    }

    /// Apply parameters based on request parts.
    pub fn request_parts(&mut self, method: &Method, headers: &HeaderMap) -> &mut Self {
        self.request_method(method);
        self.request_headers(headers);
        self
    }

    /// Apply parameters based on a request method.
    pub fn request_method(&mut self, method: &Method) -> &mut Self {
        self.is_head = *method == Method::HEAD;
        self
    }

    /// Apply parameters based on request headers.
    pub fn request_headers(&mut self, headers: &HeaderMap) -> &mut Self {
        self.if_modified_since_header(headers.get(header::IF_MODIFIED_SINCE));
        self.range_header(headers.get(header::RANGE));
        self
    }

    /// Add cache headers to responses for the given lifespan.
    pub fn cache_headers(&mut self, value: Option<u32>) -> &mut Self {
        self.cache_headers = value;
        self
    }

    /// Set whether this is a `HEAD` request, with no response body.
    pub fn is_head(&mut self, value: bool) -> &mut Self {
        self.is_head = value;
        self
    }

    /// Build responses for the given `If-Modified-Since` date-time.
    pub fn if_modified_since(&mut self, value: Option<DateTime<LocalTz>>) -> &mut Self {
        self.if_modified_since = value;
        self
    }

    /// Build responses for the given `If-Modified-Since` request header value.
    pub fn if_modified_since_header(&mut self, value: Option<&header::HeaderValue>) -> &mut Self {
        self.if_modified_since = value
            .and_then(|v| v.to_str().ok())
            .and_then(|v| DateTime::parse_from_rfc2822(v).ok())
            .map(|v| v.with_timezone(&LocalTz));
        self
    }

    /// Build responses for the given `If-Range` request header value.
    pub fn if_range(&mut self, value: Option<String>) -> &mut Self {
        self.if_range = value;
        self
    }

    /// Build responses for the given `Range` request header value.
    pub fn range_header(&mut self, value: Option<&header::HeaderValue>) -> &mut Self {
        self.range = value
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string());
        self
    }

    /// Build a response for the given file and metadata.
    pub fn build(&self, file: File, metadata: Metadata, content_type: String) -> Result<Response<Body>> {
        let mut res = ResponseBuilder::new();

        // Set `Last-Modified` and check `If-Modified-Since`.
        let modified: Option<DateTime<LocalTz>> = metadata.modified().ok().filter(|v| {
            v.duration_since(UNIX_EPOCH)
                .ok()
                .filter(|v| v >= &MIN_VALID_MTIME)
                .is_some()
        });

        let mut range_cond_ok = true;
        if let Some(modified) = modified {
            let modified: DateTime<LocalTz> = modified.into();

            match self.if_modified_since {
                // Truncate before comparison, because the `Last-Modified` we serve
                // is also truncated in the HTTP date-time format.
                Some(v) if modified.trunc_subsecs(0) <= v.trunc_subsecs(0) => {
                    return ResponseBuilder::new()
                        .status(StatusCode::NOT_MODIFIED)
                        .body(Body::empty())
                }
                _ => {}
            }

            let etag = format!(
                "W/\"{0:x}-{1:x}.{2:x}\"",
                metadata.len(),
                modified.timestamp(),
                modified.timestamp_subsec_nanos()
            );

            if let Some(ref v) = self.if_range {
                
                range_cond_ok = *v == modified.to_http_date() || *v == etag;
            }

            res = res
                .header(header::LAST_MODIFIED, modified.to_http_date())
                .header(header::ETAG, etag)
                .header(header::ACCEPT_RANGES, "bytes");
        }

        // Build remaining headers.
        if let Some(seconds) = self.cache_headers {
            res = res.header(
                header::CACHE_CONTROL,
                format!("public, max-age={}", seconds),
            );
        }

        if self.is_head {
            res = res.header(header::CONTENT_LENGTH, format!("{}", metadata.len()));
            return res.status(StatusCode::OK).body(Body::empty());
        }

        let ranges = self.range
            .as_ref()
            .filter(|_| range_cond_ok)
            .and_then(|r| HttpRange::parse(r, metadata.len()).ok());

        if let Some(ranges) = ranges {
            if ranges.len() == 1 {
                let single_span = ranges[0];
                res = res
                    .header(header::CONTENT_RANGE, content_range_header(&single_span, metadata.len()))
                    .header(header::CONTENT_LENGTH, format!("{}", single_span.length));

                let body_stream = FileBytesStreamRange::new(file, single_span);
                return res.status(StatusCode::PARTIAL_CONTENT).body(body_stream.into_body());
            } else if ranges.len() > 1 {
                let mut boundary_tmp = [0u8; BOUNDARY_LENGTH];

                let mut rng = thread_rng();
                for v in boundary_tmp.iter_mut() {
                    // won't panic since BOUNDARY_CHARS is non-empty
                    *v = *BOUNDARY_CHARS.choose(&mut rng).unwrap();
                }

                // won't panic because boundary_tmp is guaranteed to be all ASCII
                let boundary = std::str::from_utf8(&boundary_tmp[..]).unwrap().to_string();

                res = res.header(hyper::header::CONTENT_TYPE,
                    format!("multipart/byteranges; boundary={}", boundary));

                let mut body_stream = FileBytesStreamMultiRange::new(file, ranges, boundary, metadata.len());
                if !content_type.is_empty() {
                    body_stream.set_content_type(&content_type);
                }
                return res.status(StatusCode::PARTIAL_CONTENT).body(body_stream.into_body());
            }
        }
        
        res = res.header(header::CONTENT_LENGTH, format!("{}", metadata.len()));
        if !content_type.is_empty() {
            res = res.header(header::CONTENT_TYPE, content_type);
        }

        // Stream the body.
        res.status(StatusCode::OK).body(FileBytesStream::new(file).into_body())
    }
}

fn content_range_header(r: &HttpRange, total_length: u64) -> String {
    format!("bytes {}-{}/{}", r.start, r.start + r.length - 1, total_length)
}


