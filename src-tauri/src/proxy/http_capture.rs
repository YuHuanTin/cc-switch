//! Complete HTTP request/response capture for debug and trace logging.
//!
//! Captures are assembled in memory and appended as one record when the
//! response finishes. This keeps concurrent requests from interleaving in
//! capture.http. SSE responses use the selected existing protocol aggregator;
//! raw event/delta lines are never written.

use http::{HeaderMap, Method, StatusCode};
use serde_json::Value;
use std::sync::{Arc, Mutex, OnceLock};
use std::{fs::OpenOptions, io::Write, path::PathBuf};
use uuid::Uuid;

static CAPTURE_FILE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub(crate) type SseAggregator = fn(&str) -> Result<Value, crate::proxy::ProxyError>;

#[derive(Clone)]
pub(crate) struct HttpCapture {
    inner: Arc<HttpCaptureInner>,
}

struct HttpCaptureInner {
    id: String,
    timestamp: String,
    path: PathBuf,
    state: Mutex<CaptureState>,
}

#[derive(Default)]
struct CaptureState {
    data: Vec<u8>,
    response_body: Vec<u8>,
    sse_aggregator: Option<SseAggregator>,
    response_is_sse: bool,
    response_started: bool,
    response_status: Option<StatusCode>,
    finished: bool,
}

impl HttpCapture {
    pub(crate) fn start(
        method: &Method,
        url: &str,
        headers: &HeaderMap,
        body: &[u8],
        sse_aggregator: Option<SseAggregator>,
    ) -> std::io::Result<Self> {
        let log_dir = crate::panic_hook::get_log_dir();
        std::fs::create_dir_all(&log_dir)?;

        let id = Uuid::new_v4().to_string();
        let timestamp = chrono::Utc::now().to_rfc3339();
        let path = log_dir.join("capture.http");
        let inner = Arc::new(HttpCaptureInner {
            id,
            timestamp,
            path,
            state: Mutex::new(CaptureState {
                sse_aggregator,
                ..CaptureState::default()
            }),
        });
        let capture = Self { inner };

        let mut request = Vec::new();
        append_text(
            &mut request,
            &format!(
                "===== HTTP CAPTURE id={} timestamp={} =====\r\n",
                capture.inner.id, capture.inner.timestamp
            ),
        );
        append_text(&mut request, "----- REQUEST -----\r\n");
        append_text(&mut request, method.as_str());
        append_text(&mut request, " ");
        append_text(&mut request, url);
        append_text(&mut request, " HTTP/1.1\r\n");
        append_request_headers(&mut request, url, headers, body.len());
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(body);
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(b"----- RESPONSE -----\r\n");

        capture
            .inner
            .state
            .lock()
            .map_err(|_| std::io::Error::other("HTTP capture state lock poisoned"))?
            .data
            .extend_from_slice(&request);

        Ok(capture)
    }

    pub(crate) fn begin_response(&self, status: StatusCode, headers: &HeaderMap) {
        let Ok(mut state) = self.inner.state.lock() else {
            log::warn!(
                "[HttpCapture] id={} state lock poisoned while starting response",
                self.inner.id
            );
            return;
        };

        if state.response_started || state.finished {
            return;
        }

        state.response_started = true;
        state.response_status = Some(status);
        state.response_is_sse = headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"));

        append_text(&mut state.data, "HTTP/1.1 ");
        append_text(&mut state.data, &status.as_u16().to_string());
        if let Some(reason) = status.canonical_reason() {
            append_text(&mut state.data, " ");
            append_text(&mut state.data, reason);
        }
        append_text(&mut state.data, "\r\n");
        append_headers(&mut state.data, headers);
        state.data.extend_from_slice(b"\r\n");
    }

    pub(crate) fn append_response_body(&self, body: &[u8]) {
        let Ok(mut state) = self.inner.state.lock() else {
            log::warn!(
                "[HttpCapture] id={} state lock poisoned while appending response body",
                self.inner.id
            );
            return;
        };

        if state.finished {
            return;
        }

        state.response_body.extend_from_slice(body);
    }

    pub(crate) fn finish(&self) {
        self.finish_with_result("complete");
    }

    pub(crate) fn finish_with_error(&self, error: &str) {
        self.finish_with_result(&format!("error: {}", single_line(error)));
    }

    fn finish_with_result(&self, result: &str) {
        let Ok(mut state) = self.inner.state.lock() else {
            log::warn!(
                "[HttpCapture] id={} state lock poisoned while finishing capture",
                self.inner.id
            );
            return;
        };

        if state.finished {
            return;
        }

        state.finished = true;
        let response_body = std::mem::take(&mut state.response_body);
        let response_is_sse = state.response_is_sse;
        let sse_aggregator = state.sse_aggregator;
        let status = state.response_status;
        let mut record = std::mem::take(&mut state.data);
        drop(state);

        append_response_body(
            &mut record,
            &response_body,
            response_is_sse,
            sse_aggregator,
            &self.inner.id,
        );

        append_text(&mut record, "\r\n----- HTTP CAPTURE END ");
        append_text(&mut record, result);
        append_text(&mut record, " -----\r\n");

        let write_result = append_to_file(&self.inner.path, &record);
        if let Err(error) = write_result {
            log::warn!(
                "[HttpCapture] id={} capture.http 写入失败: {}",
                self.inner.id,
                error
            );
            return;
        }

        log::debug!(
            "[HttpCapture] HTTP req/resp 日志 id={} timestamp={} 已输出到 capture.http (status={}, result={})",
            self.inner.id,
            self.inner.timestamp,
            status
                .map(|status| status.as_u16().to_string())
                .unwrap_or_else(|| "no response".to_string()),
            result
        );
    }
}

impl Drop for HttpCaptureInner {
    fn drop(&mut self) {
        let Ok(state) = self.state.get_mut() else {
            return;
        };
        if state.finished {
            return;
        }

        state.finished = true;
        let response_body = std::mem::take(&mut state.response_body);
        let response_is_sse = state.response_is_sse;
        let sse_aggregator = state.sse_aggregator;
        let status = state.response_status;

        let mut record = std::mem::take(&mut state.data);
        append_response_body(
            &mut record,
            &response_body,
            response_is_sse,
            sse_aggregator,
            &self.id,
        );
        append_text(
            &mut record,
            "\r\n----- HTTP CAPTURE END incomplete -----\r\n",
        );

        if let Err(error) = append_to_file(&self.path, &record) {
            log::warn!(
                "[HttpCapture] id={} incomplete capture write failed: {}",
                self.id,
                error
            );
        } else {
            log::debug!(
                "[HttpCapture] HTTP req/resp 日志 id={} timestamp={} 已输出到 capture.http (status={}, result=incomplete)",
                self.id,
                self.timestamp,
                status
                    .map(|status| status.as_u16().to_string())
                    .unwrap_or_else(|| "no response".to_string())
            );
        }
    }
}

fn append_to_file(path: &PathBuf, data: &[u8]) -> std::io::Result<()> {
    let lock = CAPTURE_FILE_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock
        .lock()
        .map_err(|_| std::io::Error::other("HTTP capture file lock poisoned"))?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(data)?;
    file.flush()
}

fn append_text(output: &mut Vec<u8>, text: &str) {
    output.extend_from_slice(text.as_bytes());
}

fn single_line(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\r' | '\n' | '\t' => ' ',
            character => character,
        })
        .collect()
}

fn append_headers(output: &mut Vec<u8>, headers: &HeaderMap) {
    for (name, value) in headers {
        output.extend_from_slice(name.as_str().as_bytes());
        output.extend_from_slice(b": ");
        output.extend_from_slice(value.as_bytes());
        output.extend_from_slice(b"\r\n");
    }
}

fn append_response_body(
    record: &mut Vec<u8>,
    body: &[u8],
    is_sse: bool,
    sse_aggregator: Option<SseAggregator>,
    capture_id: &str,
) {
    if !is_sse {
        record.extend_from_slice(body);
        return;
    }

    let Some(sse_aggregator) = sse_aggregator else {
        append_aggregation_error(record, capture_id, "no SSE aggregator configured");
        return;
    };
    let body = String::from_utf8_lossy(body);
    match sse_aggregator(&body) {
        Ok(response) => match serde_json::to_vec(&response) {
            Ok(response) => record.extend_from_slice(&response),
            Err(error) => append_aggregation_error(record, capture_id, &error.to_string()),
        },
        Err(error) => append_aggregation_error(record, capture_id, &error.to_string()),
    }
}

fn append_aggregation_error(record: &mut Vec<u8>, capture_id: &str, error: &str) {
    log::warn!(
        "[HttpCapture] id={} SSE 最终响应聚合失败: {}",
        capture_id,
        single_line(error)
    );
    append_text(record, "[SSE response aggregation failed: ");
    append_text(record, &single_line(error));
    append_text(record, "]");
}

fn append_request_headers(output: &mut Vec<u8>, url: &str, headers: &HeaderMap, body_len: usize) {
    let has_host = headers.keys().any(|name| name == http::header::HOST);
    let has_content_length = headers
        .keys()
        .any(|name| name == http::header::CONTENT_LENGTH);
    append_headers(output, headers);

    if !has_host {
        if let Ok(parsed) = url::Url::parse(url) {
            if let Some(host) = parsed.host_str() {
                output.extend_from_slice(b"host: ");
                output.extend_from_slice(host.as_bytes());
                if let Some(port) = parsed.port() {
                    output.extend_from_slice(b":");
                    output.extend_from_slice(port.to_string().as_bytes());
                }
                output.extend_from_slice(b"\r\n");
            }
        }
    }

    if !has_content_length {
        output.extend_from_slice(b"content-length: ");
        output.extend_from_slice(body_len.to_string().as_bytes());
        output.extend_from_slice(b"\r\n");
    }
}
