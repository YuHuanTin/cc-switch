//! Complete HTTP request/response capture for debug and trace logging.
//!
//! Captures are assembled in memory and appended as one record when the
//! response finishes. This keeps concurrent requests from interleaving in
//! capture.http; only the terminal response.completed SSE event is kept.

use http::{HeaderMap, Method, StatusCode};
use std::sync::{Arc, Mutex, OnceLock};
use std::{fs::OpenOptions, io::Write, path::PathBuf};
use uuid::Uuid;

static CAPTURE_FILE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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
    body_filter: ResponseBodyFilter,
    response_started: bool,
    response_status: Option<StatusCode>,
    finished: bool,
}

#[derive(Default)]
struct ResponseBodyFilter {
    mode: ResponseBodyFilterMode,
}

#[derive(Default)]
enum ResponseBodyFilterMode {
    #[default]
    Raw,
    Sse(SseCaptureFilter),
}

#[derive(Default)]
struct SseCaptureFilter {
    pending: Vec<u8>,
}

impl ResponseBodyFilter {
    fn configure(&mut self, headers: &HeaderMap) {
        let is_sse = headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"));

        self.mode = if is_sse {
            ResponseBodyFilterMode::Sse(SseCaptureFilter::default())
        } else {
            ResponseBodyFilterMode::Raw
        };
    }

    fn append(&mut self, data: &[u8]) -> Vec<u8> {
        match &mut self.mode {
            ResponseBodyFilterMode::Raw => data.to_vec(),
            ResponseBodyFilterMode::Sse(filter) => filter.append(data),
        }
    }

    fn finish(&mut self) -> Vec<u8> {
        match &mut self.mode {
            ResponseBodyFilterMode::Raw => Vec::new(),
            ResponseBodyFilterMode::Sse(filter) => filter.finish(),
        }
    }
}

impl SseCaptureFilter {
    fn append(&mut self, data: &[u8]) -> Vec<u8> {
        self.pending.extend_from_slice(data);
        let mut output = Vec::new();

        while let Some((event_end, next_event)) = find_sse_event(&self.pending) {
            if is_response_completed_event(&self.pending[..event_end]) {
                output.extend_from_slice(&self.pending[..next_event]);
            }
            self.pending.drain(..next_event);
        }

        output
    }

    fn finish(&mut self) -> Vec<u8> {
        let pending = std::mem::take(&mut self.pending);
        if is_response_completed_event(&pending) {
            pending
        } else {
            Vec::new()
        }
    }
}

impl HttpCapture {
    pub(crate) fn start(
        method: &Method,
        url: &str,
        headers: &HeaderMap,
        body: &[u8],
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
            state: Mutex::new(CaptureState::default()),
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
        state.body_filter.configure(headers);

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

        let captured = state.body_filter.append(body);
        state.data.extend_from_slice(&captured);
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
        let trailing_body = state.body_filter.finish();
        state.data.extend_from_slice(&trailing_body);
        let status = state.response_status;
        let mut record = std::mem::take(&mut state.data);
        drop(state);

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
        let trailing_body = state.body_filter.finish();
        state.data.extend_from_slice(&trailing_body);
        let status = state.response_status;

        let mut record = std::mem::take(&mut state.data);
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

fn find_sse_event(buffer: &[u8]) -> Option<(usize, usize)> {
    let mut line_start = 0;
    let mut index = 0;

    while index < buffer.len() {
        let line_break_len = match buffer[index] {
            b'\r' if buffer.get(index + 1) == Some(&b'\n') => 2,
            b'\r' if buffer.get(index + 1).is_none() => return None,
            b'\r' | b'\n' => 1,
            _ => {
                index += 1;
                continue;
            }
        };

        if line_start == index {
            return Some((index, index + line_break_len));
        }

        line_start = index + line_break_len;
        index = line_start;
    }

    None
}

fn is_response_completed_event(block: &[u8]) -> bool {
    let Ok(block) = std::str::from_utf8(block) else {
        return false;
    };

    let mut data_lines = Vec::new();
    for line in block.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(event) = sse_field(line, "event") {
            if event.trim() == "response.completed" {
                return true;
            }
        }
        if let Some(data) = sse_field(line, "data") {
            data_lines.push(data);
        }
    }

    if data_lines.is_empty() {
        return false;
    }

    let data = data_lines.join("\n");
    serde_json::from_str::<serde_json::Value>(&data)
        .ok()
        .is_some_and(|value| {
            value
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|event_type| event_type == "response.completed")
        })
}

fn sse_field<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    line.strip_prefix(field)
        .and_then(|value| value.strip_prefix(':'))
        .map(|value| value.strip_prefix(' ').unwrap_or(value))
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

#[cfg(test)]
mod tests {
    use super::{ResponseBodyFilter, ResponseBodyFilterMode};

    fn sse_filter() -> ResponseBodyFilter {
        ResponseBodyFilter {
            mode: ResponseBodyFilterMode::Sse(Default::default()),
        }
    }

    #[test]
    fn keeps_only_response_completed_events() {
        let mut filter = sse_filter();
        let input = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"large text\"}\n\n",
            "event: response.custom_tool_call_input.delta\n",
            "data: {\"type\":\"response.custom_tool_call_input.delta\",\"delta\":\"large tool input\"}\n\n",
            "event: response.output_text.done\n",
            "data: {\"type\":\"response.output_text.done\",\"text\":\"large text\"}\n\n",
            "event: response.failed\n",
            "data: {\"type\":\"response.failed\",\"error\":{}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );

        let output = filter.append(input.as_bytes());
        let output = String::from_utf8_lossy(&output);
        assert!(!output.contains("output_text.delta"));
        assert!(!output.contains("custom_tool_call_input.delta"));
        assert!(!output.contains("output_text.done"));
        assert!(!output.contains("response.failed"));
        assert!(output.contains("response.completed"));
    }

    #[test]
    fn separate_clients_keep_sse_partial_events_isolated() {
        let mut client_a = sse_filter();
        let mut client_b = sse_filter();

        let a_first = b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":";
        let b_first = b"event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"text\":";
        let a_second = b"\"client-a\"}\n\n";
        let b_second = b"\"client-b\"}\n\n";

        assert!(client_a.append(a_first).is_empty());
        assert!(client_b.append(b_first).is_empty());

        let a_output = client_a.append(a_second);
        let b_output = client_b.append(b_second);

        assert!(String::from_utf8_lossy(&a_output).is_empty());
        assert!(String::from_utf8_lossy(&b_output).is_empty());
        assert!(String::from_utf8_lossy(&client_a.finish()).is_empty());
        assert!(String::from_utf8_lossy(&client_b.finish()).is_empty());
    }

    #[test]
    fn filters_data_type_without_event_field_and_preserves_utf8_terminal_data() {
        let mut filter = sse_filter();
        let delta = concat!("data: {\"type\":\"response.output_text.delta\",\"delta\":\"你\"}\n\n");
        let terminal = "data: {\"type\":\"response.completed\",\"text\":\"你好\"}\n\n";

        let mut output = filter.append(&delta.as_bytes()[..delta.len() - 1]);
        output.extend_from_slice(&filter.append(&delta.as_bytes()[delta.len() - 1..]));
        output.extend_from_slice(&filter.append(terminal.as_bytes()));

        let output = String::from_utf8(output).expect("terminal SSE remains valid UTF-8");
        assert!(!output.contains("output_text.delta"));
        assert!(output.contains("你好"));
    }

    #[test]
    fn parses_crlf_multiline_data_and_drops_unfinished_events() {
        let mut filter = sse_filter();
        let first = b"event: response.completed\r\ndata: {\r\ndata: \"type\":\"response.completed\",\r\ndata: \"text\":\"done\"}\r\n\r\n";
        let second = b"event: response.output_text.done\r\ndata: {\"type\":\"response.output_text.done\"}\r\n\r\n";

        let output = filter.append(&first[..17]);
        assert!(output.is_empty());
        let mut output = filter.append(&first[17..]);
        output.extend_from_slice(&filter.append(second));

        let output = String::from_utf8(output).expect("completed SSE remains UTF-8");
        assert!(output.contains("response.completed"));
        assert!(!output.contains("response.output_text.done"));

        let unfinished = b"event: response.completed\ndata: {\"type\":\"response.completed\"}";
        assert!(filter.append(unfinished).is_empty());
        assert!(filter.finish().is_empty());
    }
}
