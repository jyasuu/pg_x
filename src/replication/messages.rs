use bytes::Buf;
use super::error::{ReplError, ReplResult};

pub fn parse_error_response(payload: &[u8]) -> String {
    let mut message = None;
    let mut code = None;
    let mut b = payload;
    while !b.is_empty() {
        let field = b[0]; b = &b[1..];
        if field == 0 { break; }
        if let Some(pos) = b.iter().position(|&x| x == 0) {
            let s = String::from_utf8_lossy(&b[..pos]).to_string();
            match field { b'M' => message = Some(s), b'C' => code = Some(s), _ => {} }
            b = &b[pos + 1..];
        } else { break; }
    }
    match (message, code) {
        (Some(m), Some(c)) => format!("{m} (SQLSTATE {c})"),
        (Some(m), None)    => m,
        (None, Some(c))    => format!("error (SQLSTATE {c})"),
        (None, None)       => "unknown server error".to_string(),
    }
}

/// Returns (auth_code, remaining_bytes).
pub fn parse_auth_request(payload: &[u8]) -> ReplResult<(i32, &[u8])> {
    if payload.len() < 4 {
        return Err(ReplError::Protocol("auth request too short".into()));
    }
    let mut b = payload;
    let code = b.get_i32();
    Ok((code, b))
}

pub fn parse_sasl_mechanisms(data: &[u8]) -> Vec<String> {
    let mut mechanisms = Vec::new();
    let mut remaining = data;
    while !remaining.is_empty() {
        if let Some(pos) = remaining.iter().position(|&x| x == 0) {
            if pos == 0 { break; }
            mechanisms.push(String::from_utf8_lossy(&remaining[..pos]).to_string());
            remaining = &remaining[pos + 1..];
        } else { break; }
    }
    mechanisms
}
