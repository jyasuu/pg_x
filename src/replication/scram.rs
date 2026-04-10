use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};

use super::error::{ReplError, ReplResult};

type HmacSha256 = Hmac<Sha256>;

pub struct ScramClient {
    pub client_nonce_b64: String,
    pub client_first_bare: String,
    pub client_first: String,
}

impl ScramClient {
    pub fn new(username: &str) -> Self {
        let mut nonce = [0u8; 18];
        rand::rng().fill_bytes(&mut nonce);
        let nonce_b64 = B64.encode(nonce);
        let user = sasl_escape(username);
        let client_first_bare = format!("n={user},r={nonce_b64}");
        let client_first = format!("n,,{client_first_bare}");
        Self { client_nonce_b64: nonce_b64, client_first_bare, client_first }
    }

    pub fn client_final(
        &self,
        password: &str,
        server_first: &str,
    ) -> ReplResult<(String, String, Vec<u8>)> {
        let (rnonce, salt_b64, iters) = parse_server_first(server_first)?;

        if !rnonce.starts_with(&self.client_nonce_b64) {
            return Err(ReplError::Auth("SCRAM nonce mismatch".into()));
        }
        let salt = B64.decode(salt_b64.as_bytes())
            .map_err(|e| ReplError::Auth(format!("SCRAM invalid salt base64: {e}")))?;

        let channel_binding = "biws"; // base64("n,,")
        let client_final_wo_proof = format!("c={channel_binding},r={rnonce}");
        let auth_message = format!("{},{},{}", self.client_first_bare, server_first, client_final_wo_proof);

        let salted_password = hi_sha256(password.as_bytes(), &salt, iters);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = Sha256::digest(&client_key);
        let client_sig = hmac_sha256(stored_key.as_slice(), auth_message.as_bytes());
        let proof = xor_bytes(&client_key, &client_sig);
        let proof_b64 = B64.encode(proof);

        let client_final = format!("{client_final_wo_proof},p={proof_b64}");
        Ok((client_final, auth_message, salted_password))
    }

    pub fn verify_server_final(
        server_final: &str,
        salted_password: &[u8],
        auth_message: &str,
    ) -> ReplResult<()> {
        if let Some(err) = server_final.split(',').find_map(|p| p.strip_prefix("e=")) {
            return Err(ReplError::Auth(format!("SCRAM server error: {err}")));
        }
        let v = server_final.split(',')
            .find_map(|p| p.strip_prefix("v="))
            .ok_or_else(|| ReplError::Auth("SCRAM missing server signature".into()))?;

        let server_sig = B64.decode(v.trim().as_bytes())
            .map_err(|e| ReplError::Auth(format!("SCRAM invalid signature base64: {e}")))?;

        let server_key = hmac_sha256(salted_password, b"Server Key");
        let expected = hmac_sha256(&server_key, auth_message.as_bytes());

        let ok = server_sig.len() == expected.len()
            && server_sig.iter().zip(&expected).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0;

        if !ok {
            return Err(ReplError::Auth("SCRAM signature mismatch".into()));
        }
        Ok(())
    }
}

fn parse_server_first(s: &str) -> ReplResult<(String, String, u32)> {
    let mut r = None; let mut salt = None; let mut iters = None;
    for part in s.split(',') {
        if let Some(v) = part.strip_prefix("r=") { r = Some(v.to_string()); }
        else if let Some(v) = part.strip_prefix("s=") { salt = Some(v.to_string()); }
        else if let Some(v) = part.strip_prefix("i=") { iters = v.parse::<u32>().ok(); }
    }
    Ok((
        r.ok_or_else(|| ReplError::Auth("SCRAM: missing nonce (r=)".into()))?,
        salt.ok_or_else(|| ReplError::Auth("SCRAM: missing salt (s=)".into()))?,
        iters.ok_or_else(|| ReplError::Auth("SCRAM: missing iterations (i=)".into()))?,
    ))
}

fn sasl_escape(u: &str) -> String {
    u.replace('=', "=3D").replace(',', "=2C")
}

fn hi_sha256(password: &[u8], salt: &[u8], iters: u32) -> Vec<u8> {
    let mut s1 = Vec::with_capacity(salt.len() + 4);
    s1.extend_from_slice(salt);
    s1.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac_sha256(password, &s1);
    let mut out = u.clone();
    for _ in 1..iters {
        u = hmac_sha256(password, &u);
        for (o, ui) in out.iter_mut().zip(u.iter()) { *o ^= *ui; }
    }
    out
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}
