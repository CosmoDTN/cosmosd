//! Publisher keys, pairing and trust lookup.
//!
//! A publisher signs content with an Ed25519 key. A node trusts the keys it
//! has paired, plus its own. Keys live in a key dir: `publisher.key` holds the
//! 32 raw private bytes (mode 0600) and `paired.keys` holds one pairing string
//! per line. A key ID is the first 8 bytes of BLAKE3 of the public key, in hex.

pub mod manifest;

use std::fmt;
use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

const KEY_FILE: &str = "publisher.key";
const PAIRED_FILE: &str = "paired.keys";
const PAIRING_PREFIX: &str = "cosmos-key:";

// === Key ID ===

/// 16 lowercase hex chars: the first 8 bytes of BLAKE3 of the public key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyId(pub String);

impl KeyId {
    fn of(public: &[u8; 32]) -> Self {
        let hash = blake3::hash(public);
        KeyId(hash.as_bytes()[..8].iter().map(|b| format!("{b:02x}")).collect())
    }
}

impl fmt::Display for KeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// === Publisher key ===

#[derive(Clone)]
pub struct PublisherKey {
    signing: SigningKey,
}

impl PublisherKey {
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).expect("OS random source available");
        PublisherKey { signing: SigningKey::from_bytes(&seed) }
    }

    pub fn load(dir: &Path) -> Result<Self> {
        let path = dir.join(KEY_FILE);
        let bytes = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        let seed: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow!("{} must hold exactly 32 bytes", path.display()))?;
        Ok(PublisherKey { signing: SigningKey::from_bytes(&seed) })
    }

    pub fn save(&self, dir: &Path) -> Result<()> {
        fs::create_dir_all(dir)?;
        let path = dir.join(KEY_FILE);
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&path).with_context(|| format!("creating {} (it must not already exist)", path.display()))?;
        file.write_all(&self.signing.to_bytes())?;
        Ok(())
    }

    pub fn key_id(&self) -> KeyId {
        KeyId::of(&self.public())
    }

    pub fn public(&self) -> [u8; 32] {
        self.signing.verifying_key().to_bytes()
    }

    pub fn sign(&self, msg: &[u8]) -> [u8; 64] {
        self.signing.sign(msg).to_bytes()
    }
}

pub fn verify(public: &[u8; 32], msg: &[u8], sig: &[u8; 64]) -> bool {
    match VerifyingKey::from_bytes(public) {
        Ok(key) => key.verify(msg, &Signature::from_bytes(sig)).is_ok(),
        Err(_) => false,
    }
}

// === Pairing strings ===

/// `cosmos-key:<base32, lowercase, no padding>`.
pub fn pairing_string(public: &[u8; 32]) -> String {
    format!("{PAIRING_PREFIX}{}", data_encoding::BASE32_NOPAD.encode(public).to_lowercase())
}

pub fn parse_pairing_string(s: &str) -> Result<[u8; 32]> {
    let body = s.trim().strip_prefix(PAIRING_PREFIX).ok_or_else(|| anyhow!("not a {PAIRING_PREFIX} string"))?;
    let bytes = data_encoding::BASE32_NOPAD
        .decode(body.to_uppercase().as_bytes())
        .map_err(|e| anyhow!("bad base32 in pairing string: {e}"))?;
    bytes.try_into().map_err(|_| anyhow!("pairing string must decode to 32 bytes"))
}

// === Paired keys ===

/// Trusted public keys, backed by `paired.keys` in the key dir.
pub struct PairedKeys {
    path: std::path::PathBuf,
    keys: Vec<[u8; 32]>,
}

impl PairedKeys {
    pub fn open(dir: &Path) -> Result<Self> {
        let path = dir.join(PAIRED_FILE);
        let mut keys = Vec::new();
        match fs::read_to_string(&path) {
            Ok(text) => {
                for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
                    keys.push(parse_pairing_string(line).with_context(|| format!("in {}", path.display()))?);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => bail!("reading {}: {e}", path.display()),
        }
        Ok(PairedKeys { path, keys })
    }

    /// Pairing a key that is already paired is a no-op.
    pub fn pair(&mut self, public: [u8; 32]) -> Result<KeyId> {
        if !self.keys.contains(&public) {
            self.keys.push(public);
            self.persist()?;
        }
        Ok(KeyId::of(&public))
    }

    /// Returns false when the key ID was not paired.
    pub fn unpair(&mut self, id: &KeyId) -> Result<bool> {
        let before = self.keys.len();
        self.keys.retain(|k| KeyId::of(k) != *id);
        let removed = self.keys.len() != before;
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    pub fn list(&self) -> Vec<KeyId> {
        self.keys.iter().map(KeyId::of).collect()
    }

    fn persist(&self) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }
        let text: String = self.keys.iter().map(|k| format!("{}\n", pairing_string(k))).collect();
        // Write a sibling temp file then rename, so a crash never leaves a truncated store.
        let tmp = self.path.with_extension("keys.tmp");
        fs::write(&tmp, text).with_context(|| format!("writing {}", tmp.display()))?;
        fs::rename(&tmp, &self.path).with_context(|| format!("replacing {}", self.path.display()))
    }
}

// === Lookup ===

pub trait KeyLookup {
    fn lookup(&self, id: &KeyId) -> Option<[u8; 32]>;
}

impl KeyLookup for PairedKeys {
    fn lookup(&self, id: &KeyId) -> Option<[u8; 32]> {
        self.keys.iter().find(|k| KeyId::of(k) == *id).copied()
    }
}

/// The node's own key plus its paired keys.
pub struct Trust {
    own: Option<PublisherKey>,
    paired: PairedKeys,
}

impl Trust {
    pub fn new(own: Option<PublisherKey>, paired: PairedKeys) -> Self {
        Trust { own, paired }
    }
}

impl KeyLookup for Trust {
    fn lookup(&self, id: &KeyId) -> Option<[u8; 32]> {
        match &self.own {
            Some(own) if own.key_id() == *id => Some(own.public()),
            _ => self.paired.lookup(id),
        }
    }
}

// === Tests ===

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_then_verify_roundtrips() {
        let k = PublisherKey::generate();
        let sig = k.sign(b"hello");
        assert!(verify(&k.public(), b"hello", &sig));
        assert!(!verify(&k.public(), b"hellO", &sig));
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let a = PublisherKey::generate();
        let b = PublisherKey::generate();
        let sig = a.sign(b"msg");
        assert!(!verify(&b.public(), b"msg", &sig));
    }

    #[test]
    fn key_id_is_16_hex() {
        let id = PublisherKey::generate().key_id();
        assert_eq!(id.0.len(), 16);
        assert!(id.0.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')));
        assert_eq!(id.to_string(), id.0);
    }

    #[test]
    fn pairing_string_roundtrips() {
        let k = PublisherKey::generate();
        let s = pairing_string(&k.public());
        assert!(s.starts_with("cosmos-key:"));
        assert_eq!(parse_pairing_string(&s).unwrap(), k.public());
    }

    #[test]
    fn parse_pairing_string_rejects_garbage() {
        assert!(parse_pairing_string("").is_err());
        assert!(parse_pairing_string("cosmos-key:").is_err());
        assert!(parse_pairing_string("cosmos-key:!!!!").is_err());
        assert!(parse_pairing_string("other:aaaa").is_err());
        // Valid base32 but the wrong length.
        assert!(parse_pairing_string("cosmos-key:aaaa").is_err());
    }

    #[test]
    fn pair_then_lookup_then_unpair() {
        let dir = tempfile::tempdir().unwrap();
        let k = PublisherKey::generate();
        let mut paired = PairedKeys::open(dir.path()).unwrap();
        let id = paired.pair(k.public()).unwrap();
        assert_eq!(id, k.key_id());
        assert_eq!(paired.lookup(&id), Some(k.public()));
        assert_eq!(paired.list(), vec![id.clone()]);

        // Pairing is persisted and idempotent.
        paired.pair(k.public()).unwrap();
        let mut reopened = PairedKeys::open(dir.path()).unwrap();
        assert_eq!(reopened.list(), vec![id.clone()]);

        assert!(reopened.unpair(&id).unwrap());
        assert!(!reopened.unpair(&id).unwrap());
        assert_eq!(reopened.lookup(&id), None);
        assert!(PairedKeys::open(dir.path()).unwrap().list().is_empty());
    }

    #[test]
    fn key_save_then_load_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let k = PublisherKey::generate();
        k.save(dir.path()).unwrap();
        assert_eq!(PublisherKey::load(dir.path()).unwrap().public(), k.public());
    }

    #[test]
    fn save_refuses_to_overwrite_existing_key() {
        let dir = tempfile::tempdir().unwrap();
        let first = PublisherKey::generate();
        first.save(dir.path()).unwrap();
        assert!(PublisherKey::generate().save(dir.path()).is_err());
        assert_eq!(PublisherKey::load(dir.path()).unwrap().public(), first.public());
    }

    #[test]
    fn persist_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let k = PublisherKey::generate();
        let mut paired = PairedKeys::open(dir.path()).unwrap();
        let id = paired.pair(k.public()).unwrap();
        paired.unpair(&id).unwrap();
        paired.pair(k.public()).unwrap();
        let names: Vec<_> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(names, vec![std::ffi::OsString::from(PAIRED_FILE)]);
    }

    #[cfg(unix)]
    #[test]
    fn saved_key_file_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        PublisherKey::generate().save(dir.path()).unwrap();
        let mode = std::fs::metadata(dir.path().join("publisher.key")).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn trust_resolves_own_and_paired_keys() {
        let dir = tempfile::tempdir().unwrap();
        let own = PublisherKey::generate();
        let other = PublisherKey::generate();
        let mut paired = PairedKeys::open(dir.path()).unwrap();
        paired.pair(other.public()).unwrap();
        let trust = Trust::new(Some(own.clone()), paired);
        assert_eq!(trust.lookup(&own.key_id()), Some(own.public()));
        assert_eq!(trust.lookup(&other.key_id()), Some(other.public()));
        let stranger = PublisherKey::generate();
        assert_eq!(trust.lookup(&stranger.key_id()), None);
    }
}
