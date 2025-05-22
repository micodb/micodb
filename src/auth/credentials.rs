// src/auth/credentials.rs

//! User credential management for MicoDB authentication.

use serde::{Serialize, Deserialize};
use argon2::{
    password_hash::{
        rand_core::OsRng,
        PasswordHash, PasswordHasher as PwHasher, PasswordVerifier, SaltString
    },
    Argon2
};
use crate::common::error::{MicoError, Result};

/// Credentials for user authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Credentials {
    /// Password-based authentication
    Password {
        /// Argon2 hashed password
        password_hash: String,
    },
    
    /// API key authentication
    ApiKey {
        /// Argon2 hashed API key
        key_hash: String,
    },
    
    /// External authentication (e.g., OAuth)
    External {
        /// External provider (e.g., "github", "google")
        provider: String,
        /// External user ID
        provider_user_id: String,
    },
}

/// Helper for password hashing and verification
pub struct PasswordHasher;

impl PasswordHasher {
    /// Hash a password or API key using Argon2
    pub fn hash_password(password: &str) -> Result<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        
        let password_hash = argon2.hash_password(password.as_bytes(), &salt)
            .map_err(|e| MicoError::Internal(format!("Failed to hash password: {}", e)))?
            .to_string();
            
        Ok(password_hash)
    }
    
    /// Verify a password or API key against its hash
    pub fn verify_password(password: &str, hash: &str) -> Result<bool> {
        let parsed_hash = PasswordHash::new(hash)
            .map_err(|e| MicoError::Internal(format!("Failed to parse password hash: {}", e)))?;
            
        let result = Argon2::default().verify_password(password.as_bytes(), &parsed_hash).is_ok();
        Ok(result)
    }
}