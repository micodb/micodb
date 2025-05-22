// src/auth/jwt.rs

//! JWT token implementation for MicoDB authentication.

use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{encode, decode, Header, Validation, EncodingKey, DecodingKey};
use serde::{Serialize, Deserialize};
use crate::common::error::{MicoError, Result};
use crate::auth::User;

/// JWT token claims
#[derive(Debug, Serialize, Deserialize)]
pub struct TokenClaims {
    /// Subject (username)
    pub sub: String,
    /// Name (display name)
    pub name: String,
    /// Roles
    pub roles: Vec<String>,
    /// Issued at (Unix timestamp)
    pub iat: i64,
    /// Expiration time (Unix timestamp)
    pub exp: i64,
}

/// JWT token manager
pub struct JwtTokenManager {
    /// JWT secret key for signing tokens
    secret: String,
    /// Token validity duration
    token_duration: Duration,
}

impl JwtTokenManager {
    /// Create a new JWT token manager with the given secret
    pub fn new(secret: String) -> Self {
        Self {
            secret,
            // Default token validity: 24 hours
            token_duration: Duration::hours(24),
        }
    }
    
    /// Set a custom token validity duration
    pub fn with_token_duration(mut self, duration: Duration) -> Self {
        self.token_duration = duration;
        self
    }
    
    /// Create a new JWT token for a user
    pub fn create_token(&self, user: &User) -> Result<(String, DateTime<Utc>)> {
        let now = Utc::now();
        let expires_at = now + self.token_duration;
        
        let claims = TokenClaims {
            sub: user.username.clone(),
            name: user.display_name.clone(),
            roles: user.roles.clone(),
            iat: now.timestamp(),
            exp: expires_at.timestamp(),
        };
        
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        ).map_err(|e| MicoError::Internal(format!("Failed to create JWT token: {}", e)))?;
        
        Ok((token, expires_at))
    }
    
    /// Validate a JWT token and extract its claims
    pub fn validate_token(&self, token: &str) -> Result<TokenClaims> {
        let validation = Validation::default();
        
        let token_data = decode::<TokenClaims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        ).map_err(|e| MicoError::Internal(format!("Invalid JWT token: {}", e)))?;
        
        Ok(token_data.claims)
    }
}