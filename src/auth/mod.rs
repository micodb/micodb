// src/auth/mod.rs

//! Authentication and authorization module for MicoDB.
//! 
//! This module provides:
//! 1. User authentication via several methods (password, JWT, API key)
//! 2. Role-based access control for databases and tables
//! 3. Permission management for various operations

mod credentials;
mod jwt;
mod permissions;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use crate::common::error::{MicoError, Result};
use async_trait::async_trait;

pub use credentials::{Credentials, PasswordHasher};
pub use jwt::{JwtTokenManager, TokenClaims};
pub use permissions::{Permission, Role, ResourceType};

/// Authentication method types supported by MicoDB
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuthMethod {
    /// Username and password authentication
    Password,
    /// JWT token-based authentication
    Jwt,
    /// API key authentication
    ApiKey,
}

/// A MicoDB user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Unique user identifier
    pub id: String,
    /// Username for login
    pub username: String,
    /// User's display name
    pub display_name: String,
    /// Authentication credentials
    #[serde(skip_serializing)]
    pub credentials: Credentials,
    /// Authentication method
    pub auth_method: AuthMethod,
    /// Global roles
    pub roles: Vec<String>,
    /// Whether the account is active
    pub is_active: bool,
    /// When the user was created
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last login time
    pub last_login: Option<chrono::DateTime<chrono::Utc>>,
}

impl User {
    /// Create a new user with password authentication
    pub fn new_with_password(
        username: &str,
        display_name: &str,
        password: &str,
        roles: Vec<String>,
    ) -> Result<Self> {
        let id = Uuid::new_v4().to_string();
        let password_hash = PasswordHasher::hash_password(password)?;
        
        Ok(Self {
            id,
            username: username.to_string(),
            display_name: display_name.to_string(),
            credentials: Credentials::Password { password_hash },
            auth_method: AuthMethod::Password,
            roles,
            is_active: true,
            created_at: chrono::Utc::now(),
            last_login: None,
        })
    }
    
    /// Create a new user with API key authentication
    pub fn new_with_api_key(
        username: &str,
        display_name: &str,
        roles: Vec<String>,
    ) -> Result<(Self, String)> {
        let id = Uuid::new_v4().to_string();
        let api_key = Uuid::new_v4().to_string();
        let api_key_hash = PasswordHasher::hash_password(&api_key)?;
        
        let user = Self {
            id,
            username: username.to_string(),
            display_name: display_name.to_string(),
            credentials: Credentials::ApiKey { key_hash: api_key_hash },
            auth_method: AuthMethod::ApiKey,
            roles,
            is_active: true,
            created_at: chrono::Utc::now(),
            last_login: None,
        };
        
        Ok((user, api_key))
    }
    
    /// Check if the user has a specific role
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
    
    /// Update the last login time to now
    pub fn update_last_login(&mut self) {
        self.last_login = Some(chrono::Utc::now());
    }
}

/// Database-specific permissions for a role
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabasePermissions {
    /// The database name
    pub db_name: String,
    /// Tables with specific permissions (overrides db-level permissions)
    pub tables: HashMap<String, HashSet<Permission>>,
    /// Database-wide permissions
    pub permissions: HashSet<Permission>,
}

/// A role in the system with associated permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleDefinition {
    /// Role name
    pub name: String,
    /// Role description
    pub description: String,
    /// Global permissions
    pub global_permissions: HashSet<Permission>,
    /// Database-specific permissions
    pub database_permissions: HashMap<String, DatabasePermissions>,
    /// Whether this is a system role (cannot be deleted)
    pub is_system_role: bool,
}

/// Authentication result containing user and token info
#[derive(Debug, Clone)]
pub struct AuthResult {
    /// The authenticated user
    pub user: User,
    /// JWT token for subsequent requests (if using JWT auth)
    pub token: Option<String>,
    /// Token expiration time (if using JWT auth)
    pub token_expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Authentication trait for different auth methods
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Authenticate a user with username/password
    async fn authenticate_password(&self, username: &str, password: &str) -> Result<AuthResult>;
    
    /// Authenticate a user with JWT token
    async fn authenticate_token(&self, token: &str) -> Result<AuthResult>;
    
    /// Authenticate a user with API key
    async fn authenticate_api_key(&self, key: &str) -> Result<AuthResult>;
    
    /// Create a new user
    async fn create_user(&self, user: User) -> Result<User>;
    
    /// Get a user by username
    async fn get_user(&self, username: &str) -> Result<User>;
    
    /// Check if a user has permission for an operation
    async fn check_permission(
        &self,
        user: &User,
        permission: Permission,
        resource_type: ResourceType,
        resource_name: &str,
    ) -> Result<bool>;
}

/// The authentication service for MicoDB
pub struct AuthService {
    /// User store
    users: RwLock<HashMap<String, User>>,
    /// Role definitions
    roles: RwLock<HashMap<String, RoleDefinition>>,
    /// JWT token manager (if using JWT auth)
    token_manager: Option<JwtTokenManager>,
    /// Path to the user store file
    users_file_path: PathBuf,
    /// Path to the roles file
    roles_file_path: PathBuf,
}

impl AuthService {
    /// Create a new authentication service
    pub async fn new(
        data_dir: impl AsRef<Path>,
        jwt_secret: Option<String>,
    ) -> Result<Self> {
        let auth_dir = data_dir.as_ref().join("_auth");
        
        // Create the auth directory if it doesn't exist
        if !auth_dir.exists() {
            fs::create_dir_all(&auth_dir).await.map_err(MicoError::Io)?;
        }
        
        let users_file_path = auth_dir.join("users.json");
        let roles_file_path = auth_dir.join("roles.json");
        
        // Load users if the file exists
        let users = if users_file_path.exists() {
            let content = fs::read_to_string(&users_file_path).await.map_err(MicoError::Io)?;
            serde_json::from_str::<HashMap<String, User>>(&content)
                .map_err(|e| MicoError::Internal(format!("Failed to parse users file: {}", e)))?
        } else {
            // Create default admin user if no users exist
            let mut users = HashMap::new();
            let admin = User::new_with_password(
                "admin",
                "Administrator",
                "admin123", // Default password
                vec!["admin".to_string()],
            )?;
            users.insert(admin.username.clone(), admin);
            
            // Write the initial users file
            let content = serde_json::to_string_pretty(&users)
                .map_err(|e| MicoError::Internal(format!("Failed to serialize users: {}", e)))?;
            fs::write(&users_file_path, content).await.map_err(MicoError::Io)?;
            
            users
        };
        
        // Load roles if the file exists
        let roles = if roles_file_path.exists() {
            let content = fs::read_to_string(&roles_file_path).await.map_err(MicoError::Io)?;
            serde_json::from_str::<HashMap<String, RoleDefinition>>(&content)
                .map_err(|e| MicoError::Internal(format!("Failed to parse roles file: {}", e)))?
        } else {
            // Create default roles if no roles exist
            let mut roles = HashMap::new();
            
            // Admin role with full permissions
            let admin_perms = Permission::all();
            let admin_role = RoleDefinition {
                name: "admin".to_string(),
                description: "Administrator with full access".to_string(),
                global_permissions: admin_perms,
                database_permissions: HashMap::new(),
                is_system_role: true,
            };
            
            // Read-only role
            let read_perms = HashSet::from([
                Permission::Read,
                Permission::ReadSchema,
                Permission::ExecuteQuery,
            ]);
            let readonly_role = RoleDefinition {
                name: "readonly".to_string(),
                description: "Read-only access".to_string(),
                global_permissions: read_perms,
                database_permissions: HashMap::new(),
                is_system_role: true,
            };
            
            // Power user role (read + write but no admin)
            let power_perms = HashSet::from([
                Permission::Read,
                Permission::ReadSchema,
                Permission::ExecuteQuery,
                Permission::Write,
                Permission::CreateTable,
                Permission::CreateDatabase,
                Permission::BeginTransaction,
                Permission::CommitTransaction,
                Permission::RollbackTransaction,
            ]);
            let power_role = RoleDefinition {
                name: "poweruser".to_string(),
                description: "Power user with read/write access".to_string(),
                global_permissions: power_perms,
                database_permissions: HashMap::new(),
                is_system_role: true,
            };
            
            roles.insert(admin_role.name.clone(), admin_role);
            roles.insert(readonly_role.name.clone(), readonly_role);
            roles.insert(power_role.name.clone(), power_role);
            
            // Write the initial roles file
            let content = serde_json::to_string_pretty(&roles)
                .map_err(|e| MicoError::Internal(format!("Failed to serialize roles: {}", e)))?;
            fs::write(&roles_file_path, content).await.map_err(MicoError::Io)?;
            
            roles
        };
        
        // Create token manager if JWT secret is provided
        let token_manager = jwt_secret.map(JwtTokenManager::new);
        
        Ok(Self {
            users: RwLock::new(users),
            roles: RwLock::new(roles),
            token_manager,
            users_file_path,
            roles_file_path,
        })
    }
    
    /// Save users to the users file
    async fn save_users(&self) -> Result<()> {
        let users = self.users.read().await;
        let content = serde_json::to_string_pretty(&*users)
            .map_err(|e| MicoError::Internal(format!("Failed to serialize users: {}", e)))?;
        fs::write(&self.users_file_path, content).await.map_err(MicoError::Io)?;
        Ok(())
    }
    
    /// Save roles to the roles file
    async fn save_roles(&self) -> Result<()> {
        let roles = self.roles.read().await;
        let content = serde_json::to_string_pretty(&*roles)
            .map_err(|e| MicoError::Internal(format!("Failed to serialize roles: {}", e)))?;
        fs::write(&self.roles_file_path, content).await.map_err(MicoError::Io)?;
        Ok(())
    }
    
    /// Add a new role
    pub async fn add_role(&self, role: RoleDefinition) -> Result<()> {
        let mut roles = self.roles.write().await;
        if roles.contains_key(&role.name) {
            return Err(MicoError::Internal(format!("Role '{}' already exists", role.name)));
        }
        roles.insert(role.name.clone(), role);
        drop(roles);
        
        self.save_roles().await
    }
    
    /// Get all permissions for a user
    async fn get_user_permissions(&self, user: &User) -> Result<HashSet<Permission>> {
        let roles = self.roles.read().await;
        let mut permissions = HashSet::new();
        
        // Add permissions from all roles the user has
        for role_name in &user.roles {
            if let Some(role) = roles.get(role_name) {
                // Add global permissions
                permissions.extend(&role.global_permissions);
            }
        }
        
        Ok(permissions)
    }
    
    /// Get all database-specific permissions for a user
    async fn get_user_db_permissions(
        &self,
        user: &User,
        db_name: &str,
    ) -> Result<HashSet<Permission>> {
        let roles = self.roles.read().await;
        let mut permissions = HashSet::new();
        
        // Add permissions from all roles the user has
        for role_name in &user.roles {
            if let Some(role) = roles.get(role_name) {
                // Add global permissions
                permissions.extend(&role.global_permissions);
                
                // Add database-specific permissions
                if let Some(db_perms) = role.database_permissions.get(db_name) {
                    permissions.extend(&db_perms.permissions);
                }
            }
        }
        
        Ok(permissions)
    }
    
    /// Get all table-specific permissions for a user
    async fn get_user_table_permissions(
        &self,
        user: &User,
        db_name: &str,
        table_name: &str,
    ) -> Result<HashSet<Permission>> {
        let roles = self.roles.read().await;
        let mut permissions = HashSet::new();
        
        // Add permissions from all roles the user has
        for role_name in &user.roles {
            if let Some(role) = roles.get(role_name) {
                // Add global permissions
                permissions.extend(&role.global_permissions);
                
                // Add database-specific permissions
                if let Some(db_perms) = role.database_permissions.get(db_name) {
                    permissions.extend(&db_perms.permissions);
                    
                    // Add table-specific permissions
                    if let Some(table_perms) = db_perms.tables.get(table_name) {
                        permissions.extend(table_perms);
                    }
                }
            }
        }
        
        Ok(permissions)
    }
}

#[async_trait]
impl Authenticator for AuthService {
    async fn authenticate_password(&self, username: &str, password: &str) -> Result<AuthResult> {
        let mut users = self.users.write().await;
        let user = users.get_mut(username).ok_or_else(|| {
            MicoError::Internal(format!("User '{}' not found", username))
        })?;
        
        if !user.is_active {
            return Err(MicoError::Internal(format!("User '{}' is inactive", username)));
        }
        
        // Verify the password
        match &user.credentials {
            Credentials::Password { password_hash } => {
                if !PasswordHasher::verify_password(password, password_hash)? {
                    return Err(MicoError::Internal("Invalid password".to_string()));
                }
            }
            _ => return Err(MicoError::Internal("User doesn't use password authentication".to_string())),
        }
        
        // Update last login time
        user.update_last_login();
        
        // Create an auth result with token if JWT is enabled
        let (token, token_expires_at) = if let Some(token_manager) = &self.token_manager {
            let (token, expiry) = token_manager.create_token(user)?;
            (Some(token), Some(expiry))
        } else {
            (None, None)
        };
        
        // Save user updates
        drop(users);
        self.save_users().await?;
        
        // Clone user for result
        let user_clone = users.get(username).cloned().unwrap();
        
        Ok(AuthResult {
            user: user_clone,
            token,
            token_expires_at,
        })
    }
    
    async fn authenticate_token(&self, token: &str) -> Result<AuthResult> {
        let token_manager = self.token_manager.as_ref().ok_or_else(|| {
            MicoError::Internal("JWT authentication is not enabled".to_string())
        })?;
        
        // Validate the token
        let claims = token_manager.validate_token(token)?;
        
        // Get the user
        let mut users = self.users.write().await;
        let user = users.get_mut(&claims.sub).ok_or_else(|| {
            MicoError::Internal(format!("User '{}' not found", claims.sub))
        })?;
        
        if !user.is_active {
            return Err(MicoError::Internal(format!("User '{}' is inactive", claims.sub)));
        }
        
        // Update last login time
        user.update_last_login();
        
        // Create a new token for future use
        let (new_token, token_expires_at) = token_manager.create_token(user)?;
        
        // Save user updates
        drop(users);
        self.save_users().await?;
        
        // Clone user for result
        let user_clone = users.get(&claims.sub).cloned().unwrap();
        
        Ok(AuthResult {
            user: user_clone,
            token: Some(new_token),
            token_expires_at: Some(token_expires_at),
        })
    }
    
    async fn authenticate_api_key(&self, key: &str) -> Result<AuthResult> {
        let mut users = self.users.write().await;
        
        // Find the user with this API key
        let user = users.values_mut().find_map(|user| {
            if let Credentials::ApiKey { key_hash } = &user.credentials {
                if PasswordHasher::verify_password(key, key_hash).unwrap_or(false) {
                    Some(user)
                } else {
                    None
                }
            } else {
                None
            }
        }).ok_or_else(|| {
            MicoError::Internal("Invalid API key".to_string())
        })?;
        
        if !user.is_active {
            return Err(MicoError::Internal(format!("User '{}' is inactive", user.username)));
        }
        
        // Update last login time
        user.update_last_login();
        
        // Create an auth result with token if JWT is enabled
        let (token, token_expires_at) = if let Some(token_manager) = &self.token_manager {
            let (token, expiry) = token_manager.create_token(user)?;
            (Some(token), Some(expiry))
        } else {
            (None, None)
        };
        
        // Save user updates
        let username = user.username.clone();
        drop(users);
        self.save_users().await?;
        
        // Clone user for result
        let users = self.users.read().await;
        let user_clone = users.get(&username).cloned().unwrap();
        
        Ok(AuthResult {
            user: user_clone,
            token,
            token_expires_at,
        })
    }
    
    async fn create_user(&self, user: User) -> Result<User> {
        let mut users = self.users.write().await;
        
        // Check if the username already exists
        if users.contains_key(&user.username) {
            return Err(MicoError::Internal(format!("User '{}' already exists", user.username)));
        }
        
        // Add the user
        let username = user.username.clone();
        users.insert(username.clone(), user);
        
        // Save users
        drop(users);
        self.save_users().await?;
        
        // Return the created user
        let users = self.users.read().await;
        Ok(users.get(&username).cloned().unwrap())
    }
    
    async fn get_user(&self, username: &str) -> Result<User> {
        let users = self.users.read().await;
        users.get(username).cloned().ok_or_else(|| {
            MicoError::Internal(format!("User '{}' not found", username))
        })
    }
    
    async fn check_permission(
        &self,
        user: &User,
        permission: Permission,
        resource_type: ResourceType,
        resource_name: &str,
    ) -> Result<bool> {
        // Admins always have permission
        if user.has_role("admin") {
            return Ok(true);
        }
        
        // Check permissions based on resource type
        match resource_type {
            ResourceType::Global => {
                // Check global permissions
                let permissions = self.get_user_permissions(user).await?;
                Ok(permissions.contains(&permission))
            }
            ResourceType::Database => {
                // Check database permissions
                let permissions = self.get_user_db_permissions(user, resource_name).await?;
                Ok(permissions.contains(&permission))
            }
            ResourceType::Table => {
                // Parse the resource name (format: "db_name/table_name")
                let parts: Vec<&str> = resource_name.split('/').collect();
                if parts.len() != 2 {
                    return Err(MicoError::Internal(format!(
                        "Invalid table resource name: {}. Expected format: 'db_name/table_name'",
                        resource_name
                    )));
                }
                
                let db_name = parts[0];
                let table_name = parts[1];
                
                // Check table permissions
                let permissions = self.get_user_table_permissions(user, db_name, table_name).await?;
                Ok(permissions.contains(&permission))
            }
        }
    }
}

// Create an instance of the AuthService for use throughout the application
lazy_static::lazy_static! {
    pub static ref AUTH_SERVICE: tokio::sync::OnceCell<Arc<AuthService>> = 
        tokio::sync::OnceCell::new();
}

/// Initialize the global authentication service
pub async fn init_auth_service(
    data_dir: impl AsRef<Path>,
    jwt_secret: Option<String>,
) -> Result<()> {
    let auth_service = AuthService::new(data_dir, jwt_secret).await?;
    AUTH_SERVICE.set(Arc::new(auth_service))
        .map_err(|_| MicoError::Internal("Auth service already initialized".to_string()))?;
    Ok(())
}

/// Get a reference to the global authentication service
pub fn get_auth_service() -> Result<Arc<AuthService>> {
    AUTH_SERVICE.get().cloned().ok_or_else(|| {
        MicoError::Internal("Auth service not initialized".to_string())
    })
}