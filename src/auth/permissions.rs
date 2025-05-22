// src/auth/permissions.rs

//! Permission and role management for MicoDB.

use std::collections::HashSet;
use serde::{Serialize, Deserialize};
use bitflags::bitflags;

/// Resource type for permissions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResourceType {
    /// Global resources
    Global,
    /// Database-level resources
    Database,
    /// Table-level resources
    Table,
}

bitflags! {
    /// Permission flags for MicoDB operations
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Permission: u32 {
        /// Read access (SELECT)
        const Read              = 0b0000_0000_0000_0001;
        /// Write access (INSERT/UPDATE/DELETE)
        const Write             = 0b0000_0000_0000_0010;
        /// Create tables
        const CreateTable       = 0b0000_0000_0000_0100;
        /// Drop tables
        const DropTable         = 0b0000_0000_0000_1000;
        /// Create databases
        const CreateDatabase    = 0b0000_0000_0001_0000;
        /// Drop databases
        const DropDatabase      = 0b0000_0000_0010_0000;
        /// View table schema
        const ReadSchema        = 0b0000_0000_0100_0000;
        /// Modify table schema
        const ModifySchema      = 0b0000_0000_1000_0000;
        /// Execute SQL queries
        const ExecuteQuery      = 0b0000_0001_0000_0000;
        /// Begin transactions
        const BeginTransaction  = 0b0000_0010_0000_0000;
        /// Commit transactions
        const CommitTransaction = 0b0000_0100_0000_0000;
        /// Rollback transactions
        const RollbackTransaction = 0b0000_1000_0000_0000;
        /// Manage users (admin only)
        const ManageUsers       = 0b0001_0000_0000_0000;
        /// Manage roles (admin only)
        const ManageRoles       = 0b0010_0000_0000_0000;
        /// View system information (admin only)
        const ViewSystem        = 0b0100_0000_0000_0000;
        /// Manage system settings (admin only)
        const ManageSystem      = 0b1000_0000_0000_0000;
    }
}

// Custom serialization for Permission to make it compatible with serde
impl Serialize for Permission {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bits = self.bits();
        serializer.serialize_u32(bits)
    }
}

// Custom deserialization for Permission to make it compatible with serde
impl<'de> Deserialize<'de> for Permission {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bits = u32::deserialize(deserializer)?;
        Ok(Permission::from_bits_truncate(bits))
    }
}

/// A collection of permissions that define a role
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Role {
    /// Role name
    pub name: String,
    /// Permissions granted by this role
    pub permissions: HashSet<Permission>,
}

impl Permission {
    /// Create a set with all permissions
    pub fn all() -> HashSet<Self> {
        let mut perms = HashSet::new();
        perms.insert(Permission::Read);
        perms.insert(Permission::Write);
        perms.insert(Permission::CreateTable);
        perms.insert(Permission::DropTable);
        perms.insert(Permission::CreateDatabase);
        perms.insert(Permission::DropDatabase);
        perms.insert(Permission::ReadSchema);
        perms.insert(Permission::ModifySchema);
        perms.insert(Permission::ExecuteQuery);
        perms.insert(Permission::BeginTransaction);
        perms.insert(Permission::CommitTransaction);
        perms.insert(Permission::RollbackTransaction);
        perms.insert(Permission::ManageUsers);
        perms.insert(Permission::ManageRoles);
        perms.insert(Permission::ViewSystem);
        perms.insert(Permission::ManageSystem);
        perms
    }

    /// Create a set with read-only permissions
    pub fn readonly() -> HashSet<Self> {
        let mut perms = HashSet::new();
        perms.insert(Permission::Read);
        perms.insert(Permission::ReadSchema);
        perms.insert(Permission::ExecuteQuery);
        perms
    }

    /// Create a set with standard user permissions (read + write)
    pub fn standard_user() -> HashSet<Self> {
        let mut perms = HashSet::new();
        perms.insert(Permission::Read);
        perms.insert(Permission::Write);
        perms.insert(Permission::ReadSchema);
        perms.insert(Permission::ExecuteQuery);
        perms.insert(Permission::BeginTransaction);
        perms.insert(Permission::CommitTransaction);
        perms.insert(Permission::RollbackTransaction);
        perms
    }

    /// Create a set with power user permissions (standard + schema changes)
    pub fn power_user() -> HashSet<Self> {
        let mut perms = Self::standard_user();
        perms.insert(Permission::CreateTable);
        perms.insert(Permission::CreateDatabase);
        perms.insert(Permission::ModifySchema);
        perms
    }
}