// src/client/query_builder.rs

//! Query builder for the MicoDB client.

use crate::client::error::{ClientError, Result};
use crate::client::pool::ConnectionPool;
use crate::client::types::QueryResult;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::fmt;
use std::sync::Arc;

/// Filter condition for building WHERE clauses.
#[derive(Debug, Clone)]
pub enum FilterCondition {
    /// Equal to.
    Eq(String, String),
    /// Not equal to.
    Ne(String, String),
    /// Less than.
    Lt(String, String),
    /// Less than or equal to.
    Le(String, String),
    /// Greater than.
    Gt(String, String),
    /// Greater than or equal to.
    Ge(String, String),
    /// NULL check.
    IsNull(String),
    /// NOT NULL check.
    IsNotNull(String),
    /// LIKE pattern match.
    Like(String, String),
    /// AND combinator.
    And(Box<FilterCondition>, Box<FilterCondition>),
    /// OR combinator.
    Or(Box<FilterCondition>, Box<FilterCondition>),
    /// NOT.
    Not(Box<FilterCondition>),
    /// Raw SQL condition.
    Raw(String),
}

impl FilterCondition {
    /// Convert the condition to SQL.
    fn to_sql(&self) -> String {
        match self {
            FilterCondition::Eq(col, value) => format!("{} = {}", col, value),
            FilterCondition::Ne(col, value) => format!("{} != {}", col, value),
            FilterCondition::Lt(col, value) => format!("{} < {}", col, value),
            FilterCondition::Le(col, value) => format!("{} <= {}", col, value),
            FilterCondition::Gt(col, value) => format!("{} > {}", col, value),
            FilterCondition::Ge(col, value) => format!("{} >= {}", col, value),
            FilterCondition::IsNull(col) => format!("{} IS NULL", col),
            FilterCondition::IsNotNull(col) => format!("{} IS NOT NULL", col),
            FilterCondition::Like(col, pattern) => format!("{} LIKE {}", col, pattern),
            FilterCondition::And(left, right) => format!("({} AND {})", left.to_sql(), right.to_sql()),
            FilterCondition::Or(left, right) => format!("({} OR {})", left.to_sql(), right.to_sql()),
            FilterCondition::Not(cond) => format!("NOT ({})", cond.to_sql()),
            FilterCondition::Raw(sql) => sql.clone(),
        }
    }
}

/// Order direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

impl fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// Join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Inner join.
    Inner,
    /// Left join.
    Left,
    /// Right join.
    Right,
    /// Full join.
    Full,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER JOIN"),
            JoinType::Left => write!(f, "LEFT JOIN"),
            JoinType::Right => write!(f, "RIGHT JOIN"),
            JoinType::Full => write!(f, "FULL JOIN"),
        }
    }
}

/// A builder for SELECT queries.
pub struct SelectBuilder {
    /// Database name.
    db_name: String,
    /// Table name.
    table: String,
    /// Columns to select.
    columns: Vec<String>,
    /// WHERE conditions.
    conditions: Option<FilterCondition>,
    /// GROUP BY columns.
    group_by: Vec<String>,
    /// HAVING conditions.
    having: Option<FilterCondition>,
    /// ORDER BY clauses.
    order_by: Vec<(String, OrderDirection)>,
    /// LIMIT clause.
    limit: Option<usize>,
    /// OFFSET clause.
    offset: Option<usize>,
    /// Joins.
    joins: Vec<(String, JoinType, FilterCondition)>,
    /// Connection pool for execution.
    pool: Arc<ConnectionPool>,
}

impl SelectBuilder {
    /// Create a new select builder.
    fn new(db_name: String, table: String, pool: Arc<ConnectionPool>) -> Self {
        Self {
            db_name,
            table,
            columns: vec!["*".to_string()],
            conditions: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            joins: Vec::new(),
            pool,
        }
    }
    
    /// Select specific columns.
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }
    
    /// Add a WHERE condition.
    pub fn where_clause(mut self, condition: FilterCondition) -> Self {
        self.conditions = Some(condition);
        self
    }
    
    /// Add a GROUP BY clause.
    pub fn group_by(mut self, columns: &[&str]) -> Self {
        self.group_by = columns.iter().map(|s| s.to_string()).collect();
        self
    }
    
    /// Add a HAVING clause.
    pub fn having(mut self, condition: FilterCondition) -> Self {
        self.having = Some(condition);
        self
    }
    
    /// Add an ORDER BY clause.
    pub fn order_by(mut self, column: &str, direction: OrderDirection) -> Self {
        self.order_by.push((column.to_string(), direction));
        self
    }
    
    /// Add a LIMIT clause.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    
    /// Add an OFFSET clause.
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
    
    /// Add a JOIN clause.
    pub fn join(
        mut self,
        table: &str,
        join_type: JoinType,
        condition: FilterCondition,
    ) -> Self {
        self.joins.push((table.to_string(), join_type, condition));
        self
    }
    
    /// Build the SQL query.
    pub fn build(&self) -> String {
        let mut sql = format!("SELECT {} FROM {}", 
            self.columns.join(", "),
            self.table
        );
        
        // Add JOINs
        for (table, join_type, condition) in &self.joins {
            sql.push_str(&format!(" {} {} ON {}", 
                join_type,
                table,
                condition.to_sql()
            ));
        }
        
        // Add WHERE clause
        if let Some(ref condition) = self.conditions {
            sql.push_str(&format!(" WHERE {}", condition.to_sql()));
        }
        
        // Add GROUP BY clause
        if !self.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by.join(", ")));
        }
        
        // Add HAVING clause
        if let Some(ref condition) = self.having {
            sql.push_str(&format!(" HAVING {}", condition.to_sql()));
        }
        
        // Add ORDER BY clause
        if !self.order_by.is_empty() {
            let order_clauses: Vec<String> = self.order_by
                .iter()
                .map(|(col, dir)| format!("{} {}", col, dir))
                .collect();
            sql.push_str(&format!(" ORDER BY {}", order_clauses.join(", ")));
        }
        
        // Add LIMIT clause
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        
        // Add OFFSET clause
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }
        
        sql
    }
    
    /// Execute the query.
    pub async fn execute(&self) -> Result<Vec<RecordBatch>> {
        let sql = self.build();
        let mut conn = self.pool.get().await?;
        conn.execute_sql(&self.db_name, &sql).await
    }
}

/// Query builder for MicoDB.
pub struct QueryBuilder {
    /// Connection pool.
    pool: Arc<ConnectionPool>,
}

impl QueryBuilder {
    /// Create a new query builder.
    pub(crate) fn new(pool: Arc<ConnectionPool>) -> Self {
        Self { pool }
    }
    
    /// Start building a SELECT query.
    pub fn select_from(&self, db_name: &str, table: &str) -> SelectBuilder {
        SelectBuilder::new(db_name.to_string(), table.to_string(), Arc::clone(&self.pool))
    }
}