# PostgreSQL migration

Purpose:
The purpose of this task is to migrate this codebase from sqlite3 to postgreSQL. I have already downloaded postgreSQL for Windows. My postgres password is Glaeken01.

## Migration Steps

### Step 1: Install Required Python Packages
```bash
pip install psycopg2-binary python-dotenv
```

### Step 2: Update Environment Configuration
Create/update `.env` file to include PostgreSQL connection details:
```
ALPHAVANTAGE_API_KEY=your_existing_key
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=Glaeken01
POSTGRES_DATABASE=fin_trade_craft
```

### Step 3: Create PostgreSQL Database
```sql
-- Connect to PostgreSQL as superuser and create database
CREATE DATABASE fin_trade_craft;
CREATE USER fin_trade_user WITH PASSWORD 'Glaeken01';
GRANT ALL PRIVILEGES ON DATABASE fin_trade_craft TO fin_trade_user;
```

### Step 4: Convert SQLite Schema to PostgreSQL
Key differences to address:
- `INTEGER PRIMARY KEY AUTOINCREMENT` → `SERIAL PRIMARY KEY`
- `TEXT` → `VARCHAR` or `TEXT` (both work in PostgreSQL)
- `REAL` → `NUMERIC` or `FLOAT`
- `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` → `TIMESTAMP DEFAULT NOW()`
- Remove `IF NOT EXISTS` (not standard SQL)

### Step 5: Create New DatabaseManager Class
- Replace `sqlite3` with `psycopg2`
- Update connection string format
- Modify SQL dialect differences
- Update schema initialization logic

### Step 6: Data Migration Strategy
Options:
1. **Fresh Start**: Run extractors again to populate PostgreSQL (recommended for clean data)
2. **Data Export/Import**: Export SQLite data and import to PostgreSQL
3. **Parallel Migration**: Run both databases temporarily

### Step 7: Update All Extractors
- Test each extractor with PostgreSQL
- Verify data integrity
- Update any SQLite-specific queries

### Step 8: Testing and Validation
- Compare data counts between SQLite and PostgreSQL
- Test all CRUD operations
- Verify foreign key constraints
- Test all extractors end-to-end

## Implementation Plan

### Phase 1: Infrastructure Setup
- [ ] Install PostgreSQL Python driver
- [ ] Create PostgreSQL database and user
- [ ] Update environment configuration

### Phase 2: Code Migration
- [ ] Create new PostgreSQL-compatible DatabaseManager
- [ ] Convert SQLite schema to PostgreSQL schema
- [ ] Update connection management

### Phase 3: Data Migration
- [ ] Choose migration strategy
- [ ] Migrate existing data or re-extract
- [ ] Verify data integrity

### Phase 4: Testing
- [ ] Test all extractors
- [ ] Validate data consistency
- [ ] Performance testing

### Phase 5: Deployment
- [ ] Update production configuration
- [ ] Create backup/restore procedures
- [ ] Update documentation

## Benefits of PostgreSQL Migration

1. **Better Performance**: Superior performance for large datasets
2. **ACID Compliance**: Better transaction integrity
3. **Concurrent Access**: Multiple users/processes can access safely
4. **Advanced Features**: Window functions, CTEs, better date/time handling
5. **Scalability**: Better handling of large data volumes
6. **Data Types**: Rich set of data types including JSON, arrays
7. **Production Ready**: Enterprise-grade database system

## Potential Challenges

1. **SQL Dialect Differences**: Some queries may need adjustment
2. **Connection Management**: Different connection parameters
3. **Schema Differences**: PostgreSQL is more strict about data types
4. **Performance Tuning**: May need to optimize for PostgreSQL
5. **Backup/Restore**: Different procedures than SQLite

## Next Immediate Steps

1. **Install psycopg2**: `pip install psycopg2-binary`
2. **Create PostgreSQL database**: Use pgAdmin or command line
3. **Create new DatabaseManager**: PostgreSQL-compatible version
4. **Convert schema**: Update SQL for PostgreSQL compatibility
5. **Test with sample data**: Verify basic functionality

Would you like me to proceed with implementing any of these steps?