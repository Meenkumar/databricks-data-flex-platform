# Database Migrations

This directory contains versioned database migration scripts.

## Naming Convention

Migrations should be named with the following pattern:
```
V{version}__{description}.sql
```

Example:
```
V001__initial_schema.sql
V002__add_member_indexes.sql
V003__create_audit_tables.sql
```

## Migration Process

1. Create a new migration file with the next version number
2. Test the migration in dev environment
3. Review and approve the migration
4. Apply to higher environments using deployment pipeline

## Best Practices

- Make migrations idempotent when possible (use IF NOT EXISTS)
- Include rollback scripts for complex changes
- Test migrations with representative data volumes
- Document breaking changes clearly
- Never modify existing migration files after deployment
