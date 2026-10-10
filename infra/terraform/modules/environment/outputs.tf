output "database" {
  value = snowflake_database.this.name
}

output "warehouses" {
  value = [for w in snowflake_warehouse.this : w.name]
}
