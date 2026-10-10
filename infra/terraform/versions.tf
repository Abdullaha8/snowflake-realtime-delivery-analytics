terraform {
  required_version = ">= 1.9.0"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "2.21.0" # pinned; released 2026-09-10
    }
  }
}
