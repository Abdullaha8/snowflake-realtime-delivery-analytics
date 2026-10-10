locals {
  database = "FOODDEL_${var.env}"
  prefix   = var.env == "DEV" ? "" : "${var.env}_"
  roles = {
    FOODDEL_INGEST    = "Kafka connector: insert into BRONZE only"
    FOODDEL_TRANSFORM = "Read BRONZE; build SILVER/GOLD/MARTS"
    FOODDEL_BI        = "Read governed data (MARTS_SECURE, GOLD)"
  }
  # which warehouse each functional role may use
  warehouse_usage = {
    FOODDEL_TRANSFORM = "WH_TRANSFORM"
    FOODDEL_BI        = "WH_BI"
  }
}

resource "snowflake_database" "this" {
  name                        = local.database
  comment                     = "Real-time food delivery analytics (Pune) - ${var.env}"
  data_retention_time_in_days = 1
}

resource "snowflake_schema" "this" {
  for_each = toset(var.schemas)
  database = snowflake_database.this.name
  name     = each.value
}

# Cost guardrails: one monitor per warehouse, notify -> suspend -> suspend immediately.
resource "snowflake_resource_monitor" "warehouse" {
  for_each                  = var.warehouses
  name                      = "RM_${local.prefix}${each.key}"
  credit_quota              = each.value
  frequency                 = "MONTHLY"
  start_timestamp           = var.monitor_start
  notify_triggers           = [80]
  suspend_trigger           = 100
  suspend_immediate_trigger = 110
}

resource "snowflake_warehouse" "this" {
  for_each            = var.warehouses
  name                = "${local.prefix}${each.key}"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = "true"
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.warehouse[each.key].name
  comment             = "fooddel ${var.env}"
}

resource "snowflake_account_role" "this" {
  for_each = local.roles
  name     = "${local.prefix}${each.key}"
  comment  = each.value
}

# Custom roles roll up to SYSADMIN, as in snowflake/02_rbac.sql.
resource "snowflake_grant_account_role" "to_sysadmin" {
  for_each         = snowflake_account_role.this
  role_name        = each.value.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each          = snowflake_account_role.this
  account_role_name = each.value.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.this.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each          = local.warehouse_usage
  account_role_name = snowflake_account_role.this[each.key].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.this[each.value].name
  }
}
