# Key-pair auth for a deployment user (in CI: secrets, never in git). Nothing here is applied
# from this repo by default - DEV was built with the idempotent SQL in snowflake/; this
# configuration is the promotion path for further environments (e.g. PROD).
provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = var.user
  role              = "ACCOUNTADMIN"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = var.private_key_path == "" ? null : file(var.private_key_path)
}
