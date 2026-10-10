# One module call per environment. Same naming and cost guardrails as DEV (snowflake/01-02).
module "prod" {
  source = "./modules/environment"

  env                  = "PROD"
  account_credit_quota = 50
}
