variable "env" {
  type        = string
  description = "Environment suffix: DEV, QA, PROD -> database FOODDEL_<env>"
}

variable "schemas" {
  type        = list(string)
  description = "Schemas in the environment's database"
  default     = ["BRONZE", "SILVER", "GOLD", "STAGING", "MARTS", "MARTS_SECURE", "ML", "OPS", "GOVERNANCE", "APPS"]
}

variable "warehouses" {
  type        = map(number)
  description = "Warehouse name -> monthly credit quota of its resource monitor"
  default     = { WH_INGEST = 5, WH_TRANSFORM = 5, WH_BI = 5 }
}

variable "account_credit_quota" {
  type        = number
  description = "Monthly credit quota for the environment-wide resource monitor"
  default     = 20
}

variable "monitor_start" {
  type        = string
  description = "Resource monitor start ('YYYY-MM-DD HH:MI'); the provider requires it with a frequency"
  default     = "2026-11-01 00:00"
}
