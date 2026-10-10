variable "organization_name" {
  type        = string
  description = "Snowflake organization (first part of <org>-<account>)"
}

variable "account_name" {
  type        = string
  description = "Snowflake account name (second part of <org>-<account>)"
}

variable "user" {
  type        = string
  description = "Deployment user with key-pair auth"
}

variable "private_key_path" {
  type        = string
  description = "Path to the deployment user's PKCS8 private key"
  default     = ""
}
