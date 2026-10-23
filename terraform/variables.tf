variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
  sensitive   = true
}

variable "resource_group_name" {
  description = "Name of the existing Azure Resource Group"
  type        = string
}

variable "databricks_workspace_name" {
  description = "Name of the existing Azure Databricks Workspace"
  type        = string
}