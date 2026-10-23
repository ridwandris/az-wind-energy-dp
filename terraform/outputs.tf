output "storage_account_name" {
  description = "The generated ADLS Gen2 storage account name"
  value       = azurerm_storage_account.adls.name
}

output "container_name" {
  description = "The landing container name"
  value       = azurerm_storage_data_lake_gen2_filesystem.landing.name
}

output "access_connector_id" {
  description = "The Resource ID of the Databricks Access Connector"
  value       = azurerm_databricks_access_connector.unity_connector.id
}