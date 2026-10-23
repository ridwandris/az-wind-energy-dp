output "storage_account_name" {
  description = "The generated ADLS Gen2 storage account name"
  value       = azurerm_storage_account.adls.name
}

output "container_name" {
  description = "The landing container name"
  value       = azurerm_storage_data_lake_gen2_filesystem.landing.name
}