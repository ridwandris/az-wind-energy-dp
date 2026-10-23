provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

data "azurerm_resource_group" "rg" {
  name = var.resource_group_name
}

data "azurerm_databricks_workspace" "dbx" {
  name                = var.databricks_workspace_name
  resource_group_name = data.azurerm_resource_group.rg.name
}


# Globally unique storage name
resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

# ADLS Gen2 Storage Account
resource "azurerm_storage_account" "adls" {
  name                     = "stwinddata${random_string.suffix.result}"
  resource_group_name      = data.azurerm_resource_group.rg.name
  location                 = data.azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Enables hierarchical namespace for ADLS Gen2
}

# Storage container for landing our wind data
resource "azurerm_storage_data_lake_gen2_filesystem" "landing" {
  name               = "wind-platform"
  storage_account_id = azurerm_storage_account.adls.id
}

# Access Connector for Managed Identity
resource "azurerm_databricks_access_connector" "unity_connector" {
  name                = "dbx-access-connector"
  resource_group_name = data.azurerm_resource_group.rg.name
  location            = data.azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }
}

# Connector permission to read/write to the Storage Account
resource "azurerm_role_assignment" "storage_data_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity_connector.identity[0].principal_id
}