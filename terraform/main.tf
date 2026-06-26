# ==========================================
# PART 1: THE CONNECTIONS & PROVIDERS
# ==========================================
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {} # <─── This explicit block fixes the error!
}

# ==========================================
# PART 2: THE AZURE CLOUD RESOURCES
# ==========================================

# 1. THE RESOURCE GROUP
resource "azurerm_resource_group" "sustainability_resource" {
  name     = "rg-sustainability-platform"
  location = "westeurope"
}

# 2. THE ADLS GEN2 STORAGE ACCOUNT
resource "azurerm_storage_account" "storage" {
  name                     = "sustdata9512" 
  resource_group_name      = azurerm_resource_group.sustainability_resource.name     
  location                 = azurerm_resource_group.sustainability_resource.location 
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true 
}

# 3. THE DATA LAKE STORAGE CONTAINER
resource "azurerm_storage_data_lake_gen2_filesystem" "container" {
  name               = "raw-landing"
  storage_account_id = azurerm_storage_account.storage.id 
}

# 4. THE DATABRICKS WORKSPACE
resource "azurerm_databricks_workspace" "databricks" {
  name                = "dbw-sustainability-platform"
  sku                 = "premium" 
  resource_group_name = azurerm_resource_group.sustainability_resource.name
  location            = azurerm_resource_group.sustainability_resource.location
}
