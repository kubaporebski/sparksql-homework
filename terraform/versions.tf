terraform {
	required_providers {
		google = {
			source = "hashicorp/google"
			version = "3.78.0"
		}
		
		random = {
			source = "hashicorp/random"
			version = "3.1.0"
		}
        
        databricks = {
          source  = "databrickslabs/databricks"
          version = "0.3.9"
        }
	}
}