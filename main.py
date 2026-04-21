from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import (
    DataIngestionConfig,
    TrainingPipelineConfig,
    DataValidationConfig
)
import sys


if __name__ == "__main__":
    try:
        logging.info("Pipeline started")

        # Step 1: Pipeline config
        trainingpipelineconfig = TrainingPipelineConfig()

        # Step 2: Data ingestion
        dataingestionconfig = DataIngestionConfig(trainingpipelineconfig)
        data_ingestion = DataIngestion(dataingestionconfig)

        logging.info("Initiating data ingestion pipeline")
        dataingestionartifact = data_ingestion.initiate_data_ingestion()

        print("\nData Ingestion Completed Successfully")
        print(dataingestionartifact)

        # Step 3: Data validation
        data_validation_config = DataValidationConfig(trainingpipelineconfig)

        # ✅ FIXED HERE
        data_validation = DataValidation(
            dataingestionartifact,
            data_validation_config
        )

        logging.info("Initiating data validation pipeline")

        data_validation_artifact = data_validation.initiate_data_validation()

        print("\nData Validation Completed Successfully")
        print(data_validation_artifact)

        # Optional: stop pipeline if validation fails
        if not data_validation_artifact.validation_status:
            raise Exception("Data validation failed (drift detected). Stopping pipeline.")

        logging.info("Pipeline completed successfully")

    except Exception as e:
        logging.error(f"Error occurred in main pipeline: {e}")
        raise NetworkSecurityException(e, sys)