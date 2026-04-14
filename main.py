from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig
import sys


if __name__ == "__main__":
    try:
        # Step 1: Create pipeline config
        trainingpipelineconfig = TrainingPipelineConfig()

        # Step 2: Create data ingestion config
        dataingestionconfig = DataIngestionConfig(trainingpipelineconfig)

        # Step 3: Initialize ingestion
        data_ingestion = DataIngestion(dataingestionconfig)

        logging.info("Initiating data ingestion pipeline")

        # Step 4: Run ingestion
        dataingestionartifact = data_ingestion.initiate_data_ingestion()

        print("\nData Ingestion Completed Successfully")
        print(dataingestionartifact)

        logging.info("Data ingestion pipeline completed successfully")

    except Exception as e:
        logging.error("Error occurred in main pipeline")
        raise NetworkSecurityException(e, sys)