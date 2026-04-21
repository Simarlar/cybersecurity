from dataclasses import dataclass
import os
import sys
import pandas as pd
from scipy.stats import ks_2samp

from networksecurity.entity.artifact_entity import (
    DataIngestionArtifact,
    DataValidationArtifact
)
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:
    def __init__(
        self,
        data_ingestion_artifact: DataIngestionArtifact,
        data_validation_config: DataValidationConfig
    ):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)

            logging.info("DataValidation initialized successfully")

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    # -----------------------------
    # Utility Methods
    # -----------------------------
    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        try:
            logging.info(f"Reading data from: {file_path}")
            df = pd.read_csv(file_path)
            logging.info(f"Loaded dataframe with shape: {df.shape}")
            return df
        except Exception as e:
            raise NetworkSecurityException(f"Failed to read file: {file_path}", sys)

    # -----------------------------
    # Validation Methods
    # -----------------------------
    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            expected_columns = len(self._schema_config)
            actual_columns = len(dataframe.columns)

            logging.info(f"Expected columns: {expected_columns}")
            logging.info(f"Actual columns: {actual_columns}")

            if actual_columns != expected_columns:
                logging.error("Column count mismatch detected")
                return False

            return True

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(
        self,
        base_df: pd.DataFrame,
        current_df: pd.DataFrame,
        threshold: float = 0.05
    ) -> (bool, dict):
        """
        Returns:
            drift_status (bool): True if no drift, False if drift detected
            report (dict): column-wise drift report
        """
        try:
            logging.info("Starting dataset drift detection")

            drift_detected = False
            report = {}

            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]

                test_result = ks_2samp(d1, d2)

                drift = test_result.pvalue < threshold

                if drift:
                    drift_detected = True

                report[column] = {
                    "p_value": float(test_result.pvalue),
                    "drift_detected": drift
                }

            # Save report
            drift_report_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_path), exist_ok=True)

            write_yaml_file(drift_report_path, report)

            logging.info(f"Drift report saved at: {drift_report_path}")

            return (not drift_detected), report

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    # -----------------------------
    # Main Pipeline Method
    # -----------------------------
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            logging.info("Starting data validation pipeline")

            train_path = self.data_ingestion_artifact.trained_file_path
            test_path = self.data_ingestion_artifact.test_file_path

            # Step 1: Load data
            train_df = self.read_data(train_path)
            test_df = self.read_data(test_path)

            error_messages = []

            # Step 2: Validate schema
            if not self.validate_number_of_columns(train_df):
                error_messages.append("Train dataset column mismatch")

            if not self.validate_number_of_columns(test_df):
                error_messages.append("Test dataset column mismatch")

            if error_messages:
                error_msg = " | ".join(error_messages)
                logging.error(error_msg)
                raise NetworkSecurityException(error_msg, sys)

            # Step 3: Drift detection
            validation_status, drift_report = self.detect_dataset_drift(
                base_df=train_df,
                current_df=test_df
            )

            # Step 4: Save validated datasets
            os.makedirs(
                os.path.dirname(self.data_validation_config.valid_train_file_path),
                exist_ok=True
            )

            train_df.to_csv(
                self.data_validation_config.valid_train_file_path,
                index=False
            )

            test_df.to_csv(
                self.data_validation_config.valid_test_file_path,
                index=False
            )

            logging.info("Validated datasets saved successfully")

            # Step 5: Create artifact
            artifact = DataValidationArtifact(
                validation_status=validation_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            logging.info("Data validation completed successfully")

            return artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)