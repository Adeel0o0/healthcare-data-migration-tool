#!/usr/bin/env python3
"""
Legacy EHR Data Extractor

This script connects to a legacy EHR (Electronic Health Record) system, extracts patient data, and prepares it for 
transformation to FHIR format. It handles pagination, batch processing, and error recovery
to ensure reliable extraction of large datasets.

"""

import os
import json
import time
import logging
import argparse
import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Iterator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ehr_extraction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ehr_extractor")

class EHRExtractor:
    """Extracts data from legacy EHR systems."""
    
    def __init__(self, 
                 source_system: str,
                 data_dir: str = "./legacy_ehr_data", 
                 output_dir: str = "./extracted_data",
                 batch_size: int = 100,
                 retries: int = 3,
                 retry_delay: float = 2.0):
        """
        Initialize the EHR extractor.
        
        Args:
            source_system: Name of the source EHR system
            data_dir: Directory containing legacy EHR data (for file-based extraction)
            output_dir: Directory to save extracted data
            batch_size: Number of records to process in each batch
            retries: Number of retry attempts for failed operations
            retry_delay: Delay between retry attempts (in seconds)
        """
        self.source_system = source_system
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.retries = retries
        self.retry_delay = retry_delay
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
        
        # Initialize connection to the appropriate EHR system
        self._initialize_connection()
    
    def _initialize_connection(self) -> None:
        """Initialize connection to the legacy EHR system."""
        logger.info(f"Initializing connection to {self.source_system} EHR system")
        
        # For file-based extraction (our synthetic data)
        if self.source_system.lower() in ["file", "synthetic", "test", "local"]:
            self.extraction_method = "file"
            
            # Check if data directory exists
            if not os.path.exists(self.data_dir):
                logger.error(f"Data directory not found: {self.data_dir}")
                raise FileNotFoundError(f"Data directory not found: {self.data_dir}")
            
            # Check for required data files
            required_files = ["patients.json", "encounters.json", "observations.json", "medications.json"]
            missing_files = []
            
            for file in required_files:
                file_path = os.path.join(self.data_dir, file)
                if not os.path.exists(file_path):
                    missing_files.append(file)
            
            if missing_files:
                logger.error(f"Missing data files: {', '.join(missing_files)}")
                raise FileNotFoundError(f"Missing data files: {', '.join(missing_files)}")
            
            logger.info(f"Successfully connected to file-based EHR data in {self.data_dir}")
            return
            
        # For API-based extraction (for real systems)
        # Add support for different EHR systems here
        if self.source_system.lower() == "epic":
            self.extraction_method = "epic_api"
            # In a real implementation, we would initialize API connection here
            # self.api_client = EpicApiClient(...)
            logger.info("EPIC API connection would be initialized here")
            
        elif self.source_system.lower() == "cerner":
            self.extraction_method = "cerner_api"
            # self.api_client = CernerApiClient(...)
            logger.info("Cerner API connection would be initialized here")
            
        elif self.source_system.lower() == "allscripts":
            self.extraction_method = "allscripts_api"
            # self.api_client = AllscriptsApiClient(...)
            logger.info("Allscripts API connection would be initialized here")
            
        else:
            logger.error(f"Unsupported EHR system: {self.source_system}")
            raise ValueError(f"Unsupported EHR system: {self.source_system}")
    
    def _load_json_data(self, entity_type: str) -> List[Dict[str, Any]]:
        """
        Load data from JSON file for file-based extraction.
        
        Args:
            entity_type: Type of entity to load (patients, encounters, etc.)
            
        Returns:
            List of entities loaded from JSON
        """
        file_path = os.path.join(self.data_dir, f"{entity_type}.json")
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                logger.info(f"Loaded {len(data)} {entity_type} from {file_path}")
                return data
        except Exception as e:
            logger.error(f"Error loading {entity_type} data: {str(e)}")
            raise
    
    def extract_patients(self, 
                         start_date: Optional[str] = None, 
                         end_date: Optional[str] = None,
                         patient_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Extract patient data from the legacy EHR system.
        
        Args:
            start_date: Optional start date filter (YYYY-MM-DD)
            end_date: Optional end date filter (YYYY-MM-DD)
            patient_ids: Optional list of specific patient IDs to extract
            
        Returns:
            List of extracted patients
        """
        logger.info(f"Extracting patients from {self.source_system}")
        
        if start_date:
            logger.info(f"Filtering patients from {start_date}")
        if end_date:
            logger.info(f"Filtering patients to {end_date}")
        if patient_ids:
            logger.info(f"Extracting {len(patient_ids)} specific patients")
        
        # For file-based extraction
        if self.extraction_method == "file":
            patients = self._load_json_data("patients")
            
            # Apply filters
            filtered_patients = []
            for patient in patients:
                # Apply patient ID filter
                if patient_ids and patient.get("patient_id") not in patient_ids:
                    continue
                
                # Apply date filters (using registration_date)
                if start_date or end_date:
                    reg_date = patient.get("registration_date")
                    if not reg_date:
                        continue
                    
                    if start_date and reg_date < start_date:
                        continue
                    if end_date and reg_date > end_date:
                        continue
                
                filtered_patients.append(patient)
            
            logger.info(f"Extracted {len(filtered_patients)} patients after filtering")
            return filtered_patients
            
        # For API-based extraction
        elif self.extraction_method == "epic_api":
            # This would be implemented for a real system
            # In a real implementation, we would use pagination and error handling
            logger.info("EPIC API extraction would happen here")
            return []
        
        else:
            logger.error(f"Extraction method not implemented: {self.extraction_method}")
            return []
    
    def extract_encounters(self, 
                          patient_ids: Optional[List[str]] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract encounter data from the legacy EHR system.
        
        Args:
            patient_ids: Optional list of specific patient IDs to extract encounters for
            start_date: Optional start date filter (YYYY-MM-DD)
            end_date: Optional end date filter (YYYY-MM-DD)
            
        Returns:
            List of extracted encounters
        """
        logger.info(f"Extracting encounters from {self.source_system}")
        
        if patient_ids:
            logger.info(f"Filtering encounters for {len(patient_ids)} patients")
        if start_date:
            logger.info(f"Filtering encounters from {start_date}")
        if end_date:
            logger.info(f"Filtering encounters to {end_date}")
        
        # For file-based extraction
        if self.extraction_method == "file":
            encounters = self._load_json_data("encounters")
            
            # Apply filters
            filtered_encounters = []
            for encounter in encounters:
                # Apply patient ID filter
                if patient_ids and encounter.get("patient_id") not in patient_ids:
                    continue
                
                # Apply date filters
                if start_date or end_date:
                    enc_date = encounter.get("encounter_date")
                    if not enc_date:
                        continue
                    
                    if start_date and enc_date < start_date:
                        continue
                    if end_date and enc_date > end_date:
                        continue
                
                filtered_encounters.append(encounter)
            
            logger.info(f"Extracted {len(filtered_encounters)} encounters after filtering")
            return filtered_encounters
            
        # For API-based extraction
        elif self.extraction_method == "epic_api":
            # This would be implemented for a real system
            logger.info("EPIC API extraction would happen here")
            return []
        
        else:
            logger.error(f"Extraction method not implemented: {self.extraction_method}")
            return []
    
    def extract_observations(self,
                           patient_ids: Optional[List[str]] = None,
                           encounter_ids: Optional[List[str]] = None,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract observation data from the legacy EHR system.
        
        Args:
            patient_ids: Optional list of specific patient IDs
            encounter_ids: Optional list of specific encounter IDs
            start_date: Optional start date filter (YYYY-MM-DD)
            end_date: Optional end date filter (YYYY-MM-DD)
            
        Returns:
            List of extracted observations
        """
        logger.info(f"Extracting observations from {self.source_system}")
        
        # For file-based extraction
        if self.extraction_method == "file":
            observations = self._load_json_data("observations")
            
            # Apply filters
            filtered_observations = []
            for observation in observations:
                # Apply patient ID filter
                if patient_ids and observation.get("patient_id") not in patient_ids:
                    continue
                
                # Apply encounter ID filter
                if encounter_ids and observation.get("encounter_id") not in encounter_ids:
                    continue
                
                # Apply date filters
                if start_date or end_date:
                    obs_date = observation.get("observation_date")
                    if not obs_date:
                        continue
                    
                    if start_date and obs_date < start_date:
                        continue
                    if end_date and obs_date > end_date:
                        continue
                
                filtered_observations.append(observation)
            
            logger.info(f"Extracted {len(filtered_observations)} observations after filtering")
            return filtered_observations
            
        # For API-based extraction
        elif self.extraction_method == "epic_api":
            # This would be implemented for a real system
            logger.info("EPIC API extraction would happen here")
            return []
        
        else:
            logger.error(f"Extraction method not implemented: {self.extraction_method}")
            return []
    
    def extract_medications(self,
                          patient_ids: Optional[List[str]] = None,
                          encounter_ids: Optional[List[str]] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract medication data from the legacy EHR system.
        
        Args:
            patient_ids: Optional list of specific patient IDs
            encounter_ids: Optional list of specific encounter IDs
            start_date: Optional start date filter (YYYY-MM-DD)
            end_date: Optional end date filter (YYYY-MM-DD)
            
        Returns:
            List of extracted medications
        """
        logger.info(f"Extracting medications from {self.source_system}")
        
        # For file-based extraction
        if self.extraction_method == "file":
            medications = self._load_json_data("medications")
            
            # Apply filters
            filtered_medications = []
            for medication in medications:
                # Apply patient ID filter
                if patient_ids and medication.get("patient_id") not in patient_ids:
                    continue
                
                # Apply encounter ID filter
                if encounter_ids and medication.get("encounter_id") not in encounter_ids:
                    continue
                
                # Apply date filters
                if start_date or end_date:
                    med_date = medication.get("prescription_date")
                    if not med_date:
                        continue
                    
                    if start_date and med_date < start_date:
                        continue
                    if end_date and med_date > end_date:
                        continue
                
                filtered_medications.append(medication)
            
            logger.info(f"Extracted {len(filtered_medications)} medications after filtering")
            return filtered_medications
            
        # For API-based extraction
        elif self.extraction_method == "epic_api":
            # This would be implemented for a real system
            logger.info("EPIC API extraction would happen here")
            return []
        
        else:
            logger.error(f"Extraction method not implemented: {self.extraction_method}")
            return []
    
    def batch_extract_patients(self,
                             start_date: Optional[str] = None,
                             end_date: Optional[str] = None) -> Iterator[List[Dict[str, Any]]]:
        """
        Extract patients in batches to handle large datasets.
        
        Args:
            start_date: Optional start date filter (YYYY-MM-DD)
            end_date: Optional end date filter (YYYY-MM-DD)
            
        Yields:
            Batches of patient records
        """
        if self.extraction_method == "file":
            # Load all patients first
            all_patients = self.extract_patients(start_date=start_date, end_date=end_date)
            
            # Process in batches
            total_patients = len(all_patients)
            for i in range(0, total_patients, self.batch_size):
                batch = all_patients[i:i + self.batch_size]
                logger.info(f"Processing batch {i//self.batch_size + 1} with {len(batch)} patients")
                yield batch
        
        else:
            # For API-based systems, this would use pagination
            # This is a placeholder implementation
            logger.warning("Batch extraction not fully implemented for this extraction method")
            yield []
    
    def extract_patient_with_related_data(self, patient_id: str) -> Dict[str, Any]:
        """
        Extract a complete patient record including related encounters, observations,
        and medications.
        
        Args:
            patient_id: ID of the patient to extract
            
        Returns:
            Dictionary containing complete patient data
        """
        logger.info(f"Extracting complete patient record for {patient_id}")
        
        # Extract patient data
        patients = self.extract_patients(patient_ids=[patient_id])
        if not patients:
            logger.warning(f"Patient not found: {patient_id}")
            return {"patient": None, "encounters": [], "observations": [], "medications": []}
        
        patient = patients[0]
        
        # Extract related encounters
        encounters = self.extract_encounters(patient_ids=[patient_id])
        
        # Extract related observations and medications
        if encounters:
            encounter_ids = [enc["encounter_id"] for enc in encounters]
            observations = self.extract_observations(patient_ids=[patient_id], encounter_ids=encounter_ids)
            medications = self.extract_medications(patient_ids=[patient_id], encounter_ids=encounter_ids)
        else:
            observations = []
            medications = []
        
        # Build complete patient record
        patient_record = {
            "patient": patient,
            "encounters": encounters,
            "observations": observations,
            "medications": medications
        }
        
        logger.info(f"Extracted patient record with {len(encounters)} encounters, "
                  f"{len(observations)} observations, and {len(medications)} medications")
        
        return patient_record
    
    def extract_all_data(self, 
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract all data from the legacy EHR system.
        
        Args:
            start_date: Optional start date filter (YYYY-MM-DD)
            end_date: Optional end date filter (YYYY-MM-DD)
            
        Returns:
            Dictionary containing all extracted data
        """
        logger.info(f"Starting extraction of all data from {self.source_system}")
        
        # Extract patients
        patients = self.extract_patients(start_date=start_date, end_date=end_date)
        
        if not patients:
            logger.warning("No patients found matching criteria")
            return {"patients": [], "encounters": [], "observations": [], "medications": []}
        
        # Extract patient IDs
        patient_ids = [p["patient_id"] for p in patients]
        
        # Extract related data
        encounters = self.extract_encounters(patient_ids=patient_ids, start_date=start_date, end_date=end_date)
        
        if encounters:
            encounter_ids = [e["encounter_id"] for e in encounters]
            observations = self.extract_observations(
                patient_ids=patient_ids,
                encounter_ids=encounter_ids,
                start_date=start_date,
                end_date=end_date
            )
            medications = self.extract_medications(
                patient_ids=patient_ids,
                encounter_ids=encounter_ids,
                start_date=start_date,
                end_date=end_date
            )
        else:
            observations = []
            medications = []
        
        # Build complete dataset
        extracted_data = {
            "patients": patients,
            "encounters": encounters,
            "observations": observations,
            "medications": medications
        }
        
        logger.info(f"Extracted {len(patients)} patients, {len(encounters)} encounters, "
                  f"{len(observations)} observations, and {len(medications)} medications")
        
        return extracted_data
    
    def save_extracted_data(self, data: Dict[str, Any]) -> None:
        """
        Save extracted data to JSON files.
        
        Args:
            data: The data to save
        """
        # Save each entity type to a separate file
        for entity_type, entities in data.items():
            filename = os.path.join(self.output_dir, f"{entity_type}.json")
            with open(filename, 'w') as f:
                json.dump(entities, f, indent=2)
            logger.info(f"Saved {len(entities)} {entity_type} to {filename}")
        
        # Save metadata about the extraction
        metadata = {
            "extraction_time": datetime.datetime.now().isoformat(),
            "source_system": self.source_system,
            "entity_counts": {
                entity_type: len(entities) for entity_type, entities in data.items()
            }
        }
        
        metadata_file = os.path.join(self.output_dir, "extraction_metadata.json")
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Saved extraction metadata to {metadata_file}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Extract data from legacy EHR system')
    parser.add_argument('--source', required=True, help='Source EHR system (e.g., file, epic, cerner)')
    parser.add_argument('--data-dir', default='./legacy_ehr_data', help='Directory containing legacy EHR data (for file-based extraction)')
    parser.add_argument('--output-dir', default='./extracted_data', help='Directory to save extracted data')
    parser.add_argument('--batch-size', type=int, default=100, help='Number of records to process in each batch')
    parser.add_argument('--start-date', help='Start date filter (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date filter (YYYY-MM-DD)')
    parser.add_argument('--patient', help='Extract data for a specific patient ID')
    
    args = parser.parse_args()
    
    try:
        # Initialize extractor
        extractor = EHRExtractor(
            source_system=args.source,
            data_dir=args.data_dir,
            output_dir=args.output_dir,
            batch_size=args.batch_size
        )
        
        # Extract data
        if args.patient:
            # Extract single patient with related data
            data = extractor.extract_patient_with_related_data(args.patient)
            # Restructure for consistent saving
            restructured_data = {
                "patients": [data["patient"]] if data["patient"] else [],
                "encounters": data["encounters"],
                "observations": data["observations"],
                "medications": data["medications"]
            }
            extractor.save_extracted_data(restructured_data)
        else:
            # Extract all data with optional date filters
            data = extractor.extract_all_data(
                start_date=args.start_date,
                end_date=args.end_date
            )
            extractor.save_extracted_data(data)
        
        logger.info("Extraction completed successfully")
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()