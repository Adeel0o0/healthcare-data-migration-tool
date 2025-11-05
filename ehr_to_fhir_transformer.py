#!/usr/bin/env python3
"""
EHR to FHIR Transformer

This script transforms legacy EHR data into FHIR format. It implements mapping logic
for converting data structures to standardized FHIR resources including
Patient, Encounter, Observation, and MedicationRequest.

"""

import os
import json
import uuid
import logging
import argparse
import datetime
from typing import Dict, List, Any, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fhir_transformation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fhir_transformer")

class EHRtoFHIRTransformer:
    """Transforms legacy EHR data to FHIR format."""
    
    def __init__(self, 
                 input_dir: str = "./extracted_data", 
                 output_dir: str = "./fhir_data",
                 source_system: str = "unknown"):
        """
        Initialize the EHR to FHIR transformer.
        
        Args:
            input_dir: Directory containing extracted legacy EHR data
            output_dir: Directory to save transformed FHIR data
            source_system: Name of the source EHR system
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.source_system = source_system
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
        
        # Load reference data for mapping
        self._load_reference_data()
    
    def _load_reference_data(self) -> None:
        """Load reference data for mapping legacy codes to standard codes."""
        # Gender mapping (legacy code to FHIR code)
        self.gender_map = {
            "M": "male",
            "F": "female",
            "U": "unknown",
            "O": "other"
        }
        
        # Encounter status mapping
        self.encounter_status_map = {
            "completed": "finished",
            "in-progress": "in-progress",
            "cancelled": "cancelled",
            "entered-in-error": "entered-in-error",
            # Add more mappings as needed
        }
        
        # Medication status mapping
        self.medication_status_map = {
            "active": "active",
            "completed": "completed",
            "cancelled": "stopped",
            "on-hold": "on-hold",
            # Add more mappings as needed
        }
        
        # Add more reference data mappings as needed
    
    def _load_json_data(self, entity_type: str) -> List[Dict[str, Any]]:
        """
        Load data from JSON file.
        
        Args:
            entity_type: Type of entity to load (patients, encounters, etc.)
            
        Returns:
            List of entities loaded from JSON
        """
        file_path = os.path.join(self.input_dir, f"{entity_type}.json")
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                logger.info(f"Loaded {len(data)} {entity_type} from {file_path}")
                return data
        except Exception as e:
            logger.error(f"Error loading {entity_type} data: {str(e)}")
            raise
    
    def _format_date(self, date_str: Optional[str]) -> Optional[str]:
        """
        Format date strings to FHIR format.
        
        Args:
            date_str: Date string to format
            
        Returns:
            Formatted date string
        """
        if not date_str:
            return None
        
        # FHIR uses ISO 8601 format (YYYY-MM-DD)
        # Many legacy systems already use this format, so we may not need to change it
        # If a different format is used, conversion logic would go here
        return date_str
    
    def transform_patient(self, patient_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform legacy patient data to FHIR Patient resource.
        
        Args:
            patient_data: Legacy patient data
            
        Returns:
            FHIR Patient resource
        """
        # Generate a FHIR ID (you might want to use a deterministic method in practice)
        fhir_id = f"Patient-{patient_data.get('patient_id', str(uuid.uuid4()))}"
        
        # Map gender code
        gender_code = patient_data.get("gender", "U")
        fhir_gender = self.gender_map.get(gender_code, "unknown")
        
        # Transform patient to FHIR format
        fhir_patient = {
            "resourceType": "Patient",
            "id": fhir_id,
            "meta": {
                "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"]
            },
            "identifier": [
                {
                    "system": f"urn:oid:{self.source_system}",
                    "value": patient_data.get("patient_id")
                }
            ],
            "active": patient_data.get("active", True),
            "name": [
                {
                    "use": "official",
                    "family": patient_data.get("last_name", ""),
                    "given": [
                        patient_data.get("first_name", "")
                    ]
                }
            ],
            "gender": fhir_gender,
            "birthDate": self._format_date(patient_data.get("birth_date")),
            "deceasedBoolean": patient_data.get("deceased", False)
        }
        
        # Add middle name if present
        middle_name = patient_data.get("middle_name")
        if middle_name:
            fhir_patient["name"][0]["given"].append(middle_name)
        
        # Add MRN if present
        mrn = patient_data.get("mrn")
        if mrn:
            fhir_patient["identifier"].append({
                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "MR",
                            "display": "Medical Record Number"
                        }
                    ]
                },
                "value": mrn
            })
        
        # Add address if present
        if "address" in patient_data:
            addr = patient_data["address"]
            fhir_address = {
                "use": "home",
                "line": []
            }
            
            if addr.get("line1"):
                fhir_address["line"].append(addr["line1"])
            if addr.get("line2"):
                fhir_address["line"].append(addr["line2"])
            if addr.get("city"):
                fhir_address["city"] = addr["city"]
            if addr.get("state_code"):
                fhir_address["state"] = addr["state_code"]
            elif addr.get("state"):
                fhir_address["state"] = addr["state"]
            if addr.get("postal_code"):
                fhir_address["postalCode"] = addr["postal_code"]
            if addr.get("country"):
                fhir_address["country"] = addr["country"]
            
            fhir_patient["address"] = [fhir_address]
        
        # Add telecom if present
        telecom = []
        if "contact" in patient_data:
            if patient_data["contact"].get("phone"):
                telecom.append({
                    "system": "phone",
                    "value": patient_data["contact"]["phone"],
                    "use": "home"
                })
            if patient_data["contact"].get("email"):
                telecom.append({
                    "system": "email",
                    "value": patient_data["contact"]["email"]
                })
        
        if telecom:
            fhir_patient["telecom"] = telecom
        
        # Add preferred language if present
        language = patient_data.get("preferred_language")
        if language:
            fhir_patient["communication"] = [
                {
                    "language": {
                        "coding": [
                            {
                                "system": "urn:ietf:bcp:47",
                                "code": self._map_language_code(language),
                                "display": language
                            }
                        ],
                        "text": language
                    },
                    "preferred": True
                }
            ]
        
        return fhir_patient
    
    def _map_language_code(self, language: str) -> str:
        """
        Map language name to BCP-47 language code.
        
        Args:
            language: Language name
            
        Returns:
            BCP-47 language code
        """
        language_map = {
            "English": "en",
            "Spanish": "es",
            "French": "fr",
            "German": "de",
            "Chinese": "zh",
            "Japanese": "ja",
            "Korean": "ko",
            "Russian": "ru",
            "Arabic": "ar",
            "Hindi": "hi",
            "Portuguese": "pt"
        }
        
        return language_map.get(language, "en")
    
    def transform_encounter(self, encounter_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform legacy encounter data to FHIR Encounter resource.
        
        Args:
            encounter_data: Legacy encounter data
            
        Returns:
            FHIR Encounter resource
        """
        # Generate a FHIR ID
        fhir_id = f"Encounter-{encounter_data.get('encounter_id', str(uuid.uuid4()))}"
        
        # Map encounter status
        status = encounter_data.get("status", "unknown")
        fhir_status = self.encounter_status_map.get(status, "unknown")
        
        # Transform encounter to FHIR format
        fhir_encounter = {
            "resourceType": "Encounter",
            "id": fhir_id,
            "meta": {
                "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter"]
            },
            "identifier": [
                {
                    "system": f"urn:oid:{self.source_system}",
                    "value": encounter_data.get("encounter_id")
                }
            ],
            "status": fhir_status,
            "class": {
                "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                "code": self._map_encounter_type_to_class(encounter_data.get("type", "AMB")),
                "display": encounter_data.get("type", "Ambulatory")
            },
            "type": [
                {
                    "text": encounter_data.get("type", "Encounter")
                }
            ],
            "subject": {
                "reference": f"Patient/{encounter_data.get('patient_id')}"
            },
            "period": {
                "start": self._format_date(encounter_data.get("encounter_date"))
            }
        }
        
        # Add end date if present
        if encounter_data.get("discharge_date"):
            fhir_encounter["period"]["end"] = self._format_date(encounter_data["discharge_date"])
        
        # Add diagnoses if present
        if encounter_data.get("diagnoses"):
            fhir_encounter["diagnosis"] = []
            for i, diagnosis in enumerate(encounter_data["diagnoses"]):
                fhir_diagnosis = {
                    "condition": {
                        "display": diagnosis.get("diagnosis", "Unknown")
                    },
                    "rank": i + 1
                }
                
                # Add coding if code is available
                if diagnosis.get("code"):
                    coding_system = "http://hl7.org/fhir/sid/icd-10-cm"
                    if diagnosis.get("type") == "ICD-9":
                        coding_system = "http://hl7.org/fhir/sid/icd-9-cm"
                    
                    fhir_diagnosis["condition"]["coding"] = [
                        {
                            "system": coding_system,
                            "code": diagnosis["code"],
                            "display": diagnosis.get("diagnosis", "Unknown")
                        }
                    ]
                
                fhir_encounter["diagnosis"].append(fhir_diagnosis)
        
        # Add location if present
        if encounter_data.get("location"):
            fhir_encounter["location"] = [
                {
                    "location": {
                        "display": encounter_data["location"]
                    }
                }
            ]
        
        # Add provider if present
        if encounter_data.get("provider"):
            provider = encounter_data["provider"]
            fhir_encounter["participant"] = [
                {
                    "type": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                    "code": "PPRF",
                                    "display": "Primary Performer"
                                }
                            ]
                        }
                    ],
                    "individual": {
                        "display": provider.get("name", "Unknown Provider")
                    }
                }
            ]
        
        # Add reason for visit if present
        if encounter_data.get("chief_complaint"):
            fhir_encounter["reasonCode"] = [
                {
                    "text": encounter_data["chief_complaint"]
                }
            ]
        
        return fhir_encounter
    
    def _map_encounter_type_to_class(self, encounter_type: str) -> str:
        """
        Map encounter type to FHIR class code.
        
        Args:
            encounter_type: Encounter type
            
        Returns:
            FHIR class code
        """
        # Map common encounter types to FHIR ActCode
        encounter_class_map = {
            "Office Visit": "AMB",
            "Outpatient": "AMB",
            "Ambulatory": "AMB",
            "Hospital Encounter": "IMP",
            "Inpatient": "IMP",
            "Emergency": "EMER",
            "Surgery": "SS",
            "Telehealth": "VR",
            "Virtual": "VR",
            "Home Visit": "HH",
            "Nursing Home": "NONAC",
            "Skilled Nursing": "NONAC",
            "Urgent Care": "AMB"
        }
        
        return encounter_class_map.get(encounter_type, "AMB")
    
    def transform_observation(self, observation_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform legacy observation data to FHIR Observation resources.
        
        Args:
            observation_data: Legacy observation data
            
        Returns:
            List of FHIR Observation resources
        """
        fhir_observations = []
        
        # Each component in the results array becomes a separate FHIR Observation
        if "results" in observation_data and observation_data["results"]:
            for i, result in enumerate(observation_data["results"]):
                # Generate a FHIR ID
                fhir_id = f"Observation-{observation_data.get('observation_id', str(uuid.uuid4()))}-{i}"
                
                # Transform observation to FHIR format
                fhir_observation = {
                    "resourceType": "Observation",
                    "id": fhir_id,
                    "meta": {
                        "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab"]
                    },
                    "identifier": [
                        {
                            "system": f"urn:oid:{self.source_system}",
                            "value": f"{observation_data.get('observation_id')}-{i}"
                        }
                    ],
                    "status": self._map_observation_status(observation_data.get("status", "final")),
                    "category": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                    "code": "laboratory",
                                    "display": "Laboratory"
                                }
                            ]
                        }
                    ],
                    "code": {
                        "text": result.get("component", observation_data.get("test_name", "Unknown Test"))
                    },
                    "subject": {
                        "reference": f"Patient/{observation_data.get('patient_id')}"
                    },
                    "encounter": {
                        "reference": f"Encounter/{observation_data.get('encounter_id')}"
                    },
                    "effectiveDateTime": self._format_date(observation_data.get("observation_date"))
                }
                
                # Add test code if available
                if observation_data.get("test_code"):
                    fhir_observation["code"]["coding"] = [
                        {
                            "system": "http://loinc.org",
                            "code": observation_data["test_code"],
                            "display": observation_data.get("test_name", "Unknown Test")
                        }
                    ]
                
                # Add value based on result
                if "value" in result:
                    # Try to convert to numeric if possible
                    try:
                        numeric_value = float(result["value"])
                        fhir_observation["valueQuantity"] = {
                            "value": numeric_value,
                            "unit": result.get("unit", ""),
                            "system": "http://unitsofmeasure.org",
                            "code": result.get("unit", "")
                        }
                    except (ValueError, TypeError):
                        # If not numeric, use string
                        fhir_observation["valueString"] = result["value"]
                
                # Add interpretation if available
                if "status" in result:
                    interpretation_code = "N"  # Normal
                    if result["status"] == "high":
                        interpretation_code = "H"
                    elif result["status"] == "low":
                        interpretation_code = "L"
                    elif result["status"] == "abnormal":
                        interpretation_code = "A"
                    
                    fhir_observation["interpretation"] = [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
                                    "code": interpretation_code,
                                    "display": result["status"].capitalize()
                                }
                            ]
                        }
                    ]
                
                # Add reference range if available
                if "reference_range" in result:
                    fhir_observation["referenceRange"] = [
                        {
                            "text": result["reference_range"]
                        }
                    ]
                
                # Add performer if available
                if observation_data.get("performer"):
                    fhir_observation["performer"] = [
                        {
                            "display": observation_data["performer"]
                        }
                    ]
                
                fhir_observations.append(fhir_observation)
        else:
            # If no results array, create a single observation
            fhir_id = f"Observation-{observation_data.get('observation_id', str(uuid.uuid4()))}"
            
            fhir_observation = {
                "resourceType": "Observation",
                "id": fhir_id,
                "meta": {
                    "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab"]
                },
                "identifier": [
                    {
                        "system": f"urn:oid:{self.source_system}",
                        "value": observation_data.get('observation_id')
                    }
                ],
                "status": self._map_observation_status(observation_data.get("status", "final")),
                "category": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                "code": "laboratory",
                                "display": "Laboratory"
                            }
                        ]
                    }
                ],
                "code": {
                    "text": observation_data.get("test_name", "Unknown Test")
                },
                "subject": {
                    "reference": f"Patient/{observation_data.get('patient_id')}"
                },
                "effectiveDateTime": self._format_date(observation_data.get("observation_date"))
            }
            
            # Add encounter if available
            if observation_data.get("encounter_id"):
                fhir_observation["encounter"] = {
                    "reference": f"Encounter/{observation_data['encounter_id']}"
                }
            
            # Add test code if available
            if observation_data.get("test_code"):
                fhir_observation["code"]["coding"] = [
                    {
                        "system": "http://loinc.org",
                        "code": observation_data["test_code"],
                        "display": observation_data.get("test_name", "Unknown Test")
                    }
                ]
            
            fhir_observations.append(fhir_observation)
        
        return fhir_observations
    
    def _map_observation_status(self, status: str) -> str:
        """
        Map observation status to FHIR status.
        
        Args:
            status: Observation status
            
        Returns:
            FHIR observation status
        """
        status_map = {
            "final": "final",
            "preliminary": "preliminary",
            "corrected": "corrected",
            "cancelled": "cancelled",
            "entered-in-error": "entered-in-error"
        }
        
        return status_map.get(status, "unknown")
    
    def transform_medication(self, medication_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform legacy medication data to FHIR MedicationRequest resource.
        
        Args:
            medication_data: Legacy medication data
            
        Returns:
            FHIR MedicationRequest resource
        """
        # Generate a FHIR ID
        fhir_id = f"MedicationRequest-{medication_data.get('medication_id', str(uuid.uuid4()))}"
        
        # Map medication status
        status = medication_data.get("status", "active")
        fhir_status = self.medication_status_map.get(status, "active")
        
        # Transform medication to FHIR format
        fhir_medication = {
            "resourceType": "MedicationRequest",
            "id": fhir_id,
            "meta": {
                "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-medicationrequest"]
            },
            "identifier": [
                {
                    "system": f"urn:oid:{self.source_system}",
                    "value": medication_data.get("medication_id")
                }
            ],
            "status": fhir_status,
            "intent": "order",
            "medicationCodeableConcept": {
                "text": medication_data.get("medication_name", "Unknown Medication")
            },
            "subject": {
                "reference": f"Patient/{medication_data.get('patient_id')}"
            },
            "authoredOn": self._format_date(medication_data.get("prescription_date"))
        }
        
        # Add encounter if available
        if medication_data.get("encounter_id"):
            fhir_medication["encounter"] = {
                "reference": f"Encounter/{medication_data['encounter_id']}"
            }
        
        # Add dosage instructions
        dosage_instruction = {
            "text": f"{medication_data.get('dose', '')} {medication_data.get('route', '')} {medication_data.get('frequency', '')}"
        }
        
        # Add route if available
        if medication_data.get("route"):
            dosage_instruction["route"] = {
                "text": medication_data["route"]
            }
        
        # Add dose if available
        if medication_data.get("dose"):
            dosage_instruction["doseAndRate"] = [
                {
                    "text": medication_data["dose"]
                }
            ]
        
        fhir_medication["dosageInstruction"] = [dosage_instruction]
        
        # Add prescriber if available
        if medication_data.get("prescriber"):
            fhir_medication["requester"] = {
                "display": medication_data["prescriber"]
            }
        
        # Add dispense request (refills, duration)
        if medication_data.get("refills") is not None or medication_data.get("duration_days"):
            dispense_request = {}
            
            if medication_data.get("refills") is not None:
                dispense_request["numberOfRepeatsAllowed"] = medication_data["refills"]
            
            if medication_data.get("duration_days"):
                dispense_request["expectedSupplyDuration"] = {
                    "value": medication_data["duration_days"],
                    "unit": "day",
                    "system": "http://unitsofmeasure.org",
                    "code": "d"
                }
            
            fhir_medication["dispenseRequest"] = dispense_request
        
        return fhir_medication
    
    def transform_all_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform all legacy EHR data to FHIR format.
        
        Returns:
            Dictionary containing all transformed FHIR resources
        """
        logger.info("Starting transformation of all data to FHIR format")
        
        # Load legacy data
        patients = self._load_json_data("patients")
        encounters = self._load_json_data("encounters")
        observations = self._load_json_data("observations")
        medications = self._load_json_data("medications")
        
        # Transform data
        fhir_patients = []
        for patient in patients:
            fhir_patient = self.transform_patient(patient)
            fhir_patients.append(fhir_patient)
        
        fhir_encounters = []
        for encounter in encounters:
            fhir_encounter = self.transform_encounter(encounter)
            fhir_encounters.append(fhir_encounter)
        
        fhir_observations = []
        for observation in observations:
            # Each observation may generate multiple FHIR observations
            obs_resources = self.transform_observation(observation)
            fhir_observations.extend(obs_resources)
        
        fhir_medications = []
        for medication in medications:
            fhir_medication = self.transform_medication(medication)
            fhir_medications.append(fhir_medication)
        
        # Build complete FHIR dataset
        fhir_data = {
            "Patient": fhir_patients,
            "Encounter": fhir_encounters,
            "Observation": fhir_observations,
            "MedicationRequest": fhir_medications
        }
        
        logger.info(f"Transformed {len(fhir_patients)} patients, {len(fhir_encounters)} encounters, "
                  f"{len(fhir_observations)} observations, and {len(fhir_medications)} medication requests")
        
        return fhir_data
    
    def save_fhir_data(self, fhir_data: Dict[str, List[Dict[str, Any]]]) -> None:
        """
        Save transformed FHIR data to JSON files.
        
        Args:
            fhir_data: The FHIR data to save
        """
        # Save each resource type to a separate file
        for resource_type, resources in fhir_data.items():
            filename = os.path.join(self.output_dir, f"{resource_type}.json")
            with open(filename, 'w') as f:
                json.dump(resources, f, indent=2)
            logger.info(f"Saved {len(resources)} {resource_type} resources to {filename}")
        
        # Create a FHIR Bundle containing all resources
        bundle = self._create_fhir_bundle(fhir_data)
        
        # Save Bundle
        bundle_file = os.path.join(self.output_dir, "bundle.json")
        with open(bundle_file, 'w') as f:
            json.dump(bundle, f, indent=2)
        
        logger.info(f"Saved FHIR Bundle to {bundle_file}")
    
    def _create_fhir_bundle(self, fhir_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Create a FHIR Bundle containing all resources.
        
        Args:
            fhir_data: Dictionary of FHIR resources by type
            
        Returns:
            FHIR Bundle resource
        """
        entries = []
        
        # Add all resources to the bundle
        for resource_type, resources in fhir_data.items():
            for resource in resources:
                entries.append({
                    "fullUrl": f"{resource_type}/{resource.get('id', '')}",
                    "resource": resource
                })
        
        # Create Bundle resource
        bundle = {
            "resourceType": "Bundle",
            "id": f"bundle-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
            "type": "transaction",
            "entry": entries
        }
        
        return bundle


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Transform legacy EHR data to FHIR format')
    parser.add_argument('--input-dir', default='./extracted_data', help='Directory containing extracted legacy EHR data')
    parser.add_argument('--output-dir', default='./fhir_data', help='Directory to save transformed FHIR data')
    parser.add_argument('--source-system', default='legacy_ehr', help='Name of the source EHR system')
    
    args = parser.parse_args()
    
    try:
        # Initialize transformer
        transformer = EHRtoFHIRTransformer(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            source_system=args.source_system
        )
        
        # Transform data
        fhir_data = transformer.transform_all_data()
        
        # Save transformed data
        transformer.save_fhir_data(fhir_data)
        
        logger.info("Transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()