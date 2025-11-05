#!/usr/bin/env python3
"""
FHIR Validator

This script validates transformed FHIR resources against the FHIR specification.
It performs both schema validation and business rule validation to ensure data integrity
and compliance with healthcare data standards.

"""

import os
import json
import logging
import argparse
import re
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Set, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fhir_validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fhir_validator")

@dataclass
class ValidationResult:
    """Class for storing validation results."""
    resource_type: str
    resource_id: str
    is_valid: bool
    errors: List[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        """Initialize lists if None."""
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
    
    def add_error(self, message: str) -> None:
        """Add an error message and mark as invalid."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
    
    def __str__(self) -> str:
        """String representation of validation result."""
        if self.is_valid:
            if not self.warnings:
                return f"Valid {self.resource_type}/{self.resource_id}"
            else:
                return f"Valid {self.resource_type}/{self.resource_id} with warnings: {', '.join(self.warnings)}"
        else:
            return f"Invalid {self.resource_type}/{self.resource_id}: {', '.join(self.errors)}"


class FHIRValidator:
    """Validates FHIR resources against FHIR specification."""
    
    def __init__(self, 
                 fhir_dir: str = "./fhir_data",
                 output_dir: str = "./validation_results",
                 fail_fast: bool = False):
        """
        Initialize the FHIR validator.
        
        Args:
            fhir_dir: Directory containing FHIR data
            output_dir: Directory to save validation results
            fail_fast: Whether to stop validation after the first error
        """
        self.fhir_dir = fhir_dir
        self.output_dir = output_dir
        self.fail_fast = fail_fast
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
        
        # Load resource definitions
        self._load_resource_definitions()
    
    def _load_resource_definitions(self) -> None:
        """Load FHIR resource definitions for validation."""
        # Define required fields for each resource type
        self.required_fields = {
            "Patient": ["resourceType", "id"],
            "Encounter": ["resourceType", "id", "status", "subject"],
            "Observation": ["resourceType", "id", "status", "code", "subject"],
            "MedicationRequest": ["resourceType", "id", "status", "intent", "subject"]
        }
        
        # Define allowed values for enumerated fields
        self.value_sets = {
            "Encounter.status": ["planned", "arrived", "triaged", "in-progress", "onleave", "finished", "cancelled"],
            "Observation.status": ["registered", "preliminary", "final", "amended", "corrected", "cancelled", "entered-in-error", "unknown"],
            "MedicationRequest.status": ["active", "on-hold", "cancelled", "completed", "entered-in-error", "stopped", "draft", "unknown"],
            "MedicationRequest.intent": ["proposal", "plan", "order", "original-order", "reflex-order", "filler-order", "instance-order", "option"]
        }
        
        # Define regex patterns for common fields
        self.patterns = {
            "id": r"^[A-Za-z0-9\-\.]{1,64}$",
            "date": r"^([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1]))?)?$",
            "datetime": r"^([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1])(T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]{1,9})?(Z|(\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00)))?)?)?$",
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        }
    
    def _load_fhir_data(self, resource_type: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Load FHIR data from JSON files.
        
        Args:
            resource_type: Optional resource type to load
            
        Returns:
            Dictionary of FHIR resources by type
        """
        fhir_data = {}
        
        if resource_type:
            # Load a specific resource type
            file_path = os.path.join(self.fhir_dir, f"{resource_type}.json")
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    resources = json.load(f)
                    fhir_data[resource_type] = resources
                    logger.info(f"Loaded {len(resources)} {resource_type} resources from {file_path}")
            else:
                logger.warning(f"Resource file not found: {file_path}")
        else:
            # Load all resource types
            for resource_type in ["Patient", "Encounter", "Observation", "MedicationRequest"]:
                file_path = os.path.join(self.fhir_dir, f"{resource_type}.json")
                if os.path.exists(file_path):
                    with open(file_path, 'r') as f:
                        resources = json.load(f)
                        fhir_data[resource_type] = resources
                        logger.info(f"Loaded {len(resources)} {resource_type} resources from {file_path}")
            
            # Also check for bundle file
            bundle_path = os.path.join(self.fhir_dir, "bundle.json")
            if os.path.exists(bundle_path):
                with open(bundle_path, 'r') as f:
                    bundle = json.load(f)
                    
                    # Extract resources from bundle
                    if bundle.get("resourceType") == "Bundle" and "entry" in bundle:
                        for entry in bundle["entry"]:
                            if "resource" in entry:
                                resource = entry["resource"]
                                resource_type = resource.get("resourceType")
                                
                                if resource_type:
                                    if resource_type not in fhir_data:
                                        fhir_data[resource_type] = []
                                    fhir_data[resource_type].append(resource)
        
        return fhir_data
    
    def validate_resource(self, resource: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single FHIR resource.
        
        Args:
            resource: FHIR resource to validate
            
        Returns:
            ValidationResult object
        """
        # Check if resource has resourceType
        if "resourceType" not in resource:
            return ValidationResult(
                resource_type="Unknown",
                resource_id="Unknown",
                is_valid=False,
                errors=["Missing resourceType"]
            )
        
        resource_type = resource["resourceType"]
        resource_id = resource.get("id", "Unknown")
        
        # Create validation result
        result = ValidationResult(
            resource_type=resource_type,
            resource_id=resource_id,
            is_valid=True
        )
        
        # Check if resource type is supported
        if resource_type not in self.required_fields:
            result.add_warning(f"Unsupported resource type: {resource_type}")
            return result
        
        # Validate required fields
        for field in self.required_fields[resource_type]:
            if field not in resource:
                result.add_error(f"Missing required field: {field}")
        
        # Validate id format
        if "id" in resource and not re.match(self.patterns["id"], resource["id"]):
            result.add_error(f"Invalid id format: {resource['id']}")
        
        # Validate enumerated fields
        for field_path, allowed_values in self.value_sets.items():
            if field_path.startswith(f"{resource_type}."):
                # Extract field name after the resource type
                field_name = field_path.split(".", 1)[1]
                if field_name in resource and resource[field_name] not in allowed_values:
                    result.add_error(f"Invalid {field_name}: {resource[field_name]}. Must be one of: {', '.join(allowed_values)}")
        
        # Validate date/datetime fields
        self._validate_date_fields(resource, result)
        
        # Additional resource-specific validation
        if resource_type == "Patient":
            self._validate_patient(resource, result)
        elif resource_type == "Encounter":
            self._validate_encounter(resource, result)
        elif resource_type == "Observation":
            self._validate_observation(resource, result)
        elif resource_type == "MedicationRequest":
            self._validate_medication_request(resource, result)
        
        return result
    
    def _validate_date_fields(self, resource: Dict[str, Any], result: ValidationResult) -> None:
        """
        Validate date and datetime fields in a resource.
        
        Args:
            resource: FHIR resource
            result: ValidationResult to update
        """
        # Common date fields by resource type
        date_fields = {
            "Patient": ["birthDate"],
            "Encounter": ["period.start", "period.end"],
            "Observation": ["effectiveDateTime", "issued"],
            "MedicationRequest": ["authoredOn"]
        }
        
        resource_type = resource["resourceType"]
        
        # Check common date fields for this resource type
        if resource_type in date_fields:
            for field_path in date_fields[resource_type]:
                # Handle nested fields
                if "." in field_path:
                    parent, child = field_path.split(".", 1)
                    if parent in resource and isinstance(resource[parent], dict) and child in resource[parent]:
                        value = resource[parent][child]
                        if value and not re.match(self.patterns["date"], value) and not re.match(self.patterns["datetime"], value):
                            result.add_error(f"Invalid date format in {field_path}: {value}")
                else:
                    # Handle direct fields
                    if field_path in resource:
                        value = resource[field_path]
                        if value and not re.match(self.patterns["date"], value) and not re.match(self.patterns["datetime"], value):
                            result.add_error(f"Invalid date format in {field_path}: {value}")
    
    def _validate_patient(self, resource: Dict[str, Any], result: ValidationResult) -> None:
        """
        Validate Patient-specific fields.
        
        Args:
            resource: Patient resource
            result: ValidationResult to update
        """
        # Validate gender
        if "gender" in resource:
            gender = resource["gender"]
            if gender not in ["male", "female", "other", "unknown"]:
                result.add_error(f"Invalid gender: {gender}. Must be one of: male, female, other, unknown")
        
        # Validate telecom
        if "telecom" in resource and isinstance(resource["telecom"], list):
            for i, telecom in enumerate(resource["telecom"]):
                if "system" not in telecom:
                    result.add_error(f"Missing system in telecom[{i}]")
                elif telecom["system"] not in ["phone", "email", "fax", "pager", "url", "sms", "other"]:
                    result.add_error(f"Invalid telecom system in telecom[{i}]: {telecom['system']}")
                
                if "value" not in telecom:
                    result.add_error(f"Missing value in telecom[{i}]")
                elif telecom.get("system") == "email" and not re.match(self.patterns["email"], telecom["value"]):
                    result.add_error(f"Invalid email format in telecom[{i}]: {telecom['value']}")
    
    def _validate_encounter(self, resource: Dict[str, Any], result: ValidationResult) -> None:
        """
        Validate Encounter-specific fields.
        
        Args:
            resource: Encounter resource
            result: ValidationResult to update
        """
        # Validate subject reference
        if "subject" in resource:
            subject = resource["subject"]
            if not self._validate_reference(subject, "Patient"):
                result.add_error(f"Invalid subject reference: {subject}")
        
        # Validate period
        if "period" in resource:
            period = resource["period"]
            
            # Check start and end dates
            if "start" in period and "end" in period:
                start = period["start"]
                end = period["end"]
                
                # If both dates are present, make sure end is not before start
                try:
                    start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                    
                    if end_dt < start_dt:
                        result.add_error(f"End date {end} is before start date {start}")
                except (ValueError, TypeError):
                    # If dates can't be parsed, they should already be caught by date validation
                    pass
    
    def _validate_observation(self, resource: Dict[str, Any], result: ValidationResult) -> None:
        """
        Validate Observation-specific fields.
        
        Args:
            resource: Observation resource
            result: ValidationResult to update
        """
        # Validate subject reference
        if "subject" in resource:
            subject = resource["subject"]
            if not self._validate_reference(subject, "Patient"):
                result.add_error(f"Invalid subject reference: {subject}")
        
        # Validate code
        if "code" in resource:
            code = resource["code"]
            if "coding" in code and isinstance(code["coding"], list):
                for i, coding in enumerate(code["coding"]):
                    if "system" not in coding:
                        result.add_warning(f"Missing system in code.coding[{i}]")
                    if "code" not in coding:
                        result.add_warning(f"Missing code in code.coding[{i}]")
            elif "text" not in code:
                result.add_warning("Code should have either coding or text")
        
        # Validate value[x]
        value_present = False
        for key in resource:
            if key.startswith("value") and key != "value":
                value_present = True
                break
        
        if not value_present and "dataAbsentReason" not in resource and "component" not in resource:
            result.add_warning("Observation should have a value[x], dataAbsentReason, or component")
    
    def _validate_medication_request(self, resource: Dict[str, Any], result: ValidationResult) -> None:
        """
        Validate MedicationRequest-specific fields.
        
        Args:
            resource: MedicationRequest resource
            result: ValidationResult to update
        """
        # Validate subject reference
        if "subject" in resource:
            subject = resource["subject"]
            if not self._validate_reference(subject, "Patient"):
                result.add_error(f"Invalid subject reference: {subject}")
        
        # Check for either medicationCodeableConcept or medicationReference
        if "medicationCodeableConcept" not in resource and "medicationReference" not in resource:
            result.add_error("MedicationRequest must have either medicationCodeableConcept or medicationReference")
        
        # Validate dosageInstruction
        if "dosageInstruction" in resource:
            dosage = resource["dosageInstruction"]
            if not isinstance(dosage, list):
                result.add_error("dosageInstruction must be an array")
            elif len(dosage) == 0:
                result.add_warning("dosageInstruction is empty")
    
    def _validate_reference(self, reference: Dict[str, Any], expected_type: str) -> bool:
        """
        Validate a resource reference.
        
        Args:
            reference: Reference object
            expected_type: Expected resource type
            
        Returns:
            True if reference is valid, False otherwise
        """
        if not isinstance(reference, dict):
            return False
        
        if "reference" in reference:
            ref_str = reference["reference"]
            
            # Check if reference has the expected format (Type/ID)
            if "/" in ref_str:
                ref_type, ref_id = ref_str.split("/", 1)
                
                if ref_type != expected_type:
                    return False
                
                # Check if ID is valid format
                if not re.match(self.patterns["id"], ref_id):
                    return False
            
            return True
        
        # If no reference, but has display, it's a warning but still valid
        return "display" in reference
    
    def validate_all_resources(self) -> List[ValidationResult]:
        """
        Validate all FHIR resources.
        
        Returns:
            List of ValidationResult objects
        """
        logger.info("Starting validation of all FHIR resources")
        
        # Load FHIR data
        fhir_data = self._load_fhir_data()
        
        results = []
        error_count = 0
        warning_count = 0
        
        # Validate each resource
        for resource_type, resources in fhir_data.items():
            logger.info(f"Validating {len(resources)} {resource_type} resources")
            
            for resource in resources:
                result = self.validate_resource(resource)
                results.append(result)
                
                # Log result
                if not result.is_valid:
                    logger.warning(str(result))
                    error_count += len(result.errors)
                    
                    # Stop validation if fail_fast is enabled
                    if self.fail_fast:
                        logger.info("Stopping validation due to fail_fast setting")
                        break
                elif result.warnings:
                    logger.info(str(result))
                    warning_count += len(result.warnings)
            
            # Stop if fail_fast and there are errors
            if self.fail_fast and error_count > 0:
                break
        
        logger.info(f"Validation completed: {len(results)} resources validated, {error_count} errors, {warning_count} warnings")
        
        return results
    
    def cross_validate_resources(self, results: List[ValidationResult]) -> List[ValidationResult]:
        """
        Perform cross-validation between different resources.
        
        Args:
            results: List of ValidationResult objects from individual validation
            
        Returns:
            Updated list of ValidationResult objects
        """
        logger.info("Starting cross-validation between resources")
        
        # Load all FHIR data
        fhir_data = self._load_fhir_data()
        
        # Create lookup dictionaries for resources
        resource_map = {}
        for resource_type, resources in fhir_data.items():
            if resource_type not in resource_map:
                resource_map[resource_type] = {}
            
            for resource in resources:
                if "id" in resource:
                    resource_map[resource_type][resource["id"]] = resource
        
        # Check references between resources
        for resource_type, resources in fhir_data.items():
            logger.info(f"Cross-validating references in {len(resources)} {resource_type} resources")
            
            for resource in resources:
                resource_id = resource.get("id")
                if not resource_id:
                    continue
                
                # Find the corresponding validation result
                result = None
                for r in results:
                    if r.resource_type == resource_type and r.resource_id == resource_id:
                        result = r
                        break
                
                if not result:
                    # Create a new validation result if not found
                    result = ValidationResult(resource_type=resource_type, resource_id=resource_id, is_valid=True)
                    results.append(result)
                
                # Validate references based on resource type
                if resource_type == "Encounter":
                    self._cross_validate_encounter(resource, result, resource_map)
                elif resource_type == "Observation":
                    self._cross_validate_observation(resource, result, resource_map)
                elif resource_type == "MedicationRequest":
                    self._cross_validate_medication_request(resource, result, resource_map)
        
        logger.info("Cross-validation completed")
        return results
    
    def _cross_validate_encounter(self, resource: Dict[str, Any], result: ValidationResult, resource_map: Dict[str, Dict[str, Any]]) -> None:
        """
        Cross-validate Encounter references.
        
        Args:
            resource: Encounter resource
            result: ValidationResult to update
            resource_map: Map of all resources by type and ID
        """
        # Validate subject reference
        if "subject" in resource and "reference" in resource["subject"]:
            ref = resource["subject"]["reference"]
            if ref.startswith("Patient/"):
                patient_id = ref.split("/")[1]
                if "Patient" not in resource_map or patient_id not in resource_map["Patient"]:
                    result.add_error(f"Referenced Patient not found: {patient_id}")
    
    def _cross_validate_observation(self, resource: Dict[str, Any], result: ValidationResult, resource_map: Dict[str, Dict[str, Any]]) -> None:
        """
        Cross-validate Observation references.
        
        Args:
            resource: Observation resource
            result: ValidationResult to update
            resource_map: Map of all resources by type and ID
        """
        # Validate subject reference
        if "subject" in resource and "reference" in resource["subject"]:
            ref = resource["subject"]["reference"]
            if ref.startswith("Patient/"):
                patient_id = ref.split("/")[1]
                if "Patient" not in resource_map or patient_id not in resource_map["Patient"]:
                    result.add_error(f"Referenced Patient not found: {patient_id}")
        
        # Validate encounter reference
        if "encounter" in resource and "reference" in resource["encounter"]:
            ref = resource["encounter"]["reference"]
            if ref.startswith("Encounter/"):
                encounter_id = ref.split("/")[1]
                if "Encounter" not in resource_map or encounter_id not in resource_map["Encounter"]:
                    result.add_error(f"Referenced Encounter not found: {encounter_id}")
    
    def _cross_validate_medication_request(self, resource: Dict[str, Any], result: ValidationResult, resource_map: Dict[str, Dict[str, Any]]) -> None:
        """
        Cross-validate MedicationRequest references.
        
        Args:
            resource: MedicationRequest resource
            result: ValidationResult to update
            resource_map: Map of all resources by type and ID
        """
        # Validate subject reference
        if "subject" in resource and "reference" in resource["subject"]:
            ref = resource["subject"]["reference"]
            if ref.startswith("Patient/"):
                patient_id = ref.split("/")[1]
                if "Patient" not in resource_map or patient_id not in resource_map["Patient"]:
                    result.add_error(f"Referenced Patient not found: {patient_id}")
        
        # Validate encounter reference
        if "encounter" in resource and "reference" in resource["encounter"]:
            ref = resource["encounter"]["reference"]
            if ref.startswith("Encounter/"):
                encounter_id = ref.split("/")[1]
                if "Encounter" not in resource_map or encounter_id not in resource_map["Encounter"]:
                    result.add_error(f"Referenced Encounter not found: {encounter_id}")
    
    def save_validation_results(self, results: List[ValidationResult]) -> None:
        """
        Save validation results to JSON file.
        
        Args:
            results: List of ValidationResult objects
        """
        # Convert results to serializable format
        serializable_results = []
        for result in results:
            serializable_results.append({
                "resource_type": result.resource_type,
                "resource_id": result.resource_id,
                "is_valid": result.is_valid,
                "errors": result.errors,
                "warnings": result.warnings
            })
        
        # Calculate statistics
        total = len(results)
        valid = sum(1 for r in results if r.is_valid)
        invalid = total - valid
        with_warnings = sum(1 for r in results if r.is_valid and r.warnings)
        
        stats = {
            "total": total,
            "valid": valid,
            "invalid": invalid,
            "with_warnings": with_warnings,
            "validation_time": datetime.now().isoformat()
        }
        
        # Save results
        output = {
            "statistics": stats,
            "results": serializable_results
        }
        
        output_file = os.path.join(self.output_dir, "validation_results.json")
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        
        logger.info(f"Saved validation results to {output_file}")
        
        # Also save summary as readable text file
        summary_file = os.path.join(self.output_dir, "validation_summary.txt")
        with open(summary_file, 'w') as f:
            f.write(f"FHIR Validation Summary\n")
            f.write(f"======================\n\n")
            f.write(f"Validation Time: {stats['validation_time']}\n\n")
            f.write(f"Resources Validated: {stats['total']}\n")
            f.write(f"Valid Resources: {stats['valid']} ({stats['valid']/stats['total']*100:.1f}%)\n")
            f.write(f"Invalid Resources: {stats['invalid']} ({stats['invalid']/stats['total']*100:.1f}%)\n")
            f.write(f"Valid Resources with Warnings: {stats['with_warnings']} ({stats['with_warnings']/stats['valid']*100:.1f}% of valid)\n\n")
            
            # Group errors and warnings by resource type
            errors_by_type = {}
            warnings_by_type = {}
            
            for result in results:
                if not result.is_valid:
                    if result.resource_type not in errors_by_type:
                        errors_by_type[result.resource_type] = []
                    errors_by_type[result.resource_type].extend(result.errors)
                
                if result.warnings:
                    if result.resource_type not in warnings_by_type:
                        warnings_by_type[result.resource_type] = []
                    warnings_by_type[result.resource_type].extend(result.warnings)
            
            # Write error summary
            if errors_by_type:
                f.write(f"Common Errors:\n")
                for resource_type, errors in errors_by_type.items():
                    f.write(f"\n{resource_type}:\n")
                    error_counts = {}
                    for error in errors:
                        error_counts[error] = error_counts.get(error, 0) + 1
                    
                    for error, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
                        f.write(f"  - {error} ({count} occurrences)\n")
            
            # Write warning summary
            if warnings_by_type:
                f.write(f"\nCommon Warnings:\n")
                for resource_type, warnings in warnings_by_type.items():
                    f.write(f"\n{resource_type}:\n")
                    warning_counts = {}
                    for warning in warnings:
                        warning_counts[warning] = warning_counts.get(warning, 0) + 1
                    
                    for warning, count in sorted(warning_counts.items(), key=lambda x: x[1], reverse=True):
                        f.write(f"  - {warning} ({count} occurrences)\n")
        
        logger.info(f"Saved validation summary to {summary_file}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Validate FHIR resources')
    parser.add_argument('--fhir-dir', default='./fhir_data', help='Directory containing FHIR data')
    parser.add_argument('--output-dir', default='./validation_results', help='Directory to save validation results')
    parser.add_argument('--fail-fast', action='store_true', help='Stop validation after the first error')
    parser.add_argument('--cross-validate', action='store_true', help='Perform cross-validation between resources')
    
    args = parser.parse_args()
    
    try:
        # Initialize validator
        validator = FHIRValidator(
            fhir_dir=args.fhir_dir,
            output_dir=args.output_dir,
            fail_fast=args.fail_fast
        )
        
        # Validate resources
        results = validator.validate_all_resources()
        
        # Perform cross-validation if requested
        if args.cross_validate:
            results = validator.cross_validate_resources(results)
        
        # Save validation results
        validator.save_validation_results(results)
        
        # Determine exit code
        if all(result.is_valid for result in results):
            logger.info("All resources are valid")
            exit_code = 0
        else:
            logger.warning("Some resources are invalid")
            exit_code = 1
        
        return exit_code
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        return 2


if __name__ == "__main__":
    exit(main())