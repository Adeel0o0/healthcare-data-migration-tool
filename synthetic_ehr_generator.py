#!/usr/bin/env python3
"""
Synthetic EHR Data Generator

This script generates realistic mock data for a legacy Electronic Health Record (EHR) system.
It creates sample patients, encounters, observations, and medications that mimic the structure
of real healthcare data while being completely made up.
"""

import os
import json
import random
import datetime
import argparse
import uuid
from typing import Dict, List, Any, Optional


class SyntheticEHRGenerator:
    """Generate synthetic EHR data for testing and development purposes."""
    
    def __init__(self, output_dir: str = "./legacy_ehr_data", seed: Optional[int] = None):
        """
        Initialize the synthetic data generator.
        
        Args:
            output_dir: Directory to save generated data
            seed: Random seed for reproducibility
        """
        self.output_dir = output_dir
        
        # Set random seed for reproducibility if provided
        if seed is not None:
            random.seed(seed)
            
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")
            
        # Load reference data for realistic generation
        self._load_reference_data()
    
    def _load_reference_data(self):
        """Load reference data for realistic data generation."""
        # Common first names
        self.first_names = [
            "James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles",
            "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen",
            "Christopher", "Daniel", "Matthew", "Anthony", "Mark", "Donald", "Steven", "Paul", "Andrew", "Joshua",
            "Michelle", "Amanda", "Melissa", "Stephanie", "Rebecca", "Laura", "Sharon", "Cynthia", "Kathleen", "Amy"
        ]
        
        # Common last names
        self.last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
            "Hernandez", "Lopez", "Gonzales", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
            "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
            "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green"
        ]
        
        # Common street names
        self.streets = [
            "Main Street", "Oak Street", "Maple Avenue", "Washington Street", "Park Avenue", "Elm Street",
            "Lake Drive", "Pine Street", "Cedar Avenue", "Hillside Drive", "Forest Avenue", "River Road",
            "Highland Avenue", "Meadow Lane", "Valley View Drive", "Sunset Drive", "Church Street",
            "Willow Street", "Ridge Road", "Woodland Drive", "Broadway"
        ]
        
        # Common cities
        self.cities = [
            "Springfield", "Franklin", "Greenville", "Bristol", "Clinton", "Kingston", "Marion", "Salem",
            "Georgetown", "Oakland", "Ashland", "Burlington", "Manchester", "Milton", "Newport", "Riverside",
            "Winchester", "Fairview", "Arlington", "Lebanon", "Westport", "Plainfield", "Centerville"
        ]
        
        # States with abbreviations
        self.states = [
            ("Alabama", "AL"), ("Alaska", "AK"), ("Arizona", "AZ"), ("Arkansas", "AR"), ("California", "CA"),
            ("Colorado", "CO"), ("Connecticut", "CT"), ("Delaware", "DE"), ("Florida", "FL"), ("Georgia", "GA"),
            ("Hawaii", "HI"), ("Idaho", "ID"), ("Illinois", "IL"), ("Indiana", "IN"), ("Iowa", "IA"),
            ("Kansas", "KS"), ("Kentucky", "KY"), ("Louisiana", "LA"), ("Maine", "ME"), ("Maryland", "MD"),
            ("Massachusetts", "MA"), ("Michigan", "MI"), ("Minnesota", "MN"), ("Mississippi", "MS"), ("Missouri", "MO")
        ]
        
        # Common diagnoses with ICD-10 codes
        self.diagnoses = [
            ("Hypertension", "I10"), 
            ("Type 2 Diabetes", "E11.9"),
            ("Asthma", "J45.909"),
            ("Acute Upper Respiratory Infection", "J06.9"),
            ("Low Back Pain", "M54.5"),
            ("Major Depressive Disorder", "F32.9"),
            ("Anxiety Disorder", "F41.9"),
            ("Hyperlipidemia", "E78.5"),
            ("Urinary Tract Infection", "N39.0"),
            ("Sinusitis", "J32.9"),
            ("GERD", "K21.9"),
            ("Osteoarthritis", "M19.90"),
            ("Obesity", "E66.9"),
            ("Vitamin D Deficiency", "E55.9"),
            ("Migraine", "G43.909")
        ]
        
        # Common medications with units
        self.medications = [
            ("Lisinopril", "10 mg"),
            ("Atorvastatin", "20 mg"),
            ("Levothyroxine", "50 mcg"),
            ("Metformin", "500 mg"),
            ("Amlodipine", "5 mg"),
            ("Metoprolol", "25 mg"),
            ("Albuterol", "90 mcg/actuation"),
            ("Omeprazole", "40 mg"),
            ("Prednisone", "5 mg"),
            ("Gabapentin", "300 mg"),
            ("Sertraline", "50 mg"),
            ("Fluticasone", "50 mcg/actuation"),
            ("Hydrochlorothiazide", "12.5 mg"),
            ("Ibuprofen", "800 mg"),
            ("Acetaminophen", "500 mg"),
            ("Losartan", "50 mg"),
            ("Amoxicillin", "500 mg"),
            ("Furosemide", "20 mg"),
            ("Simvastatin", "40 mg"),
            ("Escitalopram", "10 mg")
        ]
        
        # Common lab tests
        self.lab_tests = [
            ("Complete Blood Count", "CBC"),
            ("Basic Metabolic Panel", "BMP"),
            ("Comprehensive Metabolic Panel", "CMP"),
            ("Lipid Panel", "LIPID"),
            ("Hemoglobin A1C", "HBA1C"),
            ("Thyroid Stimulating Hormone", "TSH"),
            ("Urinalysis", "UA"),
            ("Liver Function Tests", "LFT"),
            ("Prothrombin Time", "PT"),
            ("Partial Thromboplastin Time", "PTT"),
            ("C-Reactive Protein", "CRP"),
            ("Erythrocyte Sedimentation Rate", "ESR"),
            ("Vitamin D, 25-Hydroxy", "VIT-D"),
            ("Vitamin B12", "B12"),
            ("Ferritin", "FERRITN"),
            ("Iron and Total Iron Binding Capacity", "IRON/TIBC"),
            ("Blood Culture", "BLDCX"),
            ("Urine Culture", "URINECX")
        ]
        
        # Common lab units
        self.lab_units = {
            "CBC": {
                "WBC": ("4.5-11.0", "x10^3/uL"),
                "RBC": ("4.5-5.9", "x10^6/uL"),
                "HGB": ("13.5-17.5", "g/dL"),
                "HCT": ("41.0-53.0", "%"),
                "PLT": ("150-400", "x10^3/uL")
            },
            "BMP": {
                "Sodium": ("135-145", "mmol/L"),
                "Potassium": ("3.5-5.0", "mmol/L"),
                "Chloride": ("98-108", "mmol/L"),
                "CO2": ("22-30", "mmol/L"),
                "BUN": ("7-20", "mg/dL"),
                "Creatinine": ("0.6-1.3", "mg/dL"),
                "Glucose": ("70-100", "mg/dL")
            },
            "LIPID": {
                "Total Cholesterol": ("125-200", "mg/dL"),
                "HDL": ("40-60", "mg/dL"),
                "LDL": ("0-130", "mg/dL"),
                "Triglycerides": ("0-150", "mg/dL")
            },
            "HBA1C": {
                "HbA1c": ("4.0-5.6", "%")
            },
            "TSH": {
                "TSH": ("0.4-4.0", "mIU/L")
            }
        }
        
        # Common encounter types
        self.encounter_types = [
            "Office Visit", "Hospital Encounter", "Telehealth", "Emergency", "Surgery", 
            "Wellness Exam", "Urgent Care", "Follow-up", "Specialist Consult"
        ]
        
        # Common encounter statuses
        self.encounter_statuses = [
            "completed", "in-progress", "cancelled", "entered-in-error"
        ]
        
        # Common provider specialties
        self.provider_specialties = [
            "Family Medicine", "Internal Medicine", "Cardiology", "Pediatrics", 
            "Obstetrics and Gynecology", "Psychiatry", "Orthopedics", "Neurology",
            "Dermatology", "Gastroenterology", "Endocrinology", "Nephrology", "Oncology"
        ]
        
        # Common provider names (first+last)
        self.provider_names = [
            "Dr. James Wilson", "Dr. Sarah Johnson", "Dr. Michael Chen", "Dr. Elizabeth Taylor",
            "Dr. David Kim", "Dr. Jennifer Martinez", "Dr. Robert Garcia", "Dr. Lisa Brown",
            "Dr. William Thompson", "Dr. Maria Rodriguez", "Dr. Thomas Anderson", "Dr. Susan Davis",
            "Dr. Richard Lee", "Dr. Patricia Scott", "Dr. Joseph Wright", "Dr. Emily Clark"
        ]
    
    def generate_patient(self) -> Dict[str, Any]:
        """
        Generate synthetic patient data.
        
        Returns:
            Dictionary containing patient demographic information
        """
        # Generate a unique patient ID
        patient_id = f"PT{random.randint(10000, 99999)}"
        
        # Randomly select gender
        gender = random.choice(["M", "F"])
        
        # Generate random name based on gender
        if gender == "M":
            first_name = random.choice(self.first_names[:20])
        else:
            first_name = random.choice(self.first_names[20:])
        last_name = random.choice(self.last_names)
        
        # Generate date of birth (between 18 and 90 years ago)
        years_ago = random.randint(18, 90)
        days_variation = random.randint(0, 365)
        dob = datetime.datetime.now() - datetime.timedelta(days=years_ago*365 + days_variation)
        dob_str = dob.strftime("%Y-%m-%d")
        
        # Generate random address
        street_number = random.randint(1, 9999)
        street = random.choice(self.streets)
        city = random.choice(self.cities)
        state, state_abbr = random.choice(self.states)
        zipcode = f"{random.randint(10000, 99999)}"
        
        # Generate random contact information
        phone = f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        has_email = random.random() > 0.3  # 70% chance of having email
        if has_email:
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@example.com"
        else:
            email = None
        
        # Generate random insurance information
        insurances = ["Medicare", "Medicaid", "Blue Cross Blue Shield", "Aetna", "UnitedHealthcare", "Cigna", "Kaiser", "Humana"]
        has_insurance = random.random() > 0.1  # 90% chance of having insurance
        if has_insurance:
            insurance = random.choice(insurances)
            insurance_id = f"INS{random.randint(10000000, 99999999)}"
        else:
            insurance = "Self Pay"
            insurance_id = None
        
        # Generate random MRN (Medical Record Number)
        mrn = f"MRN{random.randint(100000, 999999)}"
        
        # Build patient record in legacy EHR format
        patient = {
            "patient_id": patient_id,
            "mrn": mrn,
            "first_name": first_name,
            "last_name": last_name,
            "middle_name": random.choice(["", random.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))]),
            "birth_date": dob_str,
            "gender": gender,
            "address": {
                "line1": f"{street_number} {street}",
                "line2": random.choice(["", f"Apt {random.randint(1, 999)}"]),
                "city": city,
                "state": state,
                "state_code": state_abbr,
                "postal_code": zipcode,
                "country": "USA"
            },
            "contact": {
                "phone": phone,
                "email": email
            },
            "insurance": {
                "company": insurance,
                "id": insurance_id,
                "group_number": f"GRP{random.randint(1000, 9999)}" if has_insurance and insurance != "Medicare" and insurance != "Medicaid" else None
            },
            "registration_date": (datetime.datetime.now() - datetime.timedelta(days=random.randint(0, years_ago*365))).strftime("%Y-%m-%d"),
            "active": random.random() > 0.1,  # 90% chance of being active
            "deceased": random.random() < 0.05,  # 5% chance of being deceased
            "preferred_language": random.choice(["English", "English", "English", "Spanish", "Chinese", "French"])
        }
        
        return patient
    
    def generate_encounter(self, patient_id: str) -> Dict[str, Any]:
        """
        Generate synthetic encounter data for a patient.
        
        Args:
            patient_id: The patient ID to associate with the encounter
            
        Returns:
            Dictionary containing encounter information
        """
        # Generate encounter ID
        encounter_id = f"ENC{random.randint(100000, 999999)}"
        
        # Generate random encounter type and status
        encounter_type = random.choice(self.encounter_types)
        status = random.choice(self.encounter_statuses)
        
        # Generate random provider
        provider_name = random.choice(self.provider_names)
        provider_specialty = random.choice(self.provider_specialties)
        provider_id = f"PROV{provider_name.replace('Dr. ', '').replace(' ', '')[0:3]}{random.randint(100, 999)}"
        
        # Generate random encounter date (within last 2 years)
        days_ago = random.randint(0, 730)
        encounter_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).strftime("%Y-%m-%d")
        
        # Generate random diagnoses (1-3)
        num_diagnoses = random.randint(1, 3)
        diagnoses = []
        selected_diagnoses = random.sample(self.diagnoses, num_diagnoses)
        for diagnosis_name, diagnosis_code in selected_diagnoses:
            diagnoses.append({
                "diagnosis": diagnosis_name,
                "code": diagnosis_code,
                "type": "ICD-10"
            })
        
        # Generate random chief complaint
        chief_complaints = [
            "Chest pain", "Shortness of breath", "Headache", "Abdominal pain",
            "Back pain", "Fatigue", "Fever", "Cough", "Nausea", "Dizziness",
            "Rash", "Sore throat", "Joint pain", "Difficulty sleeping", "Anxiety"
        ]
        chief_complaint = random.choice(chief_complaints)
        
        # Build encounter record in legacy EHR format
        encounter = {
            "encounter_id": encounter_id,
            "patient_id": patient_id,
            "type": encounter_type,
            "status": status,
            "provider": {
                "id": provider_id,
                "name": provider_name,
                "specialty": provider_specialty
            },
            "encounter_date": encounter_date,
            "chief_complaint": chief_complaint,
            "diagnoses": diagnoses,
            "location": random.choice(["Main Campus", "North Clinic", "South Clinic", "East Wing", "West Wing", "Telehealth"])
        }
        
        # Add discharge date if encounter is complete
        if status == "completed":
            # Randomly determine length of stay (0-10 days)
            length_of_stay = 0
            if encounter_type in ["Hospital Encounter", "Emergency", "Surgery"]:
                length_of_stay = random.randint(0, 10)
                
            discharge_date = (datetime.datetime.strptime(encounter_date, "%Y-%m-%d") + 
                             datetime.timedelta(days=length_of_stay)).strftime("%Y-%m-%d")
            encounter["discharge_date"] = discharge_date
        
        # Add notes if completed
        if status == "completed":
            encounter["notes"] = f"Patient presented with {chief_complaint}. Examination performed. "
            if diagnoses:
                encounter["notes"] += f"Diagnosed with {diagnoses[0]['diagnosis']}. "
            
            # Add random note ending
            note_endings = [
                "Patient advised to follow up in 2 weeks.",
                "Prescription provided. Follow up as needed.",
                "Referral to specialist recommended.",
                "Patient counseled on lifestyle modifications.",
                "Condition improved with treatment."
            ]
            encounter["notes"] += random.choice(note_endings)
        
        return encounter
    
    def generate_observation(self, patient_id: str, encounter_id: str) -> Dict[str, Any]:
        """
        Generate synthetic observation (lab result) for a patient encounter.
        
        Args:
            patient_id: The patient ID to associate with the observation
            encounter_id: The encounter ID to associate with the observation
            
        Returns:
            Dictionary containing observation information
        """
        # Generate observation ID
        observation_id = f"OBS{random.randint(1000000, 9999999)}"
        
        # Generate random test
        lab_test, lab_code = random.choice(self.lab_tests)
        
        # Generate observation date based on encounter date
        # Assume observation is taken on the same day as encounter for simplicity
        observation_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # Generate random values if test has predefined units
        results = []
        if lab_code in self.lab_units:
            for component, (range_str, unit) in self.lab_units[lab_code].items():
                try:
                    # Parse range and generate a value within or slightly outside that range
                    low, high = map(float, range_str.split("-"))
                    # 80% chance value is within range, 20% chance it's slightly abnormal
                    if random.random() < 0.8:
                        value = round(random.uniform(low, high), 1)
                        status = "normal"
                    else:
                        # Generate slightly abnormal value
                        if random.random() < 0.5:
                            # Lower than normal
                            value = round(random.uniform(low * 0.7, low * 0.99), 1)
                            status = "low"
                        else:
                            # Higher than normal
                            value = round(random.uniform(high * 1.01, high * 1.3), 1)
                            status = "high"
                            
                    results.append({
                        "component": component,
                        "value": str(value),
                        "unit": unit,
                        "reference_range": range_str,
                        "status": status
                    })
                except ValueError:
                    # Fallback for malformed ranges
                    results.append({
                        "component": component,
                        "value": "5.0",
                        "unit": unit,
                        "reference_range": range_str,
                        "status": "normal"
                    })
        else:
            # Generate generic result for tests without predefined units
            status_options = ["normal", "abnormal", "normal", "normal"]  # 75% chance of being normal
            results.append({
                "component": lab_test,
                "value": str(round(random.uniform(1, 100), 1)),
                "unit": random.choice(["mg/dL", "mmol/L", "U/L", "%"]),
                "reference_range": f"{round(random.uniform(1, 40), 1)}-{round(random.uniform(41, 120), 1)}",
                "status": random.choice(status_options)
            })
        
        # Build observation record in legacy EHR format
        observation = {
            "observation_id": observation_id,
            "patient_id": patient_id,
            "encounter_id": encounter_id,
            "test_name": lab_test,
            "test_code": lab_code,
            "observation_date": observation_date,
            "results": results,
            "status": random.choice(["final", "preliminary", "corrected", "cancelled"]),
            "performer": random.choice(["Main Lab", "Point of Care", "Reference Lab", "Radiology"])
        }
        
        return observation
    
    def generate_medication(self, patient_id: str, encounter_id: str) -> Dict[str, Any]:
        """
        Generate synthetic medication order for a patient encounter.
        
        Args:
            patient_id: The patient ID to associate with the medication
            encounter_id: The encounter ID to associate with the medication
            
        Returns:
            Dictionary containing medication information
        """
        # Generate medication ID
        medication_id = f"MED{random.randint(1000000, 9999999)}"
        
        # Generate random medication
        medication_name, medication_dose = random.choice(self.medications)
        
        # Generate prescription date based on encounter
        prescription_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # Generate random duration (in days)
        duration_days = random.choice([7, 10, 14, 30, 60, 90, 180, 365])
        
        # Generate random frequency
        frequencies = ["Once daily", "Twice daily", "Three times daily", "Four times daily", 
                      "Every morning", "Every evening", "Every 4 hours", "Every 6 hours",
                      "Every 8 hours", "Every 12 hours", "As needed", "Weekly"]
        frequency = random.choice(frequencies)
        
        # Build medication record in legacy EHR format
        medication = {
            "medication_id": medication_id,
            "patient_id": patient_id,
            "encounter_id": encounter_id,
            "medication_name": medication_name,
            "dose": medication_dose,
            "route": random.choice(["Oral", "Intravenous", "Intramuscular", "Topical", "Inhalation", "Subcutaneous"]),
            "frequency": frequency,
            "prescription_date": prescription_date,
            "duration_days": duration_days,
            "refills": random.randint(0, 5),
            "status": random.choice(["active", "completed", "cancelled", "on-hold"]),
            "prescriber": random.choice(self.provider_names),
            "pharmacy": random.choice(["CVS Pharmacy", "Walgreens", "Rite Aid", "Hospital Pharmacy", "Mail Order"])
        }
        
        return medication
    
    def generate_full_patient_record(self, num_encounters_range: tuple = (1, 10)) -> Dict[str, Any]:
        """
        Generate a complete patient record with encounters, observations, and medications.
        
        Args:
            num_encounters_range: Range of number of encounters to generate (min, max)
            
        Returns:
            Dictionary containing complete patient data
        """
        # Generate patient demographics
        patient = self.generate_patient()
        patient_id = patient["patient_id"]
        
        # Generate random number of encounters
        num_encounters = random.randint(num_encounters_range[0], num_encounters_range[1])
        
        encounters = []
        observations = []
        medications = []
        
        # Generate encounters, observations, and medications
        for _ in range(num_encounters):
            # Generate encounter
            encounter = self.generate_encounter(patient_id)
            encounter_id = encounter["encounter_id"]
            encounters.append(encounter)
            
            # Generate random number of observations (0-5)
            num_observations = random.randint(0, 5)
            for _ in range(num_observations):
                observation = self.generate_observation(patient_id, encounter_id)
                observations.append(observation)
            
            # Generate random number of medications (0-3)
            num_medications = random.randint(0, 3)
            for _ in range(num_medications):
                medication = self.generate_medication(patient_id, encounter_id)
                medications.append(medication)
        
        # Build complete patient record
        patient_record = {
            "patient": patient,
            "encounters": encounters,
            "observations": observations,
            "medications": medications
        }
        
        return patient_record
    
    def generate_database(self, num_patients: int = 100) -> Dict[str, Any]:
        """
        Generate a complete synthetic EHR database with multiple patients.
        
        Args:
            num_patients: Number of patient records to generate
            
        Returns:
            Dictionary containing complete EHR database
        """
        print(f"Generating synthetic EHR database with {num_patients} patients...")
        
        patients = []
        all_encounters = []
        all_observations = []
        all_medications = []
        
        # Generate patient records
        for i in range(num_patients):
            if i % 10 == 0:
                print(f"Generated {i} patients...")
                
            # Generate patient record
            patient_record = self.generate_full_patient_record()
            
            # Extract components
            patients.append(patient_record["patient"])
            all_encounters.extend(patient_record["encounters"])
            all_observations.extend(patient_record["observations"])
            all_medications.extend(patient_record["medications"])
        
        # Build complete database
        database = {
            "patients": patients,
            "encounters": all_encounters,
            "observations": all_observations,
            "medications": all_medications
        }
        
        print(f"Generated {len(patients)} patients with " +
              f"{len(all_encounters)} encounters, " +
              f"{len(all_observations)} observations, and " +
              f"{len(all_medications)} medications.")
        
        return database
    
    def save_database(self, database: Dict[str, Any], split_files: bool = True) -> None:
        """
        Save the generated database to JSON files.
        
        Args:
            database: The database to save
            split_files: Whether to split into separate files by entity type
        """
        if split_files:
            # Save each entity type to a separate file
            for entity_type, entities in database.items():
                filename = os.path.join(self.output_dir, f"{entity_type}.json")
                with open(filename, 'w') as f:
                    json.dump(entities, f, indent=2)
                print(f"Saved {len(entities)} {entity_type} to {filename}")
        else:
            # Save entire database to a single file
            filename = os.path.join(self.output_dir, "legacy_ehr_database.json")
            with open(filename, 'w') as f:
                json.dump(database, f, indent=2)
            print(f"Saved entire database to {filename}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Generate synthetic EHR data')
    parser.add_argument('--output', default='./legacy_ehr_data', help='Output directory')
    parser.add_argument('--patients', type=int, default=50, help='Number of patients to generate')
    parser.add_argument('--seed', type=int, help='Random seed for reproducibility')
    parser.add_argument('--single-file', action='store_true', help='Save as a single file instead of separate files')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = SyntheticEHRGenerator(output_dir=args.output, seed=args.seed)
    
    # Generate database
    database = generator.generate_database(num_patients=args.patients)
    
    # Save database
    generator.save_database(database, split_files=not args.single_file)


if __name__ == "__main__":
    main()