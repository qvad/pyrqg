-- Healthcare Records System Schema
-- HIPAA-compliant patient records, appointments, and medical history

-- Patients
CREATE TABLE IF NOT EXISTS patients (
    patient_id SERIAL PRIMARY KEY,
    mrn VARCHAR(20) UNIQUE NOT NULL, -- Medical Record Number
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender VARCHAR(10),
    ssn_encrypted BYTEA, -- Encrypted SSN
    email VARCHAR(255),
    phone VARCHAR(20),
    emergency_contact_name VARCHAR(200),
    emergency_contact_phone VARCHAR(20),
    blood_type VARCHAR(5),
    allergies TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Healthcare providers
CREATE TABLE IF NOT EXISTS providers (
    provider_id SERIAL PRIMARY KEY,
    npi VARCHAR(10) UNIQUE NOT NULL, -- National Provider Identifier
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    specialty VARCHAR(100),
    license_number VARCHAR(50),
    license_state VARCHAR(2),
    email VARCHAR(255),
    phone VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE
);

-- Departments
CREATE TABLE IF NOT EXISTS departments (
    department_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(20) UNIQUE NOT NULL,
    floor INTEGER,
    building VARCHAR(50)
);

-- Appointments
CREATE TABLE IF NOT EXISTS appointments (
    appointment_id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES patients(patient_id),
    provider_id INTEGER REFERENCES providers(provider_id),
    department_id INTEGER REFERENCES departments(department_id),
    appointment_datetime TIMESTAMP NOT NULL,
    duration_minutes INTEGER DEFAULT 30,
    appointment_type VARCHAR(50),
    status VARCHAR(20) DEFAULT 'scheduled',
    check_in_time TIMESTAMP,
    check_out_time TIMESTAMP,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cancelled_at TIMESTAMP,
    cancellation_reason VARCHAR(200)
);

-- Medical visits/encounters
CREATE TABLE IF NOT EXISTS encounters (
    encounter_id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES patients(patient_id),
    provider_id INTEGER REFERENCES providers(provider_id),
    appointment_id INTEGER REFERENCES appointments(appointment_id),
    encounter_date DATE NOT NULL,
    chief_complaint TEXT,
    vital_signs JSONB,
    diagnosis_codes TEXT[],
    procedure_codes TEXT[],
    notes TEXT,
    is_emergency BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Lab orders
CREATE TABLE IF NOT EXISTS lab_orders (
    lab_order_id SERIAL PRIMARY KEY,
    encounter_id INTEGER REFERENCES encounters(encounter_id),
    patient_id INTEGER REFERENCES patients(patient_id),
    ordering_provider_id INTEGER REFERENCES providers(provider_id),
    test_code VARCHAR(20) NOT NULL,
    test_name VARCHAR(200) NOT NULL,
    status VARCHAR(20) DEFAULT 'ordered',
    ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    collected_at TIMESTAMP,
    resulted_at TIMESTAMP,
    priority VARCHAR(20) DEFAULT 'routine'
);

-- Lab results
CREATE TABLE IF NOT EXISTS lab_results (
    result_id SERIAL PRIMARY KEY,
    lab_order_id INTEGER REFERENCES lab_orders(lab_order_id),
    component_name VARCHAR(200),
    value VARCHAR(100),
    unit VARCHAR(50),
    reference_range VARCHAR(100),
    flag VARCHAR(10),
    resulted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Medications
CREATE TABLE IF NOT EXISTS medications (
    medication_id SERIAL PRIMARY KEY,
    ndc_code VARCHAR(20),
    name VARCHAR(200) NOT NULL,
    generic_name VARCHAR(200),
    strength VARCHAR(50),
    form VARCHAR(50),
    route VARCHAR(50)
);

-- Prescriptions
CREATE TABLE IF NOT EXISTS prescriptions (
    prescription_id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES patients(patient_id),
    provider_id INTEGER REFERENCES providers(provider_id),
    encounter_id INTEGER REFERENCES encounters(encounter_id),
    medication_id INTEGER REFERENCES medications(medication_id),
    dosage VARCHAR(100),
    frequency VARCHAR(100),
    duration VARCHAR(100),
    quantity INTEGER,
    refills INTEGER DEFAULT 0,
    prescribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active',
    discontinued_at TIMESTAMP
);

-- Immunizations
CREATE TABLE IF NOT EXISTS immunizations (
    immunization_id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES patients(patient_id),
    vaccine_code VARCHAR(20),
    vaccine_name VARCHAR(200),
    administered_date DATE,
    administered_by INTEGER REFERENCES providers(provider_id),
    lot_number VARCHAR(50),
    expiration_date DATE,
    site VARCHAR(50),
    dose_number INTEGER
);

-- Insurance information
CREATE TABLE IF NOT EXISTS insurance (
    insurance_id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES patients(patient_id),
    insurance_company VARCHAR(200),
    policy_number VARCHAR(50),
    group_number VARCHAR(50),
    subscriber_name VARCHAR(200),
    subscriber_relationship VARCHAR(50),
    effective_date DATE,
    termination_date DATE,
    is_primary BOOLEAN DEFAULT TRUE
);

-- Indexes for performance
CREATE INDEX idx_appointments_patient ON appointments(patient_id);
CREATE INDEX idx_appointments_provider ON appointments(provider_id);
CREATE INDEX idx_appointments_datetime ON appointments(appointment_datetime);
CREATE INDEX idx_appointments_status ON appointments(status);
CREATE INDEX idx_encounters_patient ON encounters(patient_id);
CREATE INDEX idx_encounters_date ON encounters(encounter_date);
CREATE INDEX idx_lab_orders_patient ON lab_orders(patient_id);
CREATE INDEX idx_lab_orders_status ON lab_orders(status);
CREATE INDEX idx_prescriptions_patient ON prescriptions(patient_id);
CREATE INDEX idx_prescriptions_status ON prescriptions(status);

-- Views for common queries
CREATE OR REPLACE VIEW patient_summary AS
SELECT 
    p.patient_id,
    p.mrn,
    p.first_name || ' ' || p.last_name as full_name,
    p.date_of_birth,
    AGE(p.date_of_birth) as age,
    COUNT(DISTINCT e.encounter_id) as total_visits,
    COUNT(DISTINCT pr.prescription_id) as active_medications,
    MAX(e.encounter_date) as last_visit
FROM patients p
LEFT JOIN encounters e ON p.patient_id = e.patient_id
LEFT JOIN prescriptions pr ON p.patient_id = pr.patient_id AND pr.status = 'active'
GROUP BY p.patient_id;