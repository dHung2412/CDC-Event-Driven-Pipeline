DROP TABLE IF EXISTS Bookings_Active;
CREATE TABLE Bookings_Active (
    booking_id VARCHAR(100) PRIMARY KEY,
    shipment_type VARCHAR(50) NOT NULL,
    booking_date DATETIME NOT NULL,
    vehicle_no VARCHAR(50) NOT NULL,
    vehicle_type VARCHAR(100),
    origin TEXT,
    destination TEXT,
    planned_eta TIME,
    actual_eta DATETIME,
    ontime BOOLEAN,
    trip_start DATETIME,
    trip_end DATETIME,
    distance_km INT,
    driver_name VARCHAR(255),
    driver_mobile VARCHAR(20),
    customer TEXT,
    supplier TEXT,
    material TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS Bookings_History;
CREATE TABLE Bookings_History (
    booking_id VARCHAR(100) PRIMARY KEY,
    shipment_type VARCHAR(50) NOT NULL,
    booking_date DATETIME NOT NULL,
    vehicle_no VARCHAR(50) NOT NULL,
    vehicle_type VARCHAR(100),
    origin TEXT,
    destination TEXT,
    planned_eta DATETIME NOT NULL,
    actual_eta DATETIME,
    ontime BOOLEAN NOT NULL,
    trip_start DATETIME NOT NULL,
    trip_end DATETIME NOT NULL,
    distance_km INT,
    driver_name VARCHAR(255),
    driver_mobile VARCHAR(20),
    customer TEXT,
    supplier TEXT,
    material TEXT,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);