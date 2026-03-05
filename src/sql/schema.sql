DROP TABLE IF EXISTS Bookings_Active;
CREATE TABLE Bookings_Active (
    booking_id VARCHAR(100) PRIMARY KEY,
    shipment_type VARCHAR(50) NOT NULL,
    booking_date DATETIME NOT NULL,
    vehicle_no VARCHAR(50) NOT NULL,
    vehicle_type VARCHAR(100),
    
    -- Routing
    origin TEXT,
    destination TEXT,
    lat_origin DECIMAL(10, 8),
    long_origin DECIMAL(11, 8),
    lat_destination DECIMAL(10, 8),
    long_destination DECIMAL(11, 8),
    distance_km INT,
    
    -- Tracking & GPS
    gps_provider VARCHAR(100),
    data_ping_time DATETIME,
    current_lat DECIMAL(10, 8),
    current_long DECIMAL(11, 8),
    
    -- SLA & Time
    planned_eta DATETIME,
    actual_eta DATETIME,
    ontime BOOLEAN,
    trip_start DATETIME,
    trip_end DATETIME,
    
    -- Driver & Performance
    driver_name VARCHAR(255),
    driver_mobile VARCHAR(20),
    customer TEXT,
    supplier TEXT,
    material TEXT,
    min_kms_day INT NULL,
    
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
    distance_km INT,
    planned_eta DATETIME NOT NULL,
    actual_eta DATETIME,
    ontime BOOLEAN NOT NULL,
    trip_start DATETIME NOT NULL,
    trip_end DATETIME NOT NULL,
    driver_name VARCHAR(255),
    driver_mobile VARCHAR(20),
    customer TEXT,
    supplier TEXT,
    material TEXT,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);