-- Create the Custody table
CREATE TABLE Custody (
    custody_id INT PRIMARY KEY,
    participant_id INT,
    gun_id INT,
    geo_id INT,
    date_id INT,
    crime_gravity DECIMAL(18, 2), -- Adjust the data type as needed
    -- Add other columns as needed
);
GO

-- Create the Geography table
CREATE TABLE Geography (
    geo_id INT PRIMARY KEY,
    latitude DECIMAL(9, 6), -- Adjust the data type as needed
    longitude DECIMAL(9, 6), -- Adjust the data type as needed
    city NVARCHAR(255), -- Adjust the data type as needed
    state NVARCHAR(255), -- Adjust the data type as needed
    continent NVARCHAR(255), -- Adjust the data type as needed
    -- Add other columns as needed
);
GO

-- Create the Gun table
CREATE TABLE Gun (
    gun_id INT PRIMARY KEY,
    is_stolen BIT, -- Boolean data type, 0 or 1
    gun_type NVARCHAR(255), -- Adjust the data type as needed
    -- Add other columns as needed
);
GO

-- Create the Date table
CREATE TABLE Date (
    date_id INT PRIMARY KEY,
    date DATE, -- Adjust the data type as needed
    day INT,
    month INT,
    year INT,
    quarter INT,
    day_of_week NVARCHAR(15), 
);
GO

-- Create the Incident table
CREATE TABLE Incident (
    incident_id INT PRIMARY KEY,
    -- Add other columns as needed
);
GO

-- Create the Participant table
CREATE TABLE Participant (
    participant_id INT PRIMARY KEY,
    age_group INT, -- Adjust the data type as needed
    gender NVARCHAR(10), -- Adjust the data type as needed
    status INT, -- Adjust the data type as needed
    type INT, --Ajust the data type as needed
	-- Add other columns as needed
);
GO

-- Add foreign keys in the Custody table
ALTER TABLE Custody
ADD CONSTRAINT FK_Custody_Participant
FOREIGN KEY (participant_id)
REFERENCES Participant(participant_id);
GO

ALTER TABLE Custody
ADD CONSTRAINT FK_Custody_Gun
FOREIGN KEY (gun_id)
REFERENCES Gun(gun_id);
GO

ALTER TABLE Custody
ADD CONSTRAINT FK_Custody_Geography
FOREIGN KEY (geo_id)
REFERENCES Geography(geo_id);
GO

ALTER TABLE Custody
ADD CONSTRAINT FK_Custody_Date
FOREIGN KEY (date_id)
REFERENCES Date(date_id);
GO