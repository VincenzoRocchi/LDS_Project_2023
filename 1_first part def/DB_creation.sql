-- Create the Custody table
CREATE TABLE Custody (
    custody_id INT PRIMARY KEY,
    participant_id INT,
    gun_id INT,
    geo_id INT,
    date_id INT,
    crime_gravity DECIMAL(18, 2), 
    incident_id INT,
);
GO

-- Create the Geography table
CREATE TABLE Geography (
    geo_id INT PRIMARY KEY,
    latitude DECIMAL(9, 6), 
    longitude DECIMAL(9, 6), 
    city NVARCHAR(255), 
    state NVARCHAR(255), 
    continent NVARCHAR(255), 

);
GO

-- Create the Gun table
CREATE TABLE Gun (
    gun_id INT PRIMARY KEY,
    is_stolen BIT, 
    gun_type NVARCHAR(255), 

);
GO

-- Create the Date table
CREATE TABLE Date (
    date_id INT PRIMARY KEY,
    date DATE, 
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

);
GO

-- Create the Participant table
CREATE TABLE Participant (
    participant_id INT PRIMARY KEY,
    age_group INT, 
    gender NVARCHAR(10), 
    status INT, 
    type INT, 

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

ALTER TABLE Custody
ADD CONSTRAINT FK_Custody_Incident
FOREIGN KEY (incident_id)
REFERENCES Incident(incident_id);
GO