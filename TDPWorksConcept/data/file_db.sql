-- Procurement Types Table
CREATE TABLE ProcurementTypes (
    ProcurementTypeID INT AUTO_INCREMENT PRIMARY KEY,
    TypeName VARCHAR(50) NOT NULL UNIQUE
);

-- Categories Table
CREATE TABLE Categories (
    CategoryID INT AUTO_INCREMENT PRIMARY KEY,
    CategoryName VARCHAR(255) NOT NULL UNIQUE
);

-- Units Table
CREATE TABLE Units (
    UnitID INT AUTO_INCREMENT PRIMARY KEY,
    UnitName VARCHAR(50) NOT NULL UNIQUE
);

-- Parts Table
CREATE TABLE Parts (
    PartID INT AUTO_INCREMENT PRIMARY KEY,
    PartNumber VARCHAR(50) NOT NULL UNIQUE,
    Nomenclature VARCHAR(255) NOT NULL,
    ProcurementTypeID INT,
    Mass DECIMAL(10, 2),
    UnitID INT,
    CriticalSafetyItem BOOLEAN,
    ExportClassification VARCHAR(50),
    Statement_Export VARCHAR(255),
    CreatedAt DATETIME,
    CreatedBy VARCHAR(255),
    FOREIGN KEY (ProcurementTypeID) REFERENCES ProcurementTypes(ProcurementTypeID),
    FOREIGN KEY (UnitID) REFERENCES Units(UnitID)
);

-- Products Table
CREATE TABLE Products (
    ProductID INT AUTO_INCREMENT PRIMARY KEY,
    ProductName VARCHAR(255) NOT NULL,
    Description TEXT,
    CategoryID INT,
    CreatedAt DATETIME,
    CreatedBy VARCHAR(255),
    FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);

-- Product Attributes Table
CREATE TABLE ProductAttributes (
    AttributeID INT AUTO_INCREMENT PRIMARY KEY,
    ProductID INT,
    AttributeName VARCHAR(255) NOT NULL,
    AttributeValue TEXT,
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

-- Bill of Materials (BOM) Table
CREATE TABLE BOM (
    BOMID INT AUTO_INCREMENT PRIMARY KEY,
    ProductID INT,
    PartID INT,
    Quantity INT CHECK (Quantity > 0),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
    FOREIGN KEY (PartID) REFERENCES Parts(PartID)
);

-- Status Table
CREATE TABLE Status (
    StatusID INT AUTO_INCREMENT PRIMARY KEY,
    StatusName VARCHAR(50) NOT NULL UNIQUE
);

-- Part Revisions Table
CREATE TABLE PartRevisions (
    RevisionID INT AUTO_INCREMENT PRIMARY KEY,
    PartID INT,
    Revision VARCHAR(10),
    CreatedAt DATETIME,
    CreatedBy VARCHAR(255),
    StatusID INT,
    FOREIGN KEY (PartID) REFERENCES Parts(PartID),
    FOREIGN KEY (StatusID) REFERENCES Status(StatusID)
);

-- Change Management Table
CREATE TABLE ChangeManagement (
    ChangeID INT AUTO_INCREMENT PRIMARY KEY,
    PartID INT,
    ChangeType VARCHAR(50),
    StatusID INT,
    ChangeDescription TEXT,
    RequestedBy VARCHAR(255),
    ChangeDate DATETIME,
    FOREIGN KEY (PartID) REFERENCES Parts(PartID),
    FOREIGN KEY (StatusID) REFERENCES Status(StatusID)
);

-- Part Components Table
CREATE TABLE PartComponents (
    ComponentID INT AUTO_INCREMENT PRIMARY KEY,
    ParentPartID INT,
    ChildPartID INT,
    Quantity INT CHECK (Quantity > 0),
    FOREIGN KEY (ParentPartID) REFERENCES Parts(PartID),
    FOREIGN KEY (ChildPartID) REFERENCES Parts(PartID)
);

-- Assemblies Table
CREATE TABLE Assemblies (
    AssemblyID INT AUTO_INCREMENT PRIMARY KEY,
    AssemblyName VARCHAR(255) NOT NULL,
    Description TEXT
);

-- Part/Assembly Occurrences Table
CREATE TABLE Occurrences (
    OccurrenceID INT AUTO_INCREMENT PRIMARY KEY,
    PartID INT,
    AssemblyID INT,
    ReferenceDesignator VARCHAR(255),
    Quantity INT CHECK (Quantity > 0),
    FOREIGN KEY (PartID) REFERENCES Parts(PartID),
    FOREIGN KEY (AssemblyID) REFERENCES Assemblies(AssemblyID)
);

-- Stakeholders Table
CREATE TABLE Stakeholders (
    StakeholderID INT AUTO_INCREMENT PRIMARY KEY,
    FullName VARCHAR(255),
    Email VARCHAR(255),
    Telephone VARCHAR(50),
    Geography VARCHAR(100),
    Language VARCHAR(50),
    CreatedAt DATETIME
);

-- Revision Effectivity Table
CREATE TABLE RevisionEffectivity (
    EffectivityID INT AUTO_INCREMENT PRIMARY KEY,
    RevisionID INT,
    UsedOn DATE,
    EffectiveToDate DATE,
    FOREIGN KEY (RevisionID) REFERENCES PartRevisions(RevisionID)
);

-- Views and Indexes remain same.
-- Triggers and other considerations remain same.
-- Example Audit Table for Parts
CREATE TABLE PartsAudit (
    AuditID INT AUTO_INCREMENT PRIMARY KEY,
    PartID INT,
    PartNumber VARCHAR(50),
    Nomenclature VARCHAR(255),
    ProcurementTypeID INT,
    Mass DECIMAL(10, 2),
    UnitID INT,
    CriticalSafetyItem BOOLEAN,
    ExportClassification VARCHAR(50),
    Statement_Export VARCHAR(255),
    CreatedAt DATETIME,
    CreatedBy VARCHAR(255),
    Action VARCHAR(10), -- 'INSERT', 'UPDATE', 'DELETE'
    ActionTimestamp DATETIME,
    ActionUser VARCHAR(255)
);

-- Example Trigger for Parts INSERT
DELIMITER //
CREATE TRIGGER parts_after_insert
AFTER INSERT ON Parts
FOR EACH ROW
BEGIN
    INSERT INTO PartsAudit (PartID, PartNumber, Nomenclature, ProcurementTypeID, Mass, UnitID, CriticalSafetyItem, ExportClassification, Statement_Export, CreatedAt, CreatedBy, Action, ActionTimestamp, ActionUser)
    VALUES (NEW.PartID, NEW.PartNumber, NEW.Nomenclature, NEW.ProcurementTypeID, NEW.Mass, NEW.UnitID, NEW.CriticalSafetyItem, NEW.ExportClassification, NEW.Statement_Export, NEW.CreatedAt, NEW.CreatedBy, 'INSERT', NOW(), USER());
END;
//

-- Example Trigger for Parts UPDATE
CREATE TRIGGER parts_after_update
AFTER UPDATE ON Parts
FOR EACH ROW
BEGIN
    INSERT INTO PartsAudit (PartID, PartNumber, Nomenclature, ProcurementTypeID, Mass, UnitID, CriticalSafetyItem, ExportClassification, Statement_Export, CreatedAt, CreatedBy, Action, ActionTimestamp, ActionUser)
    VALUES (NEW.PartID, NEW.PartNumber, NEW.Nomenclature, NEW.ProcurementTypeID, NEW.Mass, NEW.UnitID, NEW.CriticalSafetyItem, NEW.ExportClassification, NEW.Statement_Export, NEW.CreatedAt, NEW.CreatedBy, 'UPDATE', NOW(), USER());
END;
//

-- Example Trigger for Parts DELETE
CREATE TRIGGER parts_after_delete
AFTER DELETE ON Parts
FOR EACH ROW
BEGIN
    INSERT INTO PartsAudit (PartID, PartNumber, Nomenclature, ProcurementTypeID, Mass, UnitID, CriticalSafetyItem, ExportClassification, Statement_Export, CreatedAt, CreatedBy, Action, ActionTimestamp, ActionUser)
    VALUES (OLD.PartID, OLD.PartNumber, OLD.Nomenclature, OLD.ProcurementTypeID, OLD.Mass, OLD.UnitID, OLD.CriticalSafetyItem, OLD.ExportClassification, OLD.Statement_Export, OLD.CreatedAt, OLD.CreatedBy, 'DELETE', NOW(), USER());
END;
//
DELIMITER ;
DELIMITER //
CREATE PROCEDURE GetProductParts(IN productID INT)
BEGIN
    SELECT p.PartNumber, p.Nomenclature, pr.Revision, cm.ChangeDescription
    FROM Parts p
    JOIN BOM b ON p.PartID = b.PartID
    JOIN PartRevisions pr ON p.PartID = pr.PartID
    LEFT JOIN ChangeManagement cm ON p.PartID = cm.PartID
    WHERE b.ProductID = productID;
END;
//
DELIMITER ;

CREATE MATERIALIZED VIEW ProductPartQuantity AS
SELECT b.ProductID, p.PartNumber, SUM(b.Quantity) AS TotalQuantity
FROM BOM b
JOIN Parts p ON b.PartID = p.PartID
GROUP BY b.ProductID, p.PartNumber;

ALTER TABLE Products
ADD COLUMN Configuration JSON;

UPDATE Products
SET Configuration = '{"color": "red", "size": "large", "material": "steel"}'
WHERE ProductID = 1;

SELECT ProductName, Configuration->'$.color' AS Color
FROM Products;

CREATE TABLE IF NOT EXISTS FileProcessingMetrics (
    MetricID INT AUTO_INCREMENT PRIMARY KEY,
    ProcessedAt DATETIME DEFAULT NOW(),
    TotalProcessed INT DEFAULT 0,
    TotalFailed INT DEFAULT 0,
    TotalRetried INT DEFAULT 0,
    AvgProcessingTime DECIMAL(10, 2) NULL
);

