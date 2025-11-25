CREATE TABLE Rooms (
    id INT PRIMARY KEY,       
    name VARCHAR(100) NOT NULL
);


CREATE TABLE Students (
    id INT PRIMARY KEY,        
    name VARCHAR(100) NOT NULL,
    birthday TIMESTAMP NOT NULL,
    sex CHAR(1) NOT NULL,
    room INT REFERENCES Rooms(id)
);