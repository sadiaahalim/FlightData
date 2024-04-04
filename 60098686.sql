SELECT * FROM dbo.flight2018

--viewing all the column names
EXEC sp_help 'dbo.flight2018';

--Counting the shape of the database before preprocessing
SELECT COUNT(*) AS shapeDbo
FROM FlightV1;

--Creating Fact Table with columns that i want

CREATE TABLE flightDbo (
    FlightDate DATE,
    Origin NVARCHAR(100),
    Dest NVARCHAR(100),
    Cancelled BIT,
    Diverted NVARCHAR(100),
    DepTime FLOAT,
    DepDelayMinutes FLOAT,
    Operating_Airline NVARCHAR(100),
    ArrTime FLOAT,
    ArrDelayMinutes FLOAT,
    AirTime FLOAT,
    ActualElapsedTime FLOAT,
    Distance FLOAT,
    DepartureDelayGroups FLOAT,
    ArrivalDelayGroups FLOAT,
    Year SMALLINT,
    Quarter TINYINT,
    Month TINYINT
);

INSERT INTO flightDbo(
	FlightDate, Origin, Dest, Cancelled, Diverted, DepTime, DepDelayMinutes, Operating_Airline, ArrTime, ArrDelayMinutes, AirTime, ActualElapsedTime, Distance, DepartureDelayGroups, ArrivalDelayGroups, Year, Quarter, Month
)
SELECT
	FlightDate, Origin, Dest, Cancelled, Diverted, DepTime, DepDelayMinutes, Operating_Airline, ArrTime, ArrDelayMinutes, AirTime, ActualElapsedTime, Distance, DepartureDelayGroups, ArrivalDelayGroups, Year, Quarter, Month
FROM dbo.flight2018



SELECT * FROM flightDbo


--Checking to see how many null values are there in each column
SELECT
    SUM(CASE WHEN FlightDate IS NULL THEN 1 ELSE 0 END) AS FlightDate,
    SUM(CASE WHEN Origin IS NULL THEN 1 ELSE 0 END) AS Origin,
    SUM(CASE WHEN Dest IS NULL THEN 1 ELSE 0 END) AS Dest,
    SUM(CASE WHEN Cancelled IS NULL THEN 1 ELSE 0 END) AS Cancelled,
    SUM(CASE WHEN Diverted IS NULL THEN 1 ELSE 0 END) AS Diverted,
    SUM(CASE WHEN DepTime IS NULL THEN 1 ELSE 0 END) AS DepTime,
    SUM(CASE WHEN DepDelayMinutes IS NULL THEN 1 ELSE 0 END) AS DepDelayMinutes,
    SUM(CASE WHEN Operating_Airline IS NULL THEN 1 ELSE 0 END) AS Operating_Airline,
    SUM(CASE WHEN ArrTime IS NULL THEN 1 ELSE 0 END) AS ArrTime,
    SUM(CASE WHEN ArrDelayMinutes IS NULL THEN 1 ELSE 0 END) AS ArrDelayMinutes,
    SUM(CASE WHEN AirTime IS NULL THEN 1 ELSE 0 END) AS AirTime,
    SUM(CASE WHEN ActualElapsedTime IS NULL THEN 1 ELSE 0 END) AS ActualElapsedTime,
    SUM(CASE WHEN Distance IS NULL THEN 1 ELSE 0 END) AS Distance,
    SUM(CASE WHEN DepartureDelayGroups IS NULL THEN 1 ELSE 0 END) AS DepartureDelayGroups,
    SUM(CASE WHEN ArrivalDelayGroups IS NULL THEN 1 ELSE 0 END) AS ArrivalDelayGroups,
    SUM(CASE WHEN Year IS NULL THEN 1 ELSE 0 END) AS Year,
    SUM(CASE WHEN Quarter IS NULL THEN 1 ELSE 0 END) AS Quarter,
    SUM(CASE WHEN Month IS NULL THEN 1 ELSE 0 END) AS Month
FROM
    flightDbo;


SELECT COUNT(*) AS shapeDbo
FROM FlightV1;

--Feature Engineering columns
UPDATE dbo.FlightDbo
SET 
    DepTime = COALESCE(DepTime, 0.0),
    DepDelayMinutes = COALESCE(DepDelayMinutes, 0.0),
    ArrTime = COALESCE(ArrTime, 0.0),
    ArrDelayMinutes = COALESCE(ArrDelayMinutes, 0.0),
    ArrivalDelayGroups = COALESCE(ArrivalDelayGroups, 0.0),
    DepartureDelayGroups = COALESCE(DepartureDelayGroups, 0.0),
    AirTime = COALESCE(AirTime, 0.0), --airtime and actualElapsed time were null whenever a flight was canceeled.
    ActualElapsedTime = COALESCE(ActualElapsedTime, 0.0);


SELECT COUNT(*) AS shapeDbo
FROM flightDbo;

SELECT * FROM flightDbo


SELECT DISTINCT Operating_Airline FROM dbo.flightDbo

--#Feature Engineering
--Create column for status
ALTER TABLE dbo.flightDbo
ADD Status NVARCHAR(50);

-- Update the Status column based on the conditions
UPDATE dbo.flightDbo
SET Status = 
    CASE 
        WHEN Cancelled = 'True' THEN 'Cancelled'
        WHEN DepDelayMinutes = 0.0 THEN 'OnTime'
        WHEN DepDelayMinutes > 0.0 AND DepDelayMinutes <= 30.0 THEN 'SlightDelay'
        WHEN DepDelayMinutes > 30.0 AND DepDelayMinutes <= 60.0 THEN 'ModerateDelay'
        WHEN DepDelayMinutes > 60.0 THEN 'HugeDelay'
    END;


SELECT DISTINCT Status FROM dbo.flightDbo


-- Pivot the data to calculate percentages for each status group within each month
SELECT Month, [OnTime], [SlightDelay], [ModerateDelay],  [HugeDelay], [Cancelled]
FROM 
    (
	SELECT Month, Status,
       (CAST(COUNT(*) AS float) / SUM(COUNT(*)) OVER (PARTITION BY Month)) * 100 AS StatusPercentage
    FROM flightDbo
    GROUP BY Month, Status
    ) AS MonthlyStatusPercentages
PIVOT (
    SUM(StatusPercentage) FOR Status IN ([OnTime], [SlightDelay], [ModerateDelay], [HugeDelay], [Cancelled])
) AS StatusPivot
ORDER BY Month;



-- Pivot the data to calculate percentages for each delay group within each airline
SELECT Operating_Airline, [OnTime], [SlightDelay], [ModerateDelay], [HugeDelay], [Cancelled]
FROM (
    SELECT Operating_Airline, Status,
           (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY Operating_Airline)) AS DelayPercentage
    FROM flightDbo
    GROUP BY Operating_Airline, Status
) AS AirlineDelayCounts
PIVOT (
    SUM(DelayPercentage) FOR Status IN ([OnTime], [SlightDelay], [ModerateDelay], [HugeDelay], [Cancelled])
) AS DelayPivot
ORDER BY Operating_Airline;


SELECT * FROM dbo.flightDbo WHERE Operating_Airline = '9E'


SELECT * FROM dbo.flightDbo

SELECT *
FROM dbo.flightDbo fv1
JOIN dbo.Airlines a  ON fv1.Operating_Airline = a.Code;


--Getting the next and previous values of DepDelayMinutes for each record in table partitioned by Operating_Airline:
SELECT 
    Operating_Airline,FlightDate,
    LAG(Status) OVER (PARTITION BY Operating_Airline ORDER BY FlightDate) AS PreviousDepDelay,
    LEAD(Status) OVER (PARTITION BY Operating_Airline ORDER BY FlightDate) AS NextDepDelay
FROM flightDbo;


--Ranking the records in table based on the DepDelayMinutes column:
SELECT 
    *,
    RANK() OVER (ORDER BY DepDelayMinutes) AS RankWithoutPartition,
    RANK() OVER (PARTITION BY Operating_Airline ORDER BY DepDelayMinutes) AS RankWithPartition
FROM flightDbo;

--If we want to pivot the data in the FlightV1 table to calculate the count of flights for each airline (Operating_Airline) and status (Status), we can use the PIVOT clause:--
SELECT *
FROM (
    SELECT Operating_Airline, Status
    FROM flightDbo
) AS SourceTable
PIVOT (
    COUNT(Operating_Airline) FOR Status IN ([OnTime], [SlightDelay], [ModerateDelay], [HugeDelay], [Cancelled])
) AS PivotTable;


SELECT * FROM dbo.flightDbo

SELECT *
FROM dbo.flightDbo
ORDER BY FlightDate;

