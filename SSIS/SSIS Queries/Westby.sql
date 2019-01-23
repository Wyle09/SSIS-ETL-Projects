--Westby file format.
-- Scenic DSI File Format
TRUNCATE TABLE dbo.WestbyData

SELECT *
INTO #TempWestby1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[WestbyCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempWestby1
---------------------------------------------------------------------------------------------------------------------------------------------
SELECT Company,
	Division,
	ProducerCode,
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecNullValues](Fat) Fat,
	[dbo].[CastAsDecNullValues]([True Protein]) [True Protein],
	[dbo].[CastAsDecNullValues](Lactose) Lactose,
	[dbo].[CastAsDecNullValues]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	[Somatic cells],
	SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	Tank,
	Coliforms,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	CustomerFramework,
	[dbo].[CastAsDecNullValues](Casein) Casein,
	right(Barcode, 4) Seq,
	'' LoadId,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	CustomerCode,
	[dbo].[ComponentsTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) ComponentsTestDate,
	Barcode,
	[Delvo Inhibitor],
	ParameterCode,
	TestCode,
	[dbo].[OfficialCode](TestCode, [Delvo Inhibitor], [Bacteria Count]) OfficialCode,
	[dbo].[AntiNumConfirmation]([Delvo Inhibitor]) AntiNumConfirmation,
	BTUNumber,
	RegistrationTime,
	ProducerGrade,
	[Other Solids],
	SampleID
INTO #TempWestby2
FROM #TempWestby1
WHERE len(Barcode) >= 10

--select * from #TempWestby2
----------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(Fat) Fat,
	Max([True Protein]) [True Protein],
	max(Lactose) Lactose,
	max([Solids not fat (SNF)]) [Solids not fat (SNF)],
	Barcode,
	max(SampleDate) SampleDate,
	min(TestDate) TestDate,
	max([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	max(LoadId) LoadId,
	max(Tank) Tank,
	max(Seq) Seq,
	max(CustomerCode) CustomerCode,
	max(CustomerFramework) CustomerFramework,
	max([Somatic Cells]) [Somatic Cells],
	max(ComponentsTestDate) ComponentsTestDate,
	max([Bacteria Count]) [Bacteria Count],
	max([Bacteria Count(PI)]) [Bacteria Count(PI)],
	max(SamplingTemperature) SamplingTemperature,
	max([Freezing Point]) [Freezing Point],
	max(Company) Company,
	max(division) Division,
	max(ProducerCode) ProducerCode,
	max(Sediment) Sediment,
	max(Coliforms) Coliforms,
	max([Thermoduric Plate Count]) [Thermoduric Plate Count],
	max([Delvo Inhibitor]) [Delvo Inhibitor],
	max(ParameterCode) ParameterCode,
	max(TestCode) TestCode,
	max(Casein) Casein,
	max(OfficialCode) OfficialCode,
	max(AntiNumConfirmation) AntiNumConfirmation,
	max(BtuNumber) BtuNumber,
	max(RegistrationTime) RegistrationTime,
	max(ProducerGrade) Grade,
	max([Other Solids]) [Other Solids],
	max(SampleID) SampleID
INTO #TempWestby3
FROM #TempWestby2
GROUP BY Barcode

--select * from #TempWestby3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT BtuNumber [Producer Plant],
	ProducerCode [Producer No],
	Tank [Unit Id],
	Grade,
	cast(cast(RegistrationTime AS DATETIME2) AS NVARCHAR(50)) [Record Date],
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) [Sample Date],
	cast(cast(Testdate AS DATETIME2) AS NVARCHAR(50)) [TimeTested],
	CASE 
		WHEN [dbo].[FormatTemp](SamplingTemperature) = '0'
			THEN NULL
		ELSE [dbo].[FormatTemp](SamplingTemperature)
		END SampleTemperature,
	Fat BF,
	[True Protein] PT,
	Lactose LT,
	[Other Solids] OS,
	dbo.TotalSolids([Solids not fat (SNF)], Fat) TS,
	[Somatic Cells] SC,
	cast(round(cast(nullif([Somatic Cells], '') AS INT), 1) AS NVARCHAR(20)) OSSC,
	[Bacteria Count] PC,
	[Bacteria Count] PLC,
	CASE 
		WHEN [dbo].[AntiCharConfirmation]([Delvo Inhibitor], [Bacteria Count], TestCode) NOT IN ('AL', 'NF')
			THEN 'FALSE'
		WHEN [dbo].[AntiCharConfirmation]([Delvo Inhibitor], [Bacteria Count], TestCode) IN ('AL', 'NF')
			THEN 'TRUE'
		END IsAntibioticTested,
	CASE 
		WHEN [dbo].[AntiCharConfirmation]([Delvo Inhibitor], [Bacteria Count], TestCode) = 'AL'
			THEN 'TRUE'
		ELSE 'FALSE'
		END IsAntibioticPos,
	[Freezing Point] Cryoscope,
	Sediment,
	[Bacteria Count(PI)] [PI],
	Casein CS,
	[Milk Urea Nitrogen (MUN)] MUN,
	CASE 
		WHEN [dbo].[OfficialCode](TestCode, [Delvo Inhibitor], [Bacteria Count]) = '99'
			THEN 'TRUE'
		ELSE 'FALSE'
		END IsOfficial,
	'27-B-00134' [Lab Id],
	SampleID [Lab Test Id],
	'FALSE' IsInvalid
INTO #TempWestby4
FROM #TempWestby3

--SELECT *
--INTO dbo.WestbyData
--FROM #TempWestby4 -- Use this if the table is deleted. 
INSERT INTO dbo.WestbyData
SELECT *
FROM #TempWestby4

DROP TABLE #TempWestby1

DROP TABLE #TempWestby2

DROP TABLE #TempWestby3

DROP TABLE #TempWestby4

--select ?=count(1) from dbo.WestbyData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM dbo.WestbyData
ORDER BY [Producer No],
	[Unit Id],
	[Sample Date]
