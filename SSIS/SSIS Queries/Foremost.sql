-- Formost file format, send file name: "dqci_yyyymmddnn"
TRUNCATE TABLE dbo.ForemostData

SELECT *
INTO #TempForemost1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE [dbo].[ForemostCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempForemost1
-------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode,
	Tank,
	right(Barcode, 3) Seq,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecNullValues](Fat) Fat,
	[dbo].[CastAsDecNullValues]([True Protein]) [True Protein],
	[dbo].[CastAsDecNullValues](Lactose) Lactose,
	[dbo].[CastAsDecNullValues]([Other Solids]) [Other Solids],
	[Somatic cells],
	SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	Barcode,
	[dbo].[OfficialCode](TestCode, [Delvo Inhibitor], [Bacteria Count]) OfficialCode,
	dbo.AntiCharConfirmation([Delvo Inhibitor], [Bacteria Count], TestCode) AntiCharConfirmation,
	[dbo].[CastAsDecNullValues]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	ParameterCode,
	TestCode,
	[Delvo Inhibitor],
	Coliforms,
	RouteNumber,
	AlternateID
INTO #TempForemost2
FROM #TempForemost1
WHERE len(Barcode) >= 10

--Select * from #TempForemost2
------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(ProducerCode) ProducerCode,
	max(Tank) Tank,
	max(Seq) Seq,
	max([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	Max(SampleDate) SampleDate,
	min(TestDate) TestDate,
	max(Fat) Fat,
	max([True Protein]) [True Protein],
	max(Lactose) Lactose,
	max([Other Solids]) [Other Solids],
	max([Somatic cells]) [Somatic cells],
	max(SamplingTemperature) SamplingTemperature,
	max([Bacteria Count]) [Bacteria Count],
	max([Freezing Point]) [Freezing Point],
	max(Sediment) Sediment,
	max([Thermoduric Plate Count]) [Thermoduric Plate Count],
	max([Bacteria Count(PI)]) [Bacteria Count(PI)],
	max([Solids not fat (SNF)]) [Solids not fat (SNF)],
	max(ParameterCode) ParameterCode,
	max([Delvo Inhibitor]) [Delvo Inhibitor],
	max(Testcode) TestCode,
	max(Coliforms) Coliforms,
	max(RouteNumber) RouteNumber,
	max(AlternateID) AlternateID,
	max(OfficialCode) OfficialCode,
	max(AntiCharConfirmation) AntiCharConfirmation
INTO #TempForemost3
FROM #TempForemost2
GROUP BY Barcode

--select * from #TempForemost3
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode,
	Tank,
	Seq,
	substring([dbo].[RemoveDecPoint]([Milk Urea Nitrogen (MUN)]), 1, 3) [Milk Urea Nitrogen (MUN)],
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(cast(nullif(TestDate, '') AS DATE) AS NVARCHAR(10)) TestDate,
	OfficialCode,
	[dbo].[RemoveDecPoint](Fat) Fat,
	[dbo].[RemoveDecPoint]([True Protein]) [True Protein],
	[dbo].[RemoveDecPoint](Lactose) Lactose,
	[dbo].[RemoveDecPoint]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	[dbo].[RemoveDecPoint]([dbo].[TotalSolids]([Solids not fat (SNF)], Fat)) TotalSolids,
	[Somatic cells],
	CASE 
		WHEN [dbo].[FormatTemp](SamplingTemperature) = 0
			THEN NULL
		ELSE [dbo].[FormatTemp](SamplingTemperature)
		END SamplingTemperature,
	[dbo].[RemoveCharacters]([Bacteria Count]) [Bacteria Count],
	[Freezing Point],
	'540' Fixed1,
	[dbo].[RemoveDecPoint]([dbo].[Water]([Freezing Point])) Water,
	AntiCharConfirmation,
	Sediment,
	'1' Fixed2,
	'DQCI' Fixed3,
	AlternateID,
	[dbo].[RemoveCharacters]([Bacteria Count(PI)]) [Bacteria Count(PI)],
	cast(cast(nullif(TestDate, '') AS TIME) AS NVARCHAR(20)) TestTime,
	'00' Fixed4,
	'' Blank1,
	'' Blank2
INTO #TempForemost4
FROM #TempForemost3

--select * into dbo.ForemostData from #TempForemost4
INSERT INTO dbo.ForemostData
SELECT *
FROM #TempForemost4

DROP TABLE #TempForemost1

DROP TABLE #TempForemost2

DROP TABLE #TempForemost3

DROP TABLE #TempForemost4

-- select ? = count(1) from dbo.ForemostData   
------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM ForemostData
WHERE len(ProducerCode) > 0
ORDER BY ProducerCode,
	Tank,
	SampleDate
