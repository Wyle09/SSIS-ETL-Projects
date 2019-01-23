-- LOL file format.
SET NOCOUNT OFF

TRUNCATE TABLE dbo.LOLdata

SELECT *
INTO #TempLOL1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliform, [Bulk Tank Coliforms], [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE [dbo].[LOLCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempLOL
-------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode,
	Tank,
	substring(Barcode, 9, 3) Seq,
	[dbo].[CastAsDecLOL]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecLOL](Fat) Fat,
	[dbo].[CastAsDecLOL]([True Protein]) [True Protein],
	[dbo].[CastAsDecLOL](Lactose) Lactose,
	[dbo].[CastAsDecLOL]([Other Solids]) [Other Solids],
	[Somatic cells],
	SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	Barcode,
	[dbo].[OfficialCode](TestCode, [Delvo Inhibitor], [Bacteria Count]) OfficialCode,
	[dbo].[CastAsDecLOL]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	ParameterCode,
	TestCode,
	[Delvo Inhibitor],
	coalesce(Coliform, [Bulk Tank Coliforms]) AS Coliforms,
	CustomerFramework,
	CustomerCode,
	[dbo].[AntiCharConfirmation]([Delvo Inhibitor], [Bacteria Count], TestCode) AntiCharConfirmation
INTO #TempLOL2
FROM #TempLOL1
WHERE len(Barcode) >= 10

--Select * from #TempLOL2
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
	Max(OfficialCode) OfficialCode,
	max([Solids not fat (SNF)]) [Solids not fat (SNF)],
	max(ParameterCode) ParameterCode,
	max([Delvo Inhibitor]) [Delvo Inhibitor],
	max(Testcode) TestCode,
	max(Coliforms) Coliforms,
	Barcode,
	max(CustomerFrameWork) CustomerFrameWork,
	max(CustomerCode) CustomerCode,
	max(AntiCharConfirmation) AntiCharConfirmation
INTO #TempLOL3
FROM #TempLOL2
GROUP BY Barcode

--select * from #TempLOL3
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT CASE 
		WHEN dbo.IsLoad(CustomerFrameWork) = '1'
			THEN [dbo].[PadRight10]([dbo].[ProducerOrLoadID](ProducerCode, Barcode, CustomerCode, CustomerFramework))
		ELSE [dbo].[PadRight6]([dbo].[ProducerOrLoadID](ProducerCode, Barcode, CustomerCode, CustomerFramework))
		END ProducerCode,
	CASE 
		WHEN dbo.IsLoad(CustomerFrameWork) = '1'
			THEN ''
		ELSE dbo.PassZeroIfNull(Tank)
		END Tank,
	CASE 
		WHEN [dbo].[IsLoad](CustomerFrameWork) = '1'
			THEN ''
		ELSE [dbo].[PadRight3]([dbo].[PassZeroIfNull](Seq))
		END Seq,
	CASE 
		WHEN dbo.IsLoad(CustomerFramework) != '1'
			THEN right(Barcode, 1)
		ELSE '2'
		END Fixed1,
	dbo.PadRight4(replace(cast([Milk Urea Nitrogen (MUN)] AS DECIMAL(4, 1)), '.', '')) [Milk Urea Nitrogen (MUN)],
	'0000' Fixed2,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(cast(nullif(TestDate, '') AS DATE) AS NVARCHAR(20)) TestDate,
	CASE 
		WHEN OfficialCode IS NULL
			OR OfficialCode != 99
			THEN '  ' -- Need 2 spaces if null
		ELSE OfficialCode
		END OfficialCode,
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint](Fat))), 4) Fat,
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint]([True Protein]))), 4) [True Protein],
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint](Lactose))), 4) Lactose,
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint]([Solids not fat (SNF)]))), 4) [Solids not fat (SNF)],
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint]([dbo].[TotalSolids]([Solids not fat (SNF)], Fat)))), 4) [Total Solids],
	[dbo].[LOLFieldLen]([dbo].[PadRight5]([dbo].[PassZeroIfNull]([Somatic cells])), 5) [Somatic Cells],
	[dbo].[LOLFieldLen]([dbo].[PadRight2]([dbo].[PassZeroIfNull]([dbo].[FormatTemp](SamplingTemperature))), 2) SamplingTemperature,
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveCharacters]([Bacteria Count]))), 4) [Bacteria Count],
	[dbo].[LOLFieldLen]([dbo].[PadRight3]([dbo].[PassZeroIfNull]([Freezing Point])), 3) [Freezing Point],
	'540' [Base FP],
	[dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint]([dbo].[Water]([Freezing Point])))) Water,
	CASE 
		WHEN AntiCharConfirmation = 'NT'
			OR AntiCharConfirmation IS NULL
			OR AntiCharConfirmation NOT IN ('NF', 'AL')
			THEN '  '
		ELSE AntiCharConfirmation
		END AntiCharConfirmation,
	'0' Sediment,
	' ' Blank1,
	' ' Blank2,
	[dbo].[TestTypeIndicator](ParameterCode, [dbo].[RemoveCharacters]([Bacteria Count])) RecordIndicator,
	'DQCI' Fixed3,
	[dbo].[PadRight6]([dbo].[PassZeroIfNull]([dbo].[RemoveCharacters](Coliforms))) Coliforms,
	[dbo].[PadRight6]([dbo].[PassZeroIfNull]([dbo].[RemoveCharacters]([Thermoduric Plate Count]))) [Thermoduric Plate Count],
	[dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveCharacters]([Bacteria Count(PI)]))) [Bacteria Count(PI)],
	'0000' DMCFixed,
	' ' Blank3
INTO #TempLOL4
FROM #TempLOL3

--select * from #TempLOL4
--select * into dbo.LOLData from #TempLOL4 -- Need to use this insert statement if the table was droped.
INSERT INTO dbo.LOLData
SELECT *
FROM #TempLOL4

DROP TABLE #TempLOL1

DROP TABLE #TempLOL2

DROP TABLE #TempLOL3

DROP TABLE #TempLOL4

-- SELECT ? = count(1)
-- FROM dbo.LOLdata
-----------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT ON

SELECT *
FROM LOLData
WHERE len(ProducerCode) > 0
ORDER BY ProducerCode,
	Tank,
	Sampledate
