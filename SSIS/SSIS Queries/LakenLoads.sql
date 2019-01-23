-- Laken loads File Format (Davisco Loads)
SET NOCOUNT ON

TRUNCATE TABLE dbo.LakenLoadsData

SELECT *
INTO #TempLakenLoads1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[DaviscoCustomerCodes](CustomerCode, CustomerFramework) = '2'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempLakenLoads1
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
	Coliform AS Coliforms,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	CustomerFramework,
	[dbo].[CastAsDecNullValues](Casein) Casein,
	right(Barcode, 4) Seq,
	'' LoadId,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	CustomerCode,
	[dbo].[CastAsDecNullValues]([Other Solids]) [Other Solids],
	[dbo].[ComponentsTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) ComponentsTestDate,
	Barcode,
	[Delvo Inhibitor],
	ParameterCode,
	TestCode
INTO #TempLakenLoads2
FROM #TempLakenLoads1
WHERE len(Barcode) >= 10

--select * from #TempLakenLoads2
----------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(Fat) Fat,
	Max([True Protein]) [True Protein],
	max(Lactose) Lactose,
	max([Solids not fat (SNF)]) [Solids not fat (SNF)],
	Barcode,
	max(SampleDate) SampleDate,
	min(TestDate) TestDate,
	max([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	max([Other Solids]) [Other Solids],
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
	max(Casein) Casein
INTO #TempLakenLoads3
FROM #TempLakenLoads2
GROUP BY Barcode

--select * from #TempLakenLoads3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT dbo.ProducerOrLoadID(ProducerCode, Barcode, CustomerCode, CustomerFramework) Manifest#,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(Fat AS NVARCHAR(20)) Fat,
	cast([True Protein] AS NVARCHAR(20)) Protein,
	cast(Lactose AS NVARCHAR(20)) Lactose,
	[Somatic Cells] [Cells(Somatic)],
	dbo.RemoveCharacters([Bacteria Count]) BacteriaCount,
	[Freezing Point] FreezingPoint,
	cast([Other Solids] AS NVARCHAR(20)) OtherSolids,
	cast(dbo.TotalSolids([Solids not fat (SNF)], [True Protein]) AS NVARCHAR(20)) TotalSolids,
	cast(Casein AS NVARCHAR(20)) Casein,
	dbo.RemoveCharacters([Thermoduric Plate Count]) LPC
INTO #TempLakenLoads4
FROM #TempLakenLoads3

--select * from #TempLakenLoads4
------------------------------------------------------------------------------------------------
-- Use this if the table is deleted.
--select *
--into dbo.LakenLoadsData
--from #TempLakenLoads4
INSERT INTO dbo.LakenLoadsData
SELECT *
FROM #TempLakenLoads4

DROP TABLE #TempLakenLoads1

DROP TABLE #TempLakenLoads2

DROP TABLE #TempLakenLoads3

DROP TABLE #TempLakenLoads4

-- select ? = count(1) from dbo.LakenLoadsData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF

SELECT *
FROM dbo.LakenLoadsData
WHERE len(Manifest#) > 0
ORDER BY Manifest#,
	SampleDate
