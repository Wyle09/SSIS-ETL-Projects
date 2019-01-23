-- Lesueur loads File Format (Davisco Loads)
SET NOCOUNT ON

TRUNCATE TABLE dbo.LesueurLoadsData

SELECT *
INTO #TempLesueurLoads1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[DaviscoCustomerCodes](CustomerCode, CustomerFramework) = '3'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempLesueurLoads1
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
INTO #TempLesueurLoads2
FROM #TempLesueurLoads1
WHERE len(Barcode) >= 10

--select * from #TempLesueurLoads2
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
INTO #TempLesueurLoads3
FROM #TempLesueurLoads2
GROUP BY Barcode

--select * from #TempLesueurLoads3
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
INTO #TempLesueurLoads4
FROM #TempLesueurLoads3

--select *
--into dbo.LesueurLoadsData
--from #TempLesueurLoads4
INSERT INTO dbo.LesueurLoadsData
SELECT *
FROM #TempLesueurLoads4

DROP TABLE #TempLesueurLoads1

DROP TABLE #TempLesueurLoads2

DROP TABLE #TempLesueurLoads3

DROP TABLE #TempLesueurLoads4

-- select ? = count(1) from dbo.LesueurLoadsData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF

SELECT *
FROM dbo.LesueurLoadsData
WHERE len(Manifest#) > 0
ORDER BY Manifest#,
	SampleDate
