-- MarigoldData file format.
TRUNCATE TABLE dbo.MarigoldData

SELECT *
INTO #TempMarigold1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Casein, [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE [dbo].[MarigoldCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempMarigold1
-------------------------------------------------------------------------------------------------------------------------------------------------------
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
	[dbo].[FormatTemp](SamplingTemperature) SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	Tank,
	Coliforms,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	CustomerFramework,
	right(Barcode, 4) Seq,
	'' LoadId,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	CustomerCode,
	[dbo].[ComponentsTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) ComponentsTestDate,
	Casein,
	Barcode,
	[Delvo Inhibitor],
	ParameterCode,
	TestCode,
	[dbo].[QualityTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) QualityTestDate,
	[dbo].[CastAsDecNullValues]([Other Solids]) [Other Solids]
INTO #TempMarigold2
FROM #TempMarigold1
WHERE len(Barcode) >= 10

--Select * from #TempMarigold2
------------------------------------------------------------------------------------------------------------------------------------------------------------
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
	max(CustomerFramework) CustomerFramwork,
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
	max(QualityTestDate) QualityTestDate
INTO #TempMarigold3
FROM #TempMarigold2
GROUP BY Barcode

--select * from #TempMarigold3
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT CustomerCode,
	[dbo].[ProducerOrLoadID](ProducerCode, Barcode, CustomerCode, CustomerFramwork) [ProducerCode],
	'' Blank1,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(cast(nullif(TestDate, '') AS DATE) AS NVARCHAR(50)) TestDate,
	'' Blank2,
	[dbo].[PadRight4](Fat) Fat,
	[dbo].[PadRight4]([True Protein]) [True Protein],
	[dbo].[PadRight4]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	[dbo].[PadRight4]([Other Solids]) [Other Solids],
	[Somatic Cells]
INTO #TempMarigold4
FROM #TempMarigold3

--select * from #TempMarigold4
INSERT INTO dbo.MarigoldData
SELECT *
FROM #TempMarigold4

--select * into dbo.MarigoldData from #TempMarigold4
DROP TABLE #TempMarigold1

DROP TABLE #TempMarigold2

DROP TABLE #TempMarigold3

DROP TABLE #TempMarigold4

--select ? = count(1) from dbo.MarigoldData
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM dbo.MarigoldData
WHERE len(ProducerCode) > 0
ORDER BY ProducerCode,
	SampleDate,
	TestDate
