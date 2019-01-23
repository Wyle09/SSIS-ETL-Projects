-- SaputoLoadsData file format.
SET NOCOUNT ON

TRUNCATE TABLE dbo.SaputoLoadsData

SELECT *
INTO #TempSaputoLoads1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Casein, [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE (
		[dbo].[SaputoCustomerCodes](CustomerCode) = '1'
		AND dbo.IsLoad(CustomerFramework) = '1'
		)
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempSaputoLoads1
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
	[dbo].[RemoveCharacters](Coliform) Coliforms,
	[dbo].[RemoveCharacters]([Thermoduric Plate Count]) [Thermoduric Plate Count],
	[dbo].[RemoveCharacters]([Bacteria Count(PI)]) [Bacteria Count(PI)],
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
	[Other Solids],
	[dbo].[AntiCharConfirmation]([Delvo Inhibitor], [Bacteria Count], TestCode) INB
INTO #TempSaputoLoads2
FROM #TempSaputoLoads1
WHERE len(Barcode) >= 10

--Select * from #TempSaputoLoads2
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
	max(QualityTestDate) QualityTestDate,
	max(INB) INB
INTO #TempSaputoLoads3
FROM #TempSaputoLoads2
GROUP BY Barcode

--select * from #TempSaputoLoads3
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT [dbo].[ProducerOrLoadID](ProducerCode, Barcode, CustomerCode, CustomerFramwork) [PRODUCER],
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) [SAMPLE DATE],
	cast(cast(TestDate AS DATE) AS NVARCHAR(50)) [TEST DATE],
	'' [OFFICIAL],
	FAT,
	[True Protein] PROTEIN,
	Lactose [LAC],
	[Solids not fat (SNF)] [SNF],
	[Other Solids] [OS],
	[Somatic Cells] [ESCC],
	[Bacteria Count] PLC,
	[dbo].[FreezePointDec]([Freezing Point], '-') FP,
	CASE 
		WHEN [dbo].[Water]([Freezing Point]) IS NULL
			OR [dbo].[Water]([Freezing Point]) = ''
			THEN '<0.1%'
		ELSE [dbo].[Water]([Freezing Point])
		END [%WATER],
	INB
INTO #TempSaputoLoads4
FROM #TempSaputoLoads3

INSERT INTO dbo.SaputoLoadsData
SELECT *
FROM #TempSaputoLoads4

--select * into dbo.SaputoLoadsData from #TempSaputoLoads4
DROP TABLE #TempSaputoLoads1

DROP TABLE #TempSaputoLoads2

DROP TABLE #TempSaputoLoads3

DROP TABLE #TempSaputoLoads4

--select ? = count(1)  from dbo.SaputoLoadsData
--------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF

SELECT *
FROM dbo.SaputoLoadsData
ORDER BY PRODUCER,
	[SAMPLE DATE]
