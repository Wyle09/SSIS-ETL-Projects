--Valley Milk Fresno producers.
SET NOCOUNT ON

TRUNCATE TABLE dbo.ValleyMilkData

SELECT *
INTO #ValleyMilk1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [Total Protein], [True Protein], Coliform, [Bulk Tank Coliforms], [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[ValleyMilkCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #ValleyMilk1
---------------------------------------------------------------------------------------------------------------------------------------------
SELECT Company,
	Division,
	ProducerCode,
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecNullValues](Fat) Fat,
	[dbo].[CastAsDecNullValues](coalesce([Total Protein], [True Protein])) [Total Protein],
	[dbo].[CastAsDecNullValues](Lactose) Lactose,
	[dbo].[CastAsDecNullValues]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	[Somatic cells],
	SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	Tank,
	coalesce(Coliform, [Bulk Tank Coliforms]) AS Coliforms,
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
	[dbo].[GeoMeanFlag](TestCode, [Delvo Inhibitor], [Bacteria Count]) FixedGeo
INTO #ValleyMilk2
FROM #ValleyMilk1
WHERE len(Barcode) >= 8

--select * from #ValleyMilk2
----------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(Fat) Fat,
	Max([Total Protein]) [Total Protein],
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
	max(FixedGeo) FixedGeo
INTO #ValleyMilk3
FROM #ValleyMilk2
GROUP BY Barcode

--select * from #ValleyMilk3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT BARCODE,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) [DATE],
	FAT,
	[Total Protein] PROTEIN,
	[Solids not fat (SNF)] SNF,
	LACTOSE,
	dbo.Times1000([Somatic Cells]) SCC,
	CASE 
		WHEN cast([Thermoduric Plate Count] AS INT) = 0
			THEN '<10EST'
		ELSE dbo.Times10(cast([Thermoduric Plate Count] AS INT))
		END LPC,
	CASE 
		WHEN cast(Coliforms AS INT) = 0
			THEN '<10EST'
		WHEN cast(Coliforms AS INT) > 160
			THEN '>1500EST'
		ELSE dbo.Times10(CAST(Coliforms AS INT))
		END COLI,
	dbo.Times1000([Bacteria Count]) SPC,
	'' TATS,
	'' FSS
INTO #ValleyMilk4
FROM #ValleyMilk3

-- Use this if the table is deleted.
-- SELECT *
-- INTO dbo.ValleyMilkData
-- FROM #ValleyMilk4 
INSERT INTO dbo.ValleyMilkData
SELECT *
FROM #ValleyMilk4

DROP TABLE #ValleyMilk1

DROP TABLE #ValleyMilk2

DROP TABLE #ValleyMilk3

DROP TABLE #ValleyMilk4

-- select ? = count(1) from dbo.ValleyMilkData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF

SELECT *
FROM dbo.ValleyMilkData
WHERE len(Barcode) > 0
ORDER BY Barcode,
	[Date]
