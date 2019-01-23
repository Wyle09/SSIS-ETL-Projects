-- Crop File Format for loads.
-- Remove previous data.
TRUNCATE TABLE dbo.CropLoadData

SELECT *
INTO #TempCropLoad1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE (
		[dbo].[CropCustomerCodes](CustomerCode) = '1'
		AND dbo.IsLoad(CustomerFramework) = '1'
		)
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempCropLoad1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT Company,
	Division,
	dbo.ProducerOrLoadId(ProducerCode, Barcode, CustomerCode, CustomerFramework) ProducerCode,
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
	[dbo].[QualityTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) QualityTestDate
INTO #TempCropLoad2
FROM #TempCropLoad1
WHERE len(Barcode) >= 10

--select * from #TempCropLoad2
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
INTO #TempCropLoad3
FROM #TempCropLoad2
GROUP BY Barcode

--select * from #TempCropLoad3
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode,
	'' Blank1,
	Seq,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(cast(nullif(TestDate, '') AS DATE) AS NVARCHAR(10)) TestDate,
	'' Blank2,
	CAST(Fat AS NVARCHAR(10)) Fat,
	cast([True Protein] AS NVARCHAR(10)) [True Protein],
	cast(Lactose AS NVARCHAR(10)) Lactose,
	cast([Solids not fat (SNF)] AS NVARCHAR(10)) [Solids not fat (SNF)],
	cast([Somatic Cells] AS NVARCHAR(10)) [Somatic Cells],
	[dbo].[RemoveCharacters]([Bacteria Count]) [Bacteria Count],
	[dbo].[RemoveCharacters]([Bacteria Count(PI)]) [Bacteria Count(PI)],
	[Freezing Point],
	'' Blank3,
	'' Blank4,
	'' Blank5,
	'' Blank6
INTO #TempCropLoad4
FROM #TempCropLoad3

--select * into dbo.CropLoadData from #TempCropLoad4
INSERT INTO dbo.CropLoadData
SELECT *
FROM #TempCropLoad4

DROP TABLE #TempCropLoad1

DROP TABLE #TempCropLoad2

DROP TABLE #TempCropLoad3

DROP TABLE #TempCropLoad4

--select ? = count(1)  from dbo.CropLoadData
---------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM dbo.CropLoadData
WHERE len(ProducerCode) > 0
ORDER BY ProducerCode,
	SampleDate,
	Seq
