-- Crop File Format for Producers.
--Remove Previous data.
TRUNCATE TABLE dbo.CropData

SELECT *
INTO #TempCrop1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE (
		[dbo].[CropCustomerCodes](CustomerCode) = '1'
		AND dbo.IsLoad(CustomerFramework) != '1'
		)
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempCrop1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
	dbo.AntiCharConfirmation([Delvo Inhibitor], [Bacteria Count], TestCode) AntiConfirmation,
	dbo.OfficialCodeYN(TestCode, [Delvo Inhibitor], [Bacteria Count]) OfficialCodeYN
INTO #TempCrop2
FROM #TempCrop1
WHERE len(Barcode) >= 10

--select * from #TempCrop2
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
	max(QualityTestDate) QualityTestDate,
	max(AntiConfirmation) AntiConfirmation,
	max(OfficialCodeYN) IsOfficialYN
INTO #TempCrop3
FROM #TempCrop2
GROUP BY Barcode

--select * from #TempCrop3
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode,
	Tank,
	Seq,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(cast(nullif(TestDate, '') AS DATE) AS NVARCHAR(10)) TestDate,
	IsOfficialYN,
	CAST(Fat AS NVARCHAR(10)) Fat,
	cast([True Protein] AS NVARCHAR(10)) [True Protein],
	cast(Lactose AS NVARCHAR(10)) Lactose,
	cast([Solids not fat (SNF)] AS NVARCHAR(10)) [Solids not fat (SNF)],
	cast([Somatic Cells] AS NVARCHAR(10)) [Somatic Cells],
	[dbo].[RemoveCharacters]([Bacteria Count]) [Bacteria Count],
	[dbo].[RemoveCharacters]([Bacteria Count(PI)]) [Bacteria Count(PI)],
	cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10)) [Milk Urea Nitrogen (MUN)],
	[dbo].[RemoveCharacters](Coliforms) Coliforms,
	dbo.RemoveCharacters([Thermoduric Plate Count]) [Thermoduric Plate Count],
	[Freezing Point],
	AntiConfirmation
INTO #TempCrop4
FROM #TempCrop3

--select * into dbo.CropData from #TempCrop4
INSERT INTO dbo.CropData
SELECT *
FROM #TempCrop4

DROP TABLE #TempCrop1

DROP TABLE #TempCrop2

DROP TABLE #TempCrop3

DROP TABLE #TempCrop4

--select ? = count(1)  from dbo.CropData
---------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM dbo.CropData
WHERE len(ProducerCode) > 0
ORDER BY ProducerCode,
	Tank,
	SampleDate
