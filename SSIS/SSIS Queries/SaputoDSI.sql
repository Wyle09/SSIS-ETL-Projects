-- Saputo DSI File Format
SET NOCOUNT ON

TRUNCATE TABLE dbo.SaputoData

SELECT *
INTO #TempSaputo1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], [Total Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[SaputoCustomerCodes](CustomerCode) = '1'
	AND dbo.IsLoad(CustomerFramework) != '1' -- Prevent the loads from sending to the DSI format.
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempSaputo1
---------------------------------------------------------------------------------------------------------------------------------------------
SELECT Company,
	Division,
	ProducerCode,
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecNullValues](Fat) Fat,
	[dbo].[CastAsDecNullValues](coalesce([True Protein], [Total Protein])) [True Protein],
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
	CASE 
		WHEN (
				len(barcode) = 10
				AND Barcode LIKE '750%'
				)
			THEN '0000'
		ELSE right(Barcode, 4)
		END Seq,
	'' LoadId,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	CustomerCode,
	[dbo].[ComponentsTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) ComponentsTestDate,
	Barcode,
	[Delvo Inhibitor],
	ParameterCode,
	TestCode,
	[dbo].[OfficialCode](TestCode, [Delvo Inhibitor], [Bacteria Count]) OfficialCode,
	[dbo].[AntiNumConfirmation]([Delvo Inhibitor]) AntiNumConfirmation
INTO #TempSaputo2
FROM #TempSaputo1
WHERE len(Barcode) >= 10

--select * from #TempSaputo2
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
	max(AntiNumConfirmation) AntiNumConfirmation
INTO #TempSaputo3
FROM #TempSaputo2
GROUP BY Barcode

--select * from #TempSaputo3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT CASE 
		WHEN dbo.PadRight2(Company) = '00'
			THEN ''
		ELSE dbo.PadRight2(Company)
		END Company,
	CASE 
		WHEN dbo.PadRight3(Division) = '000'
			THEN ''
		ELSE dbo.PadRight3(Division)
		END Division,
	dbo.ProducerOrLoadId(ProducerCode, Barcode, CustomerCode, CustomerFramework) ProducerCode,
	'' IsLoad,
	'0' Fixed1,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(Cast(nullif(TestDate, '') AS DATE) AS NVARCHAR(20)) TestDate,
	OfficialCode,
	[dbo].[RemoveDecPoint](Fat) Fat,
	[dbo].[RemoveDecPoint]([True Protein]) [True Protein],
	[dbo].[RemoveDecPoint](Lactose) Lactose,
	[dbo].[RemoveDecPoint]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	[dbo].[RemoveDecPoint](dbo.TotalSolids([Solids not fat (SNF)], Fat)) TotalSolids,
	[Somatic Cells],
	CASE 
		WHEN [dbo].[FormatTemp](SamplingTemperature) = '0'
			THEN NULL
		ELSE [dbo].[FormatTemp](SamplingTemperature)
		END SamplingTemperature,
	[dbo].[RemoveCharacters]([Bacteria Count]) [Bacteria Count],
	[Freezing Point],
	[dbo].[Water]([Freezing Point]) Water,
	'' Fixed2,
	'' Fixed3,
	Sediment,
	'' Fixed4,
	CASE 
		WHEN dbo.IsLoad(CustomerFrameWork) = '1'
			THEN '0'
		ELSE Tank
		END Tank,
	'' Fixed5,
	'DQCI' Fixed6,
	[dbo].[RemoveCharacters](Coliforms) Coliforms,
	[dbo].[RemoveCharacters]([Thermoduric Plate Count]) [Thermoduric Plate Count],
	[dbo].[RemoveCharacters]([Bacteria Count(PI)]) [Bacteria Count(PI)],
	'' Fixed7,
	Seq,
	LoadId,
	[dbo].[RemoveDecPoint]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	AntiNumConfirmation,
	'0' Fixed8,
	cast(cast(nullif(ComponentsTestDate, '') AS DATE) AS NVARCHAR(20)) ComponentsTestDate,
	'0' Fixed9,
	'' Fixed10,
	'' Fixed11,
	'' Fixed12,
	'' Fixed13,
	'' Fixed14,
	[dbo].[RemoveDecPoint](Casein) Casein
INTO #TempSaputo4
FROM #TempSaputo3

--select * into dbo.SaputoData from #TempSaputo4 -- Use this if the table is deleted.
INSERT INTO dbo.SaputoData
SELECT *
FROM #TempSaputo4

DROP TABLE #TempSaputo1

DROP TABLE #TempSaputo2

DROP TABLE #TempSaputo3

DROP TABLE #TempSaputo4

-- select ? = count(1) from SaputoData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT ON

SELECT [Company],
	[Division],
	[ProducerCode],
	[IsLoad],
	[Fixed1],
	[SampleDate],
	[TestDate],
	[OfficialCode],
	[Fat],
	[True Protein],
	[Lactose],
	[Solids not fat (SNF)],
	[TotalSolids],
	[Somatic Cells],
	[SamplingTemperature],
	[Bacteria Count],
	[Freezing Point],
	[Water],
	[Fixed2],
	[Fixed3],
	[Sediment],
	[Fixed4],
	[Tank],
	[Fixed5],
	[Fixed6],
	[Coliforms],
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	[Fixed7],
	[Seq],
	[LoadId],
	[Milk Urea Nitrogen (MUN)],
	[AntiNumConfirmation],
	[Fixed8],
	[ComponentsTestDate],
	[Fixed9],
	[Fixed10]
FROM [eLimsMilkReportExports].[dbo].[SaputoData]
WHERE len(ProducerCode) > 0
	AND (
		len(Company) > 0
		AND len(Division) > 0
		)
ORDER BY ProducerCode,
	Tank,
	SampleDate
