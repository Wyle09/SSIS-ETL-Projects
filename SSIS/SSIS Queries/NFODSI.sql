--NFO DSI File Format
TRUNCATE TABLE dbo.NFOData

-- Remove previous data.
SELECT *
INTO #TempNFO1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE (
		[dbo].[NFOCustomerCodes](CustomerCode, CustomerFramework) = '1'
		AND RowStatus = 0
		)
	OR (
		[dbo].[NFOCustomerCodes](CustomerCode, CustomerFramework) = '2'
		AND RowStatus = 1
		)
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempNFO1
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
	Coliforms,
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
	[dbo].[AntiNumConfirmation]([Delvo Inhibitor]) AntiNumConfirmation
INTO #TempNFO2
FROM #TempNFO1
WHERE len(Barcode) >= 10

--select * from #TempNFO2
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
INTO #TempNFO3
FROM #TempNFO2
GROUP BY Barcode

--select * from #TempNFO3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT dbo.PadRight2(Company) Company,
	dbo.PadRight3(Division) Division,
	dbo.ProducerOrLoadId(ProducerCode, Barcode, CustomerCode, CustomerFramework) ProducerCode,
	[dbo].IsLoad(CustomerFramework) IsLoad,
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
INTO #TempNFO4
FROM #TempNFO3

--select * into dbo.NFOData from #TempNFO4 -- Use this if the table is deleted.
INSERT INTO dbo.NFOData
SELECT *
FROM #TempNFO4

DROP TABLE #TempNFO1

DROP TABLE #TempNFO2

DROP TABLE #TempNFO3

DROP TABLE #TempNFO4

-- select ? = count(1) from dbo.NFOData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
FROM [eLimsMilkReportExports].[dbo].[NFOData]
WHERE len(ProducerCode) > 0
ORDER BY ProducerCode,
	Tank,
	SampleDate
