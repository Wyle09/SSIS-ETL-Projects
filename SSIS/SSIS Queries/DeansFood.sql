-- Deans food file format.
SET NOCOUNT ON

TRUNCATE TABLE dbo.DeansData

SELECT *
INTO #TempDeans1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE [dbo].[DeansCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

-------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode,
	Tank,
	right(Barcode, 3) Seq,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecNullValues](Fat) Fat,
	[dbo].[CastAsDecNullValues]([True Protein]) [True Protein],
	[dbo].[CastAsDecNullValues](Lactose) Lactose,
	[dbo].[CastAsDecNullValues]([Other Solids]) [Other Solids],
	[Somatic cells],
	SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	Barcode,
	[dbo].[OfficialCode](TestCode, [Delvo Inhibitor], [Bacteria Count]) OfficialCode,
	[dbo].[CastAsDecNullValues]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	ParameterCode,
	TestCode,
	[Delvo Inhibitor],
	Coliform as Coliforms,
	RouteNumber,
	Division,
	[dbo].[AntiCharConfirmationNP]([Delvo Inhibitor]) INH
INTO #TempDeans2
FROM #TempDeans1
WHERE len(Barcode) >= 10

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
	max(RouteNumber) RouteNumber,
	max(Division) Division,
	max(INH) INH
INTO #TempDeans3
FROM #TempDeans2
GROUP BY Barcode

--------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT Division,
	ProducerCode,
	Tank,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(cast(nullif(TestDate, '') AS DATE) AS NVARCHAR(20)) TestDate,
	cast(Fat AS NVARCHAR(10)) Fat,
	cast([True Protein] AS NVARCHAR(10)) [True Protein],
	[Somatic cells],
	cast(Lactose AS NVARCHAR(10)) Lactose,
	cast([dbo].[TotalSolids]([Solids not fat (SNF)], Fat) AS NVARCHAR(10)) TotalSolids,
	cast([Solids not fat (SNF)] AS NVARCHAR(10)) [Solids not fat (SNF)],
	cast([Other Solids] AS NVARCHAR(10)) [Other Solids],
	cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10)) [Milk Urea Nitrogen (MUN)],
	CASE 
		WHEN [dbo].[FormatTemp](SamplingTemperature) = 0
			THEN NULL
		ELSE [dbo].[FormatTemp](SamplingTemperature)
		END SamplingTemperature,
	[dbo].[RemoveCharacters]([Bacteria Count]) [Bacteria Count],
	[Freezing Point],
	INH, --[dbo].[AntiCharConfirmationNP]([Delvo Inhibitor], ParameterCode) INH,
	[dbo].[RemoveCharacters]([Bacteria Count(PI)]) [PI],
	[dbo].[RemoveCharacters]([Thermoduric Plate Count]) LPC,
	CASE 
		WHEN OfficialCode != '99'
			THEN '00'
		ELSE OfficialCode
		END OfficialCode,
	'0' Fixed1,
	'' Blank,
	Seq
INTO #TempDeans4
FROM #TempDeans3

--select * into dbo.DeansData from #TempDeans4 -- Need to use this insert statement if the table was droped.
INSERT INTO dbo.DeansData
SELECT *
FROM #TempDeans4

DROP TABLE #TempDeans1

DROP TABLE #TempDeans2

DROP TABLE #TempDeans3

DROP TABLE #TempDeans4

-- select ? = count(1) from dbo.DeansData
-----------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF

SELECT *
FROM DeansData
WHERE len(ProducerCode) > 0
ORDER BY ProducerCode,
	Tank,
	SampleDate,
	Seq
