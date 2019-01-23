-- Stickney Hill Dairy file format.
TRUNCATE TABLE dbo.StickneyData

SELECT *
INTO #TempStickney1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE [dbo].[StickneyCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempStickney1
-------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode,
	Tank,
	right(Barcode, 3) Seq,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecNullValues](Fat) Fat,
	[dbo].[CastAsDecNullValues]([True Protein]) [True Protein],
	cast(Lactose AS DECIMAL(4, 2)) Lactose,
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
	[dbo].[AntiCharConfirmation]([Delvo Inhibitor], [Bacteria Count], TestCode) IHB,
	dbo.OfficialCodeYN(TestCode, [Delvo Inhibitor], [Bacteria Count]) OFFICIAL,
	[dbo].[CastAsDecNullValues]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	ParameterCode,
	TestCode,
	[Delvo Inhibitor],
	Coliforms,
	RouteNumber
INTO #TempStickney2
FROM #TempStickney1
WHERE len(Barcode) >= 10

--Select * from #TempStickney2
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
	max(IHB) IHB,
	max(OFFICIAL) OFFICIAL
INTO #TempStickney3
FROM #TempStickney2
GROUP BY Barcode

--select * from #TempStickney3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode PRODUCER,
	TANK,
	SEQ,
	RouteNumber [ROUTE],
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) [SAMPLE DATE],
	cast(cast(TestDate AS DATE) AS NVARCHAR(20)) [TEST DATE],
	OFFICIAL,
	cast(Fat AS NVARCHAR(10)) FAT,
	cast([True Protein] AS NVARCHAR(10)) PROTEIN,
	cast(LACTOSE AS NVARCHAR(10)) LACTOSE,
	cast([Solids not fat (SNF)] AS NVARCHAR(10)) SNF,
	cast([Other Solids] AS NVARCHAR(10)) OS,
	[Somatic cells] ESCC,
	[dbo].[RemoveCharacters]([Bacteria Count]) PLC,
	[Bacteria Count(PI)] [PI],
	cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10)) MUN,
	[dbo].[RemoveCharacters](Coliforms) COLI,
	[dbo].[RemoveCharacters]([Thermoduric Plate Count]) LPC,
	[Freezing Point] FP,
	[dbo].[Water]([Freezing Point]) [%WATER],
	IHB
INTO #TempStickney4
FROM #TempStickney3

--select * into dbo.StickneyData from #TempStickney4 -- Need to use this insert statement if the table was droped.
INSERT INTO dbo.StickneyData
SELECT *
FROM #TempStickney4

DROP TABLE #TempStickney1

DROP TABLE #TempStickney2

DROP TABLE #TempStickney3

DROP TABLE #TempStickney4

-- select ? = count(1) from dbo.StickneyData
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM StickneyData
ORDER BY producer,
	tank,
	[sample date],
	[seq]
