-- Horizon Organic file format:
TRUNCATE TABLE dbo.HorizonOrganicData

SELECT *
INTO #TempHorizonOrganic1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE [dbo].[HorizonCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

-- Check for RowStatus 1 for Producers that has been processed by DFA package.
--select * from #TempHorizonOrganic1
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
	Coliforms,
	RouteNumber,
	AlternateID,
	ProducerName,
	[dbo].[AntiCharConfirmation]([Delvo Inhibitor], [Bacteria Count], TestCode) AntiCharConfirmation
INTO #TempHorizonOrganic2
FROM #TempHorizonOrganic1
WHERE len(Barcode) >= 10

--Select * from #TempHorizonOrganic2
------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(ProducerCode) ProducerCode,
	max(Tank) Tank,
	max(Seq) Seq,
	max([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	Max(SampleDate) SampleDate,
	Min(TestDate) TestDate,
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
	max(AlternateID) AlternateID,
	max(ProducerName) ProducerName,
	max(AntiCharConfirmation) AntiCharConfirmation
INTO #TempHorizonOrganic3
FROM #TempHorizonOrganic2
GROUP BY Barcode

--select * from #TempHorizonOrganic3
----------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode [Member #],
	Tank [Location],
	ProducerName [Member Name],
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) [Sample Date],
	'' SCC,
	'' OSCC,
	[dbo].[Times1000]([Somatic cells]) ESCC,
	[dbo].[Times1000]([dbo].[RemoveCharacters]([Bacteria Count])) PLC,
	[dbo].[Times1000]([dbo].[RemoveCharacters]([Bacteria Count])) [PLC ACT],
	'' SPC,
	'' [SPC ACT],
	[dbo].[Times1000]([dbo].[RemoveCharacters]([Bacteria Count(PI)])) [PI],
	[dbo].[RemoveCharacters]([Thermoduric Plate Count]) LPC,
	[dbo].[Times10]([dbo].[RemoveCharacters](Coliforms)) COLI,
	'' [E COLI],
	[dbo].[FreezePointDec]([Freezing Point], '-') CRYO,
	'' DMC,
	cast(Fat AS NVARCHAR(10)) FAT,
	cast([True Protein] AS NVARCHAR(10)) PROT,
	cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10)) MUN,
	cast([Solids not fat (SNF)] AS NVARCHAR(10)) SNF,
	Sediment SEDI,
	CASE 
		WHEN AntiCharConfirmation = 'AL'
			THEN 'POS'
		ELSE 'NF'
		END GI,
	CASE 
		WHEN [dbo].[FormatTemp](SamplingTemperature) = '0'
			THEN NULL
		ELSE [dbo].[FormatTemp](SamplingTemperature)
		END TEMP,
	cast(Lactose AS NVARCHAR(10)) LAC,
	cast([dbo].[TotalSolids]([Solids not fat (SNF)], Fat) AS NVARCHAR(10)) TS,
	cast([Other Solids] AS NVARCHAR(10)) OSOL,
	Seq [Lab Unique ID]
INTO #TempHorizonOrganic4
FROM #TempHorizonOrganic3

--select * from #TempHorizonOrganic4
--select * into dbo.HorizonOrganicData from #TempHorizonOrganic4
INSERT INTO dbo.HorizonOrganicData
SELECT *
FROM #TempHorizonOrganic4

DROP TABLE #TempHorizonOrganic1

DROP TABLE #TempHorizonOrganic2

DROP TABLE #TempHorizonOrganic3

DROP TABLE #TempHorizonOrganic4

-- select ? = count(1) from dbo.HorizonOrganicData
----------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM HorizonOrganicData
WHERE len([Member #]) > 0
ORDER BY [Member #],
	[Location],
	[Sample Date],
	[Lab Unique ID]
