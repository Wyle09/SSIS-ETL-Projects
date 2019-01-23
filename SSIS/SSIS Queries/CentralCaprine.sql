-- Central Caprine (Group CICS) file format.
TRUNCATE TABLE dbo.CaprineCentralData

SELECT *
INTO #TempCaprineCentral1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE CustomerCode = 'AUS001375'
	AND RowStatus = 0
	AND [dbo].[IsLoad](CustomerFramework) != '1'

--select * from #TempCaprineCentral1
-------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode,
	Tank,
	right(Barcode, 3) Seq,
	cast([Milk Urea Nitrogen (MUN)] AS DECIMAL(4, 2)) [Milk Urea Nitrogen (MUN)],
	[dbo].[UTCToCentralTime](SamplingTime) SampleDate,
	[dbo].[UTCToCentralTime](ValidationTime) TestDate,
	cast(Fat AS DECIMAL(4, 2)) Fat,
	cast([True Protein] AS DECIMAL(4, 2)) [True Protein],
	cast(Lactose AS DECIMAL(4, 2)) Lactose,
	cast([Other Solids] AS DECIMAL(4, 2)) [Other Solids],
	[Somatic cells],
	SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	Barcode,
	[dbo].[OfficialCode](TestCode) OfficialCode,
	cast([Solids not fat (SNF)] AS DECIMAL(4, 2)) [Solids not fat (SNF)],
	ParameterCode,
	TestCode,
	[Delvo Inhibitor],
	Coliforms,
	ProducerRouteNumber
INTO #TempCaprineCentral2
FROM #TempCaprineCentral1
WHERE len(Barcode) = 14

--Select * from #TempCaprineCentral2
------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(ProducerCode) ProducerCode,
	max(Tank) Tank,
	max(Seq) Seq,
	max([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	Max(SampleDate) SampleDate,
	Max(TestDate) TestDate,
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
	max(ProducerRouteNumber) ProducerRouteNumber
INTO #TempCaprineCentral3
FROM #TempCaprineCentral2
GROUP BY Barcode

--select * from #TempCaprineCentral3
--------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode [Prod #],
	Tank [Tank #],
	cast(cast(SampleDate AS DATE) AS NVARCHAR(20)) [Sample Date],
	cast(cast(TestDate AS DATE) AS NVARCHAR(20)) [Test Date],
	cast(Fat AS NVARCHAR(10)) ButterFat,
	cast([True Protein] AS NVARCHAR(10)) Protein,
	cast([Other Solids] AS NVARCHAR(10)) OtherSolids,
	[Somatic cells] DMSCC,
	[Delvo Inhibitor] GI,
	[Freezing Point] CRYO,
	[dbo].[RemoveCharacters]([Bacteria Count(PI)]) [PI],
	CASE 
		WHEN [dbo].[FormatTemp](SamplingTemperature) = '0'
			THEN NULL
		ELSE [dbo].[FormatTemp](SamplingTemperature)
		END [Sampled Temp],
	[dbo].[RemoveCharacters]([Bacteria Count]) PLC,
	cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10)) MUN,
	[dbo].[RemoveCharacters]([Thermoduric Plate Count]) LPC
INTO #TempCaprineCentral4
FROM #TempCaprineCentral3

--select * into dbo.CaprineCentralData from #TempCaprineCentral4 -- Need to use this insert statement if the table was droped.
INSERT INTO dbo.CaprineCentralData
SELECT *
FROM #TempCaprineCentral4

DROP TABLE #TempCaprineCentral1

DROP TABLE #TempCaprineCentral2

DROP TABLE #TempCaprineCentral3

DROP TABLE #TempCaprineCentral4

-- select ? = count(1) from dbo.CaprineCentralData
-----------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM CaprineCentralData
ORDER BY [Prod #],
	[Tank #],
	[Sample Date]
