-- Rock Dell
TRUNCATE TABLE dbo.RockDellData -- Remove previous data.

SELECT *
INTO #TempRockDell1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE CustomerCode = 'AUS001375'
	-- Where CustomerCode = ? -- Pass ssis CustomerCode parameter to the query.
	AND RowStatus = 0
	AND [dbo].[IsLoad](CustomerFramework) != '1'

--select * from #TempRockDell1
---------------------------------------------------------------------------------------------------------------------------------------------
SELECT Company,
	Division,
	ProducerCode,
	[dbo].[UTCToCentralTime](SamplingTime) SampleDate,
	[dbo].[UTCToCentralTime](ValidationTime) TestDate,
	cast(Fat AS DECIMAL(4, 2)) Fat,
	cast([True Protein] AS DECIMAL(4, 2)) [True Protein],
	cast(Lactose AS DECIMAL(4, 2)) Lactose,
	cast([Solids not fat (SNF)] AS DECIMAL(4, 2)) [Solids not fat (SNF)],
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
	substring(Barcode, 11, 4) Seq,
	'' LoadId,
	cast([Milk Urea Nitrogen (MUN)] AS DECIMAL(4, 2)) [Milk Urea Nitrogen (MUN)],
	CustomerCode,
	[dbo].[ComponentsTestDateCentral](ParameterCode, ValidationTime, MeasuredTime) ComponentsTestDate,
	Casein,
	Barcode,
	[Delvo Inhibitor],
	ParameterCode,
	TestCode
INTO #TempRockDell2
FROM #TempRockDell1
WHERE len(Barcode) = 14

--select * from #TempRockDell2
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
	max(Casein) Casein
INTO #TempRockDell3
FROM #TempRockDell2
GROUP BY Barcode

--select * from #TempRockDell3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT Company,
	Division,
	ProducerCode,
	[dbo].IsLoad(CustomerFramework) IsLoad,
	'0' Fixed,
	cast(cast(SampleDate AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(Cast(TestDate AS DATE) AS NVARCHAR(20)) TestDate,
	[dbo].[OfficialCode](TestCode) OfficialCode,
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
	[Bacteria Count],
	[Freezing Point],
	[dbo].[Water]([Freezing Point]) Water,
	'' Blank,
	[dbo].[AntiCharConfirmation]([Delvo Inhibitor], ParameterCode) AntiCharConformation,
	Sediment,
	'' Blank1,
	Tank,
	'' Blank0,
	'DQCI' Fixed0,
	Coliforms,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	'' Blank2,
	Seq,
	LoadId,
	[dbo].[RemoveDecPoint]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	'0' Fixed1,
	[dbo].[AntiNumConfirmation]([Delvo Inhibitor], ParameterCode) AntiNumConfirmation,
	cast(cast(ComponentsTestDate AS DATE) AS NVARCHAR(20)) ComponentsTestDate,
	'0' Fixed2,
	'' Blank3,
	'' Blank4,
	'' Blank5,
	'' Blank6,
	'' Blank7,
	[dbo].[RemoveDecPoint](Casein) Casein
INTO #TempRockDell4
FROM #TempRockDell3

--select * into dbo.RockDellData from #TempRockDell4 -- Use this if the table is deleted.
INSERT INTO dbo.RockDellData
SELECT *
FROM #TempRockDell4

DROP TABLE #TempRockDell1

DROP TABLE #TempRockDell2

DROP TABLE #TempRockDell3

DROP TABLE #TempRockDell4

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM dbo.RockDellData
ORDER BY ProducerCode,
	Tank,
	SampleDate
