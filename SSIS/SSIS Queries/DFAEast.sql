-- DFA File Format.
SET NOCOUNT ON

TRUNCATE TABLE dbo.DFAEastData

-- Remove previous data.
SELECT *
INTO #TempDFAEast1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[DFAEastCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempDFAEast1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT Company,
	Division,
	ProducerCode,
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[PassZeroIfNull]([dbo].[CastAsDecNullValues](Fat)) Fat,
	[dbo].[PassZeroIfNull]([dbo].[CastAsDecNullValues]([True Protein])) [True Protein],
	SampleID,
	[dbo].[PassZeroIfNull]([dbo].[CastAsDecNullValues](Lactose)) Lactose,
	[dbo].[PassZeroIfNull]([dbo].[CastAsDecNullValues]([Solids not fat (SNF)])) [Solids not fat (SNF)],
	[dbo].[PassZeroIfNull]([Somatic cells]) [Somatic cells],
	[dbo].[PassZeroIfNull]([dbo].[FormatTemp](SamplingTemperature)) + '.00' SamplingTemperature,
	[dbo].[RemoveCharacters]([Bacteria Count]) [Bacteria Count],
	[dbo].[FreezePointDec]([Freezing Point], '+') [Freezing Point],
	[dbo].[CastAsDecNullValues](Sediment) Sediment,
	[dbo].[PadRight2](Tank) Tank,
	[dbo].[RemoveCharacters](Coliforms) Coliforms,
	[dbo].[RemoveCharacters]([Thermoduric Plate Count]) [Thermoduric Plate Count],
	[dbo].[RemoveCharacters]([Bacteria Count(PI)]) [Bacteria Count(PI)],
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
	dbo.OfficialCode(TestCode, [Delvo Inhibitor], [Bacteria Count]) OfficialCode,
	[dbo].[AntiCharConfirmationNP]([Delvo Inhibitor]) AntiCharConfirmationNP
INTO #TempDFAEast2
FROM #TempDFAEast1
WHERE len(Barcode) >= 10

--select * from #TempDFAEast2
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(Fat) Fat,
	Max([True Protein]) [True Protein],
	max(Lactose) Lactose,
	max([Solids not fat (SNF)]) [Solids not fat (SNF)],
	max(SampleID) SampleID,
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
	max(OfficialCode) OfficialCode,
	max(AntiCharConfirmationNP) AntiCharConfirmationNP
INTO #TempDFAEast3
FROM #TempDFAEast2
GROUP BY Barcode

--select * from #TempDFAEast3
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	Division,
	ProducerCode,
	CASE 
		WHEN OfficialCode != '99'
			THEN '00'
		ELSE OfficialCode
		END OfficialCode,
	'00000000' Fixed1,
	'A' Fixed2,
	Fat,
	[True Protein],
	Lactose,
	[Solids not fat (SNF)],
	[Somatic Cells],
	'N' Fixed3,
	'0.00' Fixed4,
	SampleID,
	[Freezing Point],
	AntiCharConfirmationNP,
	Sediment,
	SamplingTemperature,
	'' BacteriaTestType1,
	cast('' AS NVARCHAR(10)) BacteriaValue1,
	'' BacteriaTestType2,
	cast('' AS NVARCHAR(10)) BacteriaValue2,
	'' BacteriaTestType3,
	cast('' AS NVARCHAR(10)) BacteriaValue3,
	'' Blank1,
	Barcode,
	Seq,
	TestCode,
	[Milk Urea Nitrogen (MUN)],
	[Bacteria Count],
	[Thermoduric Plate Count],
	Coliforms,
	[Bacteria Count(PI)],
	Tank,
	CustomerCode,
	CustomerFramwork
INTO #TempDFAEast4
FROM #TempDFAEast3

--select * from #TempDFAEast4
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE #TempDFAEast4
SET BacteriaTestType1 = CASE 
		WHEN (
				[Milk Urea Nitrogen (MUN)] IS NOT NULL
				AND [Milk Urea Nitrogen (MUN)] != 0
				)
			AND BacteriaTestType1 NOT IN ('S', 'P', 'L', 'C')
			AND 'M' NOT IN (BacteriaTestType2, BacteriaTestType3)
			THEN 'M'
		WHEN [Bacteria Count] IS NOT NULL
			AND BacteriaTestType1 NOT IN ('M', 'P', 'L', 'C')
			AND 'S' NOT IN (BacteriaTestType2, BacteriaTestType3)
			THEN 'S'
		WHEN [Bacteria Count(PI)] IS NOT NULL
			AND BacteriaTestType1 NOT IN ('M', 'S', 'L', 'C')
			AND 'P' NOT IN (BacteriaTestType2, BacteriaTestType3)
			THEN 'P'
		WHEN [Thermoduric Plate Count] IS NOT NULL
			AND BacteriaTestType1 NOT IN ('M', 'S', 'P', 'C')
			AND 'L' NOT IN (BacteriaTestType2, BacteriaTestType3)
			THEN 'L'
		WHEN Coliforms IS NOT NULL
			AND BacteriaTestType1 NOT IN ('M', 'S', 'P', 'L')
			AND 'C' NOT IN (BacteriaTestType2, BacteriaTestType3)
			THEN 'C'
		ELSE ''
		END,
	BacteriaValue1 = CASE 
		WHEN (
				[Milk Urea Nitrogen (MUN)] IS NOT NULL
				AND [Milk Urea Nitrogen (MUN)] != 0
				)
			AND BacteriaTestType1 NOT IN ('S', 'P', 'L', 'C')
			AND cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10)) NOT IN (BacteriaValue2, BacteriaValue3)
			THEN cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10))
		WHEN [Bacteria Count] IS NOT NULL
			AND BacteriaTestType1 NOT IN ('M', 'P', 'L', 'C')
			AND cast([Bacteria Count] AS NVARCHAR(10)) NOT IN (BacteriaValue2, BacteriaValue3)
			THEN cast([Bacteria Count] AS NVARCHAR(10))
		WHEN [Bacteria Count(PI)] IS NOT NULL
			AND BacteriaTestType1 NOT IN ('M', 'S', 'L', 'C')
			AND cast([Bacteria Count(PI)] AS NVARCHAR(10)) NOT IN (BacteriaValue2, BacteriaValue3)
			THEN cast([Bacteria Count(PI)] AS NVARCHAR(10))
		WHEN [Thermoduric Plate Count] IS NOT NULL
			AND BacteriaTestType1 NOT IN ('M', 'S', 'P', 'C')
			AND cast([Thermoduric Plate Count] AS NVARCHAR(10)) NOT IN (BacteriaValue2, BacteriaValue3)
			THEN cast([Thermoduric Plate Count] AS NVARCHAR(10))
		WHEN Coliforms IS NOT NULL
			AND BacteriaTestType1 NOT IN ('M', 'S', 'P', 'L')
			AND cast(Coliforms AS NVARCHAR(10)) NOT IN (BacteriaValue2, BacteriaValue3)
			THEN cast(Coliforms AS NVARCHAR(10))
		ELSE ''
		END

UPDATE #TempDFAEast4
SET BacteriaTestType2 = CASE 
		WHEN [Bacteria Count] IS NOT NULL
			AND BacteriaTestType2 NOT IN ('M', 'P', 'L', 'C')
			AND 'S' NOT IN (BacteriaTestType1, BacteriaTestType3)
			THEN 'S'
		WHEN (
				[Milk Urea Nitrogen (MUN)] IS NOT NULL
				AND [Milk Urea Nitrogen (MUN)] != 0
				)
			AND BacteriaTestType2 NOT IN ('S', 'P', 'L', 'C')
			AND 'M' NOT IN (BacteriaTestType1, BacteriaTestType3)
			THEN 'M'
		WHEN [Bacteria Count(PI)] IS NOT NULL
			AND BacteriaTestType2 NOT IN ('M', 'S', 'L', 'C')
			AND 'P' NOT IN (BacteriaTestType1, BacteriaTestType3)
			THEN 'P'
		WHEN [Thermoduric Plate Count] IS NOT NULL
			AND BacteriaTestType2 NOT IN ('M', 'S', 'P', 'C')
			AND 'L' NOT IN (BacteriaTestType1, BacteriaTestType3)
			THEN 'L'
		WHEN Coliforms IS NOT NULL
			AND BacteriaTestType2 NOT IN ('M', 'S', 'P', 'L')
			AND 'C' NOT IN (BacteriaTestType1, BacteriaTestType3)
			THEN 'C'
		ELSE ''
		END,
	BacteriaValue2 = CASE 
		WHEN [Bacteria Count] IS NOT NULL
			AND BacteriaTestType2 NOT IN ('M', 'P', 'L', 'C')
			AND cast([Bacteria Count] AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue3)
			THEN cast([Bacteria Count] AS NVARCHAR(10))
		WHEN (
				[Milk Urea Nitrogen (MUN)] IS NOT NULL
				AND [Milk Urea Nitrogen (MUN)] != 0
				)
			AND BacteriaTestType2 NOT IN ('S', 'P', 'L', 'C')
			AND cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue3)
			THEN cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10))
		WHEN cast([Bacteria Count(PI)] AS NVARCHAR(10)) IS NOT NULL
			AND BacteriaTestType2 NOT IN ('M', 'S', 'L', 'C')
			AND cast([Bacteria Count(PI)] AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue3)
			THEN cast([Bacteria Count(PI)] AS NVARCHAR(10))
		WHEN [Thermoduric Plate Count] IS NOT NULL
			AND BacteriaTestType2 NOT IN ('M', 'S', 'P', 'C')
			AND cast([Thermoduric Plate Count] AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue3)
			THEN cast([Thermoduric Plate Count] AS NVARCHAR(10))
		WHEN Coliforms IS NOT NULL
			AND BacteriaTestType2 NOT IN ('M', 'S', 'P', 'L')
			AND cast(Coliforms AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue3)
			THEN cast(Coliforms AS NVARCHAR(10))
		ELSE ''
		END

UPDATE #TempDFAEast4
SET BacteriaTestType3 = CASE 
		WHEN [Bacteria Count] IS NOT NULL
			AND BacteriaTestType3 NOT IN ('M', 'P', 'L', 'C')
			AND 'S' NOT IN (BacteriaTestType1, BacteriaTestType2)
			THEN 'S'
		WHEN (
				[Milk Urea Nitrogen (MUN)] IS NOT NULL
				AND [Milk Urea Nitrogen (MUN)] != 0
				)
			AND BacteriaTestType3 NOT IN ('S', 'P', 'L', 'C')
			AND 'M' NOT IN (BacteriaTestType1, BacteriaTestType2)
			THEN 'M'
		WHEN [Bacteria Count(PI)] IS NOT NULL
			AND BacteriaTestType3 NOT IN ('M', 'S', 'L', 'C')
			AND 'P' NOT IN (BacteriaTestType1, BacteriaTestType2)
			THEN 'P'
		WHEN [Thermoduric Plate Count] IS NOT NULL
			AND BacteriaTestType3 NOT IN ('M', 'S', 'P', 'C')
			AND 'L' NOT IN (BacteriaTestType1, BacteriaTestType2)
			THEN 'L'
		WHEN Coliforms IS NOT NULL
			AND BacteriaTestType3 NOT IN ('M', 'S', 'P', 'L')
			AND 'C' NOT IN (BacteriaTestType1, BacteriaTestType2)
			THEN 'C'
		ELSE ''
		END,
	BacteriaValue3 = CASE 
		WHEN [Bacteria Count] IS NOT NULL
			AND BacteriaTestType3 NOT IN ('M', 'P', 'L', 'C')
			AND cast([Bacteria Count] AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue2)
			THEN cast([Bacteria Count] AS NVARCHAR(10))
		WHEN (
				[Milk Urea Nitrogen (MUN)] IS NOT NULL
				AND [Milk Urea Nitrogen (MUN)] != 0
				)
			AND BacteriaTestType3 NOT IN ('S', 'P', 'L', 'C')
			AND cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue2)
			THEN cast([Milk Urea Nitrogen (MUN)] AS NVARCHAR(10))
		WHEN [Bacteria Count(PI)] IS NOT NULL
			AND BacteriaTestType3 NOT IN ('M', 'S', 'L', 'C')
			AND cast([Bacteria Count(PI)] AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue2)
			THEN cast([Bacteria Count(PI)] AS NVARCHAR(10))
		WHEN [Thermoduric Plate Count] IS NOT NULL
			AND BacteriaTestType3 NOT IN ('M', 'S', 'P', 'C')
			AND cast([Thermoduric Plate Count] AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue2)
			THEN cast([Thermoduric Plate Count] AS NVARCHAR(10))
		WHEN Coliforms IS NOT NULL
			AND BacteriaTestType3 NOT IN ('M', 'S', 'P', 'L')
			AND cast(Coliforms AS NVARCHAR(10)) NOT IN (BacteriaValue1, BacteriaValue2)
			THEN cast(Coliforms AS NVARCHAR(10))
		ELSE ''
		END

--select * from #TempDFAEast4
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT SampleDate,
	Division,
	dbo.ProducerOrLoadID(ProducerCode, Barcode, CustomerCode, CustomerFramwork) ProducerCode,
	OfficialCode,
	Fixed1,
	Fixed2,
	Fat,
	[True Protein],
	Lactose,
	[Solids not fat (SNF)],
	[Somatic Cells],
	Fixed3,
	Fixed4,
	[Freezing Point],
	AntiCharConfirmationNP,
	Sediment,
	SamplingTemperature,
	BacteriaTestType1,
	BacteriaValue1,
	BacteriaTestType2,
	BacteriaValue2,
	BacteriaTestType3,
	BacteriaValue3,
	Blank1,
	CASE 
		WHEN Barcode LIKE 'PLD%'
			THEN '00'
		ELSE Tank
		END Tank,
	Barcode,
	CASE 
		WHEN Barcode LIKE 'PLD%'
			THEN right(Barcode, 6)
		ELSE Seq
		END Seq
INTO #TempDFAEast5
FROM #TempDFAEast4

--select * into dbo.DFAEastData from #TempDFAEast5
INSERT INTO dbo.DFAEastData
SELECT *
FROM #TempDFAEast5

DROP TABLE #TempDFAEast1

DROP TABLE #TempDFAEast2

DROP TABLE #TempDFAEast3

DROP TABLE #TempDFAEast4

DROP TABLE #TempDFAEast5

--select ? = count(1)  from dbo.DFAEastData
---------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF

SELECT *
FROM dbo.DFAEastData
WHERE len(ProducerCode) > 0
ORDER BY ProducerCode,
	Tank,
	SampleDate
