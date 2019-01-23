--Plainview DSI File Format
SET NOCOUNT ON;

TRUNCATE TABLE dbo.PlainviewData

SELECT *
INTO #TempPlainview1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(MAX(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[PlainviewCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled';

--select * from #TempPlainview1
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
	Coliform AS Coliforms,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	CustomerFramework,
	[dbo].[CastAsDecNullValues](Casein) Casein,
	CASE 
		WHEN (
				LEN(barcode) = 10
				AND Barcode LIKE '750%'
				)
			THEN '0000'
		ELSE RIGHT(Barcode, 4)
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
INTO #TempPlainview2
FROM #TempPlainview1
WHERE len(Barcode) >= 10;

--select * from #TempPlainview2
----------------------------------------------------------------------------------------------------------------------------------------------------
SELECT MAX(Fat) Fat,
	MAX([True Protein]) [True Protein],
	MAX(Lactose) Lactose,
	MAX([Solids not fat (SNF)]) [Solids not fat (SNF)],
	Barcode,
	MAX(SampleDate) SampleDate,
	MIN(TestDate) TestDate,
	MAX([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	MAX(LoadId) LoadId,
	MAX(Tank) Tank,
	MAX(Seq) Seq,
	MAX(CustomerCode) CustomerCode,
	MAX(CustomerFramework) CustomerFramework,
	MAX([Somatic Cells]) [Somatic Cells],
	MAX(ComponentsTestDate) ComponentsTestDate,
	MAX([Bacteria Count]) [Bacteria Count],
	MAX([Bacteria Count(PI)]) [Bacteria Count(PI)],
	MAX(SamplingTemperature) SamplingTemperature,
	MAX([Freezing Point]) [Freezing Point],
	MAX(Company) Company,
	MAX(division) Division,
	MAX(ProducerCode) ProducerCode,
	MAX(Sediment) Sediment,
	MAX(Coliforms) Coliforms,
	MAX([Thermoduric Plate Count]) [Thermoduric Plate Count],
	MAX([Delvo Inhibitor]) [Delvo Inhibitor],
	MAX(ParameterCode) ParameterCode,
	MAX(TestCode) TestCode,
	MAX(Casein) Casein,
	MAX(OfficialCode) OfficialCode,
	MAX(AntiNumConfirmation) AntiNumConfirmation
INTO #TempPlainview3
FROM #TempPlainview2
GROUP BY Barcode;

--select * from #TempPlainview3
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
	ProducerCode,
	dbo.IsLoad(CustomerFrameWork) IsLoad,
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
	CASE 
		WHEN dbo.IsLoad(CustomerFramework) = '1'
			THEN RIGHT(Barcode, 8)
		END LoadId,
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
INTO #TempPlainview4
FROM #TempPlainview3;

--select * into dbo.PlainviewData from #TempPlainview4 -- Use this if the table is deleted.
INSERT INTO dbo.PlainviewData
SELECT *
FROM #TempPlainview4;

DROP TABLE #TempPlainview1;

DROP TABLE #TempPlainview2;

DROP TABLE #TempPlainview3;

DROP TABLE #TempPlainview4;

-- select ?=count(1) from dbo.PlainviewData
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF;

SELECT *
FROM dbo.PlainviewData
ORDER BY ProducerCode,
	Tank,
	SampleDate;
