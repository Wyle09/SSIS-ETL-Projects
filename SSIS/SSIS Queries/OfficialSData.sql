USE [eLimsMilkReportExports]
GO

/****** Object:  StoredProcedure [dbo].[GetOfficialsData]    Script Date: 11/9/2018 12:04:40 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[GetOfficialsData]
AS
SET NOCOUNT ON;

TRUNCATE TABLE dbo.OfficialsData;

SELECT *
INTO #TempOfficials1
FROM (
	SELECT *
	FROM M2MFileData
	) src
PIVOT(MAX(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein, [Analysis Temperature], [Bulk Tank Coliforms])) piv
WHERE CAST(RecordReceived AS DATE) >= CAST(DATEADD(DAY, - 1, GETDATE()) AS DATE)
	AND OfficialDataSent IS NULL
	AND TestCode LIKE 'MV%'
	AND CommercialOrderLineStatus != 'Cancelled';

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
	COALESCE(Coliform, [Bulk Tank Coliforms]) AS Coliforms,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	CustomerFramework,
	[dbo].[CastAsDecNullValues](Casein) Casein,
	RIGHT(Barcode, 4) Seq,
	'' LoadId,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	CustomerCode,
	[dbo].[ComponentsTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) ComponentsTestDate,
	Barcode,
	[Delvo Inhibitor],
	ParameterCode,
	TestCode,
	[dbo].[OfficialCode](TestCode, [Delvo Inhibitor], [Bacteria Count]) OfficialCode,
	[dbo].[AntiNumConfirmation]([Delvo Inhibitor]) AntiNumConfirmation,
	[Other Solids],
	[Analysis Temperature]
INTO #TempOfficials2
FROM #TempOfficials1
WHERE LEN(Barcode) >= 10;

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
	MAX(AntiNumConfirmation) AntiNumConfirmation,
	MAX([Other Solids]) [Other Solids],
	MAX([Analysis Temperature]) [Analysis Temperature]
INTO #TempOfficials3
FROM #TempOfficials2
GROUP BY Barcode;

SELECT ProducerCode,
	Tank,
	Seq,
	Barcode,
	SampleDate,
	TestDate,
	ComponentsTestDate,
	Fat,
	[True Protein],
	Lactose,
	[Solids not fat (SNF)],
	[Other Solids],
	dbo.TotalSolids([Solids not fat (SNF)], Fat) TotalSolids,
	[Freezing Point],
	[Milk Urea Nitrogen (MUN)],
	[Somatic Cells],
	[Bacteria Count],
	[Bacteria Count(PI)],
	[Thermoduric Plate Count],
	Coliforms,
	[Delvo Inhibitor],
	SamplingTemperature,
	[Analysis Temperature],
	CustomerCode,
	CustomerFramework,
	OfficialCode
INTO #TempOfficials4
FROM #TempOfficials3
WHERE (
		OfficialCode = '99'
		AND SampleDate IS NOT NULL
		AND [Bacteria Count] IS NOT NULL
		AND [Delvo Inhibitor] IS NOT NULL
		AND FAT IS NOT NULL
		AND [Somatic Cells] IS NOT NULL
		AND [Freezing Point] IS NOT NULL
		);

INSERT INTO dbo.OfficialsData
SELECT *
FROM #TempOfficials4;

UPDATE M2M
SET M2M.OfficialDataSent = 1
FROM M2MFileData M2M
INNER JOIN OfficialsData OD ON (M2M.Barcode = OD.Barcode);

DROP TABLE #TempOfficials1;

DROP TABLE #TempOfficials2;

DROP TABLE #TempOfficials3;

DROP TABLE #TempOfficials4;

SELECT *
FROM OfficialsData
ORDER BY CustomerCode,
	ProducerCode,
	Tank,
	SampleDate;
