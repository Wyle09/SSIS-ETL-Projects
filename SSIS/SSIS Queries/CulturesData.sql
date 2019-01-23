USE [eLimsMilkReportExports]
GO

/****** Object:  StoredProcedure [dbo].[GetCultureData]    Script Date: 10/24/2018 1:45:54 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[GetCultureData] @dataFor AS NVARCHAR(20)
AS
SET NOCOUNT ON;

TRUNCATE TABLE CulturesData;

-- Get data for AMPI only, otherwise get the data fro the rest of the clients.
IF (@dataFor = 'AMPI')
BEGIN
	SELECT *
	INTO #TempCulturesData1
	FROM M2MFileData
	WHERE TestCode IN ('MVQVP22', 'MVQVP22b', 'MVQVP22m', 'MVQVP22mb', 'MVQVP22p', 'MVQVP22pb', 'MVQVP22pm', 'MVQVP22pmb', 'MVQVP43', 'MVQVP39')
		AND CommercialOrderLineStatus != 'Cancelled'
		AND CAST(RecordReceived AS DATE) >= CAST(DATEADD(DAY, - 7, GETDATE()) AS DATE)
		AND CultureDataSent IS NULL
		AND [dbo].[AMPICustomerCodes](CustomerCode) = '1';

	SELECT CustomerCode,
		CustomerFramework,
		CFWInternalName,
		SampleID,
		CommercialOrderId,
		Barcode,
		ProducerCode,
		Tank,
		RegistrationTime,
		SamplingTime,
		MeasuredTime,
		ValidationTime,
		TestCode,
		TestName,
		ParameterCode,
		ParameterName,
		ResultValue,
		UoM
	INTO #TempCulturesData2
	FROM #TempCulturesData1;

	INSERT INTO dbo.CulturesData
	SELECT *
	FROM #TempCulturesData2;

	DROP TABLE #TempCulturesData1;

	DROP TABLE #TempCulturesData2;
END;
ELSE
BEGIN
	SELECT *
	INTO #TempCulturesData3
	FROM M2MFileData
	WHERE TestCode IN ('MVQVP22', 'MVQVP22b', 'MVQVP22m', 'MVQVP22mb', 'MVQVP22p', 'MVQVP22pb', 'MVQVP22pm', 'MVQVP22pmb')
		AND CommercialOrderLineStatus != 'Cancelled'
		AND CAST(RecordReceived AS DATE) >= CAST(DATEADD(DAY, - 7, GETDATE()) AS DATE)
		AND CultureDataSent IS NULL
		AND [dbo].[AMPICustomerCodes](CustomerCode) != '1';

	SELECT CustomerCode,
		CustomerFramework,
		CFWInternalName,
		SampleID,
		CommercialOrderId,
		Barcode,
		ProducerCode,
		Tank,
		RegistrationTime,
		SamplingTime,
		MeasuredTime,
		ValidationTime,
		TestCode,
		TestName,
		ParameterCode,
		ParameterName,
		ResultValue,
		UoM
	INTO #TempCulturesData4
	FROM #TempCulturesData3;

	INSERT INTO dbo.CulturesData
	SELECT *
	FROM #TempCulturesData4;

	DROP TABLE #TempCulturesData3;

	DROP TABLE #TempCulturesData4;
END;

UPDATE M2M
SET M2M.CultureDataSent = 1
FROM M2MFileData M2M
INNER JOIN CulturesData CD ON (
		M2M.Barcode = CD.Barcode
		AND M2M.SamplingTime = CD.SamplingTime
		);

SET NOCOUNT OFF;

SELECT *
FROM CulturesData
ORDER BY CustomerCode,
	ProducerCode,
	Tank,
	SamplingTime;
