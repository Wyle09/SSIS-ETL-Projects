USE [eLimsMilkReportExports]
GO

/****** Object:  UserDefinedFunction [dbo].[AgropurCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[AgropurCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00292720XU0')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[AMPICustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[AMPICustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00525575UDG', 'AUS001375', 'A00292718K99', 'A00292779VBU', 'A00293030E8O', 'A00366900PDW', 'A00455838R32', 'A00525577XV6', 'A00525578C39', 'A00525582ASY', 'A00547714YVM', 'A0052557913X', 'A00225860', 'A00525582ASY')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[AmpiLoadID]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[AmpiLoadID] (
	@CustomerFramework AS NVARCHAR(50),
	@Barcode AS NVARCHAR(20)
	)
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @LoadID AS NVARCHAR(20)

	SELECT @LoadID = CASE 
			WHEN dbo.IsLoad(@CustomerFramework) = '1'
				THEN '99' + RIGHT(@Barcode, 8)
			ELSE ''
			END

	RETURN @LoadId
END
GO

/****** Object:  UserDefinedFunction [dbo].[AntiCharConfirmation]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[AntiCharConfirmation] (
	@DelvoResult AS NVARCHAR(20),
	@BacteriaResult AS NVARCHAR(20),
	@TestCode AS NVARCHAR(10)
	)
RETURNS NVARCHAR(4)
AS
BEGIN
	DECLARE @AntiResult NVARCHAR(20)

	SELECT @AntiResult = CASE 
			WHEN @DelvoResult IN ('Not Found', 'NF')
				THEN 'NF'
			WHEN @DelvoResult IN ('Positive', 'P', 'AL')
				THEN 'AL'
			WHEN len(@BacteriaResult) > 0
				AND dbo.IsOfficialYN(@TestCode, @DelvoResult, @BacteriaResult) = 'N'
				THEN 'NT'
			ELSE ''
			END

	RETURN @AntiResult
END
	--ALTER function [dbo].[AntiCharConfirmation](@DelvoResult as nvarchar(20), @TestCode as nvarchar(10))
	--returns nvarchar(4) 
	--as 
	--begin 
	--declare @AntiResult nvarchar(20)
	--select @AntiResult = 
	--    case 
	--        when @DelvoResult = 'Not Found' and @TestCode = 'MVPQVP2' then 'NF'
	--        when @DelvoResult = 'Positive' and @TestCode = 'MVPQVP2' then 'AL'
	--	    when @TestCode != 'MVPQVP2' then 'NT'
	--	end
	--return @AntiResult
	--end 
GO

/****** Object:  UserDefinedFunction [dbo].[AntiCharConfirmationNP]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[AntiCharConfirmationNP] (@DelvoResult AS NVARCHAR(20))
RETURNS NVARCHAR(4)
AS
BEGIN
	DECLARE @AntiResult NVARCHAR(20)

	SELECT @AntiResult = CASE 
			WHEN @DelvoResult IN ('Not Found', 'NF')
				THEN 'N'
			WHEN @DelvoResult IN ('Positive', 'P', 'AL')
				THEN 'P'
			ELSE ''
			END

	RETURN @AntiResult
END
GO

/****** Object:  UserDefinedFunction [dbo].[AntiNumConfirmation]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[AntiNumConfirmation] (@DelvoResult AS NVARCHAR(20))
RETURNS NVARCHAR(2)
AS
BEGIN
	DECLARE @AntiResult NVARCHAR(20)

	SELECT @AntiResult = CASE 
			WHEN @DelvoResult IN ('Not Found', 'NF')
				THEN '2'
			WHEN @DelvoResult IN ('Positive', 'P', 'AL')
				THEN '1'
			ELSE '0'
			END

	RETURN @AntiResult
END
GO

/****** Object:  UserDefinedFunction [dbo].[CastAsDecLOL]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[CastAsDecLOL] (@Parm AS NVARCHAR(10))
RETURNS DECIMAL(5, 2)
AS
BEGIN
	DECLARE @ReturnValue DECIMAL(5, 2)

	SELECT @ReturnValue = CASE 
			WHEN @Parm IS NULL
				OR @Parm = ''
				THEN cast('0.00' AS DECIMAL(5, 2))
			ELSE cast(@Parm AS DECIMAL(5, 2))
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[CastAsDecNullValues]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[CastAsDecNullValues] (@Parm AS NVARCHAR(10))
RETURNS DECIMAL(5, 2)
AS
BEGIN
	DECLARE @ReturnValue DECIMAL(5, 2)

	SELECT @ReturnValue = CASE 
			WHEN @Parm IS NULL
				OR @Parm = ''
				THEN NULL
			ELSE cast(@Parm AS DECIMAL(5, 2))
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[CastAsIntNullValues]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[CastAsIntNullValues] (@Parm AS NVARCHAR(10))
RETURNS DECIMAL(5, 2)
AS
BEGIN
	DECLARE @ReturnValue INT

	SELECT @ReturnValue = CASE 
			WHEN @Parm IS NULL
				OR @Parm = ''
				THEN NULL
			ELSE cast(@Parm AS INT)
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[ComponentsTestDate]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[ComponentsTestDate] (
	@ParameterCode AS NVARCHAR(20),
	@MeasuredTime AS NVARCHAR(50),
	@ValidationTime AS NVARCHAR(50),
	@CommercialItem AS NVARCHAR(50)
	)
RETURNS DATETIME2
AS
BEGIN
	DECLARE @date DATETIME2

	SELECT @date = CASE 
			WHEN @ParameterCode = '7027A006'
				THEN coalesce(@MeasuredTime, @ValidationTime)
			END

	RETURN @date
END
GO

/****** Object:  UserDefinedFunction [dbo].[CropCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[CropCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00525584UMA')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[DaviscoCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[DaviscoCustomerCodes] (
	@CustomerCode AS NVARCHAR(20),
	@CustomerFramework AS NVARCHAR(20)
	)
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00525594BOD')
				AND @CustomerFramework NOT IN ('CFA00525594BOD-2', 'CFA00525594BOD-3')
				THEN '1'
			WHEN @CustomerFramework IN ('CFA00525594BOD-2')
				THEN '2' -- Laken Norden Loads.
			WHEN @CustomerFramework IN ('CFA00525594BOD-3')
				THEN '3' -- Lesueur Loads.
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[DeansCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[DeansCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('AUS022004', 'A005643335GB', 'A00564334ZY1')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[DFACustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[DFACustomerCodes] (
	@CustomerCode AS NVARCHAR(20),
	@CustomerFramework AS NVARCHAR(20)
	)
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('AUS023973')
				THEN '1'
			WHEN @CustomerFramework IN ('CFA00525584UMA-3', 'CFA00525590TP8-2')
				THEN '2'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[DFAEastCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[DFAEastCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00564332QXD')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[ForemostCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[ForemostCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00099666')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[FormatTemp]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FormatTemp] (@Temp AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @FinalValue AS NVARCHAR(10)

	SELECT @FinalValue = CASE 
			WHEN @Temp LIKE upper('%f%')
				THEN left(@Temp, charindex('.', @Temp) - 1)
			WHEN @Temp LIKE upper('%c%')
				THEN left(@Temp, charindex('.', @Temp) - 1)
			ELSE NULL
			END

	RETURN @FinalValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[FreezePointDec]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[FreezePointDec] (
	@FP AS NVARCHAR(10),
	@PosOrNeg AS NVARCHAR(10)
	)
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Result DECIMAL(4, 3)

	SELECT @Result = CASE 
			WHEN @FP IS NOT NULL
				AND @PosOrNeg = '+'
				THEN cast(@FP AS FLOAT) / 1000
			WHEN @FP IS NOT NULL
				AND @PosOrNeg = '-'
				THEN (cast(@FP AS FLOAT) / 1000) * - 1
			ELSE ''
			END

	RETURN cast(@Result AS NVARCHAR(10))
END
GO

/****** Object:  UserDefinedFunction [dbo].[GeoMeanFlag]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--Get geo mean flag for Officials and Components commercial Item.
CREATE FUNCTION [dbo].[GeoMeanFlag] (
	@TestCode AS NVARCHAR(10),
	@DelvoResult AS NVARCHAR(20),
	@BacteriaResult AS NVARCHAR(20)
	)
RETURNS NVARCHAR(4)
AS
BEGIN
	DECLARE @GeoFlag NVARCHAR(20)

	SELECT @GeoFlag = CASE 
			WHEN @TestCode IN ('MVQVP51B')
				OR dbo.IsOfficialYN(@TestCode, @DelvoResult, @BacteriaResult) = 'Y'
				THEN 'Y'
			ELSE 'N'
			END

	RETURN @GeoFlag
END
GO

/****** Object:  UserDefinedFunction [dbo].[GrandeCheeseCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[GrandeCheeseCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A003322065Z9')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[HorizonCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[HorizonCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00525590TP8')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[IsLoad]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[IsLoad] (@CustomerFrameWork AS NVARCHAR(20))
RETURNS NVARCHAR(2)
AS
BEGIN
	DECLARE @LoadCustomerFrameWork NVARCHAR(20)

	SELECT @LoadCustomerFrameWork = CASE 
			WHEN @CustomerFrameWork IN ('CFA002927199IK-2', 'CFA00292720XU0-7', 'CFA00292720XU0-8', 'CFA00292720XU0-9', 'CFA00292829IAV-3', 'CFA005255738YD-10', 'CFA005255738YD-12', 'CFA00525589AR1-10', 'CFA00525589AR1-2', 'CFA00525589AR1-3', 'CFA00525589AR1-4', 'CFA00525589AR1-6', 'CFA00525589AR1-7', 'CFA00525589AR1-8', 'CFA00525589AR1-9', 'CFA00525594BOD-2', 'CFA00525594BOD-3', 'CFAUS001375-10', 'CFAUS001375-11', 'CFAUS001375-12', 'CFAUS001375-13', 'CFAUS001375-14', 'CFAUS001375-5', 'CFAUS001375-6', 'CFAUS001375-7', 'CFAUS001375-8', 'CFAUS001375-9', 'CFAUS023973-7', 'CFA00292961LSV-1', 'CFA00292763BV3-1', 'CFA00525584UMA-1', 'CFAUS022732-1', 'CFA003732969Z6-4', 'CFA003732969Z6-5')
				THEN '1'
			ELSE ''
			END

	RETURN @LoadCustomerFrameWork
END
GO

/****** Object:  UserDefinedFunction [dbo].[IsOfficialYN]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[IsOfficialYN] (
	@TestCode AS NVARCHAR(10),
	@DelvoResult AS NVARCHAR(20),
	@BacteriaResult AS NVARCHAR(20)
	)
RETURNS NVARCHAR(4)
AS
BEGIN
	DECLARE @AntiResult NVARCHAR(20)

	SELECT @AntiResult = CASE 
			WHEN @TestCode IN ('MVPQVP1', 'MVPQVP2', 'MVPQVP6', 'MVPQVP8b', 'MVPQVPG', 'MVPQVPB', 'MVPQVP8', 'HOPQVP2', 'HOPQVPB', 'HOTF033', 'HOTF001G', 'HOTF033G', 'HOTF0037')
				THEN 'Y'
			WHEN @TestCode IN ('MVPQVP1', 'MVPQVP2', 'MVPQVP6', 'MVPQVP8b', 'MVPQVPG', 'MVPQVPB', 'MVPQVP8', 'HOPQVP2', 'HOPQVPB', 'HOTF033', 'HOTF001G', 'HOTF033G', 'HOTF0037')
				AND (
					[dbo].[AntiCharConfirmationNP](@DelvoResult) IN ('P', 'N')
					OR [dbo].[AntiCharConfirmation](@DelvoResult, @BacteriaResult, @TestCode) IN ('NF', 'AL')
					OR [dbo].[AntiNumConfirmation](@DelvoResult) IN ('1', '2')
					)
				THEN 'Y'
			ELSE 'N'
			END

	RETURN @AntiResult
END
GO

/****** Object:  UserDefinedFunction [dbo].[LOLCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[LOLCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00292829IAV', 'A0056428249L')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[LOLFieldLen]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[LOLFieldLen] (
	@Field AS NVARCHAR(50),
	@MaxLen INT
	)
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(10)

	IF (len(@Field) > @MaxLen)
	BEGIN
		SET @Field = left(@Field, @MaxLen)
	END

	SELECT @ReturnValue = @Field

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[MarigoldCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[MarigoldCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00292961LSV', 'A00292763BV3')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[NDPEastCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--Natural Dairy Products.
CREATE FUNCTION [dbo].[NDPEastCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00381487UEP')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[NFOCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[NFOCustomerCodes] (
	@CustomerCode AS NVARCHAR(20),
	@CustomerFramework AS NVARCHAR(20)
	)
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('AUS023973')
				THEN '1'
			WHEN @CustomerFramework IN ('CFA00525584UMA-2')
				THEN '2'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[OfficialCode]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[OfficialCode] (
	@TestCode AS NVARCHAR(10),
	@DelvoResult AS NVARCHAR(20),
	@BacteriaResult AS NVARCHAR(20)
	)
RETURNS NVARCHAR(5)
AS
BEGIN
	DECLARE @OfficCode NVARCHAR(5)

	SELECT @OfficCode = CASE 
			--Use dbo.IsPffocialYN() as the main function, otherwise you will not be able to flag the officials.
			--For example if a sample is registered for a official and a components commercial item is added to
			--the same sample it will not flag as an official do the components commercial item, if the commercial item
			-- for components is added to the official cm items function then it will also flag cm items that are
			-- components only
			WHEN dbo.IsOfficialYN(@TestCode, @DelvoResult, @BacteriaResult) = 'Y'
				THEN '99'
			ELSE ''
			END

	RETURN @OfficCode
END
GO

/****** Object:  UserDefinedFunction [dbo].[OfficialCodeYN]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[OfficialCodeYN] (
	@TestCode AS NVARCHAR(20),
	@DelvoResult AS NVARCHAR(20),
	@BacteriaResult AS NVARCHAR(20)
	)
RETURNS NVARCHAR(5)
AS
BEGIN
	DECLARE @OfficCode NVARCHAR(5)

	SELECT @OfficCode = CASE 
			WHEN dbo.IsOfficialYN(@TestCode, @DelvoResult, @BacteriaResult) = 'Y'
				THEN 'Y'
			ELSE 'N'
			END

	RETURN @OfficCode
END
GO

/****** Object:  UserDefinedFunction [dbo].[PadRight10]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[PadRight10] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Parm NVARCHAR(10)

	SELECT @Parm = CASE 
			WHEN len(@Parameter) < 7
				THEN right('000000000' + isnull(@Parameter, ''), 10)
			ELSE @Parameter
			END

	RETURN @Parm
END
GO

/****** Object:  UserDefinedFunction [dbo].[PadRight2]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[PadRight2] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Parm NVARCHAR(10)

	SELECT @Parm = CASE 
			WHEN len(@Parameter) < 2
				THEN right('00' + ISNULL(@Parameter, ''), 2)
			ELSE @Parameter
			END

	RETURN @Parm
END
GO

/****** Object:  UserDefinedFunction [dbo].[PadRight3]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[PadRight3] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Parm NVARCHAR(10)

	SELECT @Parm = CASE 
			WHEN len(@Parameter) < 3
				THEN right('000' + ISNULL(@Parameter, ''), 3)
			ELSE @Parameter
			END

	RETURN @Parm
END
GO

/****** Object:  UserDefinedFunction [dbo].[PadRight4]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[PadRight4] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Parm NVARCHAR(10)

	SELECT @Parm = CASE 
			WHEN len(@Parameter) < 4
				THEN right('0000' + isnull(@Parameter, ''), 4)
					--when len(@Parameter) < 4 then right('0000' + ISNULL(@Parameter, ''), 4)
			ELSE @Parameter
			END

	RETURN @Parm
END
GO

/****** Object:  UserDefinedFunction [dbo].[PadRight5]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[PadRight5] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Parm NVARCHAR(10)

	SELECT @Parm = CASE 
			WHEN len(@Parameter) < 5
				THEN right('00000' + isnull(@Parameter, ''), 5)
					--when len(@Parameter) < 5 then right('00000' + ISNULL(@Parameter, '0'), 5)
			ELSE '0'
			END

	RETURN @Parm
END
GO

/****** Object:  UserDefinedFunction [dbo].[PadRight6]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[PadRight6] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Parm NVARCHAR(10)

	SELECT @Parm = CASE 
			WHEN len(@Parameter) < 6
				THEN right('000000' + ISNULL(@Parameter, ''), 6)
			ELSE @Parameter
			END

	RETURN @Parm
END
GO

/****** Object:  UserDefinedFunction [dbo].[PadRight7]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[PadRight7] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Parm NVARCHAR(10)

	SELECT @Parm = CASE 
			WHEN len(@Parameter) < 7
				THEN right('000000' + isnull(@Parameter, ''), 7)
			ELSE @Parameter
			END

	RETURN @Parm
END
GO

/****** Object:  UserDefinedFunction [dbo].[PassZeroIfNull]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[PassZeroIfNull] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Parm NVARCHAR(10)

	SELECT @parm = CASE 
			WHEN @Parameter = NULL
				OR @Parameter IS NULL
				OR @Parameter = ''
				THEN '0'
			ELSE @Parameter
			END

	RETURN @Parm
END
GO

/****** Object:  UserDefinedFunction [dbo].[PlainviewCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[PlainviewCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A003732969Z6')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[ProducerOrLoadID]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- Function to report the producer Id or the Load ID
CREATE FUNCTION [dbo].[ProducerOrLoadID] (
	@ProducerCode AS NVARCHAR(20),
	@Barcode AS NVARCHAR(20),
	@CustomerCode AS NVARCHAR(20),
	@CustomerFramework AS NVARCHAR(20)
	)
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue NVARCHAR(20)

	--Marigold Loads
	IF (
			dbo.IsLoad(@CustomerFramework) = '1'
			AND dbo.MarigoldCustomerCodes(@CustomerCode) = '1'
			)
	BEGIN
		SET @ProducerCode = RIGHT(@Barcode, 9)
	END
			-- LOL
	ELSE IF (
			dbo.IsLoad(@CustomerFramework) = '1'
			AND dbo.LOLCustomerCodes(@CustomerCode) = '1'
			)
	BEGIN -- LOL has two different barcode for loads one is 10 digits the other is 12.
		SET @ProducerCode = CASE 
				WHEN len(@Barcode) = 12
					THEN substring(@Barcode, 2, 10) -- If barcode is 12 then truncate the leading 0 and the ending 2.
				ELSE @Barcode -- If Barcode is 10 report the entire barcode.
				END
	END
			-- Davisco Loads "LESUR" & "LAKEN"
	ELSE IF (
			dbo.IsLoad(@CustomerFramework) = '1'
			AND dbo.DaviscoCustomerCodes(@CustomerCode, @CustomerFramework) IN ('2', '3')
			)
	BEGIN
		SET @ProducerCode = RIGHT(@Barcode, 8)
	END
			-- DFA Loads
	ELSE IF (
			dbo.DFACustomerCodes(@CustomerCode, @CustomerFramework) IN ('1', '2')
			AND dbo.IsLoad(@CustomerFramework) = '1'
			)
	BEGIN
		SET @ProducerCode = SUBSTRING(@Barcode, 4, 7)
	END
			-- Saputo Loads.
	ELSE IF (
			dbo.SaputoCustomerCodes(@CustomerCode) = '1'
			AND dbo.IsLoad(@CustomerFramework) = '1'
			)
	BEGIN
		SET @ProducerCode = RIGHT(@Barcode, 8)
	END
			-- Agropur
	ELSE IF (
			dbo.AgropurCustomerCodes(@CustomerCode) = '1'
			AND dbo.IsLoad(@CustomerFramework) = '1'
			)
	BEGIN
		SET @ProducerCode = [dbo].[PadRight6](SUBSTRING(@Barcode, 5, 6))
	END
			-- Crop Loads
	ELSE IF (
			dbo.CropCustomerCodes(@CustomerCode) = '1'
			AND dbo.IsLoad(@CustomerFramework) = '1'
			)
	BEGIN
		SET @ProducerCode = [dbo].[PadRight6](SUBSTRING(@Barcode, 3, 6))
	END
			-- Ampi
	ELSE IF (
			dbo.AMPICustomerCodes(@CustomerCode) = '1'
			AND dbo.IsLoad(@CustomerFramework) = '1'
			)
	BEGIN
		SET @ProducerCode = SUBSTRING(@Barcode, 5, 6)
	END
			-- Schroeder
	ELSE IF (
			dbo.SchroederCustomerCodes(@CustomerCode) = '1'
			AND dbo.IsLoad(@CustomerFramework) = '1'
			)
	BEGIN
		SET @ProducerCode = RIGHT(@Barcode, 8)
	END
			--		-- Plainview
			--ELSE IF (
			--		dbo.PlainviewCustomerCodes(@CustomerCode) = '1'
			--		AND dbo.IsLoad(@CustomerFramework) = '1'
			--		)
			--BEGIN
			--	SET @ProducerCode = SUBSTRING(@Barcode, 5, 6)
			--END
	ELSE
	BEGIN
		RETURN @ProducerCode
	END

	SELECT @ReturnValue = @ProducerCode

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[QualityTestDate]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[QualityTestDate] (
	@ParameterCode AS NVARCHAR(20),
	@ValidationTime AS NVARCHAR(50),
	@MeasuredTime AS NVARCHAR(50),
	@CommercialItem AS NVARCHAR(50)
	)
RETURNS DATETIME2
AS
BEGIN
	DECLARE @date DATETIME2

	SELECT @date = CASE 
			WHEN @ParameterCode IN ('UM0004ZH', 'UMM004ZH', 'PA002W1', 'Z001JJPC')
				THEN coalesce(@MeasuredTime, @ValidationTime)
			END

	RETURN @date
END
GO

/****** Object:  UserDefinedFunction [dbo].[ReadingtonCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[ReadingtonCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00376007V80')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[RemoveCharacters]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[RemoveCharacters] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @FinalValue AS NVARCHAR(10)

	SELECT @FinalValue = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(@Parameter, ' ', ''), '<', ''), '>', ''), '-', ''), 'est', ''), '(', ''), ')', ''), 'e', ''), 'Error', ''), 'rror', '')

	--   when @Parameter like '% %' then replace(replace(@Parameter, ' ', ''), '<', '')
	--when @Parameter like '%<%' then replace(@Parameter, '<', '')
	--when @Parameter like '%>%' then replace(@Parameter, '>', '')
	--when @Parameter like '%-%' then replace(@Parameter, '-', '')
	--when @Parameter like '%(est)%' then replace(@Parameter, '(est)', '')
	--else @Parameter
	--end
	RETURN @FinalValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[RemoveDecPoint]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[RemoveDecPoint] (@Parameter AS DECIMAL(5, 2))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @FinalValue AS NVARCHAR(10)

	SELECT @FinalValue = replace(cast(@Parameter AS NVARCHAR(10)), '.', '')

	RETURN @FinalValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[SampleDateOrTestDate]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[SampleDateOrTestDate] (
	@SampleDate AS NVARCHAR(50),
	@TestDate AS DATETIME2
	)
RETURNS DATETIME2
AS
BEGIN
	DECLARE @date DATETIME2

	SELECT @date = CASE 
			WHEN (
					@SampleDate IS NULL
					OR len(@SampleDate) = 0
					)
				THEN @TestDate
			ELSE @SampleDate
			END

	RETURN @date
END
GO

/****** Object:  UserDefinedFunction [dbo].[SaputoCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[SaputoCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A005255738YD', 'A00292966GYW', 'A00525589AR1')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[ScenicCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[ScenicCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00525581RJR')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[SchroederCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[SchroederCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('AUS022732')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[StickneyCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[StickneyCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00148736')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[TestDate]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[TestDate] (
	@ParameterCode AS NVARCHAR(20),
	@ValidationTime AS NVARCHAR(50),
	@MeasuredTime AS NVARCHAR(50),
	@CommercialItem AS NVARCHAR(50)
	)
RETURNS DATETIME2
AS
BEGIN
	DECLARE @date DATETIME2

	SELECT @date = CASE 
			WHEN [dbo].[QualityTestDate](@ParameterCode, @ValidationTime, @MeasuredTime, @CommercialItem) IS NOT NULL
				OR [dbo].[QualityTestDate](@ParameterCode, @ValidationTime, @MeasuredTime, @CommercialItem) != ''
				THEN [dbo].[QualityTestDate](@ParameterCode, @ValidationTime, @MeasuredTime, @CommercialItem)
			WHEN [dbo].[QualityTestDate](@ParameterCode, @ValidationTime, @MeasuredTime, @CommercialItem) IS NULL
				OR [dbo].[QualityTestDate](@ParameterCode, @ValidationTime, @MeasuredTime, @CommercialItem) = ''
				THEN [dbo].[ComponentsTestDate](@ParameterCode, @ValidationTime, @MeasuredTime, @CommercialItem)
			END

	RETURN @date
END
GO

/****** Object:  UserDefinedFunction [dbo].[TestTypeIndicator]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[TestTypeIndicator] (
	@ParameterCode AS NVARCHAR(10),
	@BacteriaCount AS NVARCHAR(10)
	)
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @CommercialItem AS NVARCHAR(10)

	SELECT @CommercialItem = CASE 
			WHEN @ParameterCode = 'UM0004ZH'
				OR @BacteriaCount IS NOT NULL
				THEN '2'
			ELSE '1'
			END

	RETURN @CommercialItem
END
GO

/****** Object:  UserDefinedFunction [dbo].[Times10]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[Times10] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Result AS INT

	SELECT @Result = CASE 
			WHEN @Parameter IS NOT NULL
				THEN cast(@Parameter AS INT) * 10
			ELSE NULL
			END

	RETURN cast(@Result AS NVARCHAR(10))
END
GO

/****** Object:  UserDefinedFunction [dbo].[Times1000]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[Times1000] (@Parameter AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @Result AS INT

	SELECT @Result = CASE 
			WHEN @Parameter IS NOT NULL
				THEN cast(@Parameter AS INT) * 1000
			ELSE NULL
			END

	RETURN cast(@Result AS NVARCHAR(10))
END
GO

/****** Object:  UserDefinedFunction [dbo].[TotalSolids]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[TotalSolids] (
	@SNF AS DECIMAL(4, 2),
	@Fat AS DECIMAL(4, 2)
	)
RETURNS DECIMAL(4, 2)
AS
BEGIN
	DECLARE @TS DECIMAL(4, 2)

	SELECT @TS = CASE 
			WHEN @SNF IS NOT NULL
				AND @Fat IS NOT NULL
				THEN @SNF + @Fat
			END

	RETURN @TS
END
GO

/****** Object:  UserDefinedFunction [dbo].[UTCToLocalTime]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[UTCToLocalTime] (
	@Time AS NVARCHAR(50),
	@CommercialItem AS NVARCHAR(50)
	)
RETURNS DATETIME2
AS
BEGIN
	DECLARE @TimeDifferenceToUTC FLOAT

	IF (@CommercialItem LIKE 'MV%')
	BEGIN
		SET @TimeDifferenceToUTC = - 6
	END
	ELSE IF (@CommercialItem LIKE 'HO%')
	BEGIN
		SET @TimeDifferenceToUTC = - 5
	END
	ELSE IF (@CommercialItem LIKE 'FR%')
	BEGIN
		SET @TimeDifferenceToUTC = - 8
	END
	ELSE IF (@CommercialItem IS NULL)
	BEGIN
		SET @TimeDifferenceToUTC = - 6 -- Will return central time as default.
	END

	DECLARE @SecDifference FLOAT = - 1 * @TimeDifferenceToUTC * 3600
	DECLARE @ConvertedTime DATETIME2

	SELECT @ConvertedTime = dateadd(ss, - @SecDifference, cast(@Time AS DATETIME2))

	RETURN @ConvertedTime
END
GO

/****** Object:  UserDefinedFunction [dbo].[ValleyMilkCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[ValleyMilkCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00499178A6T')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  UserDefinedFunction [dbo].[Water]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[Water] (@FreezePoint AS NVARCHAR(10))
RETURNS NVARCHAR(10)
AS
BEGIN
	DECLARE @WaterResult DECIMAL(4, 2)

	SELECT @WaterResult = CASE 
			WHEN @FreezePoint IS NOT NULL
				AND cast(@FreezePoint AS INT) > 0
				AND cast(@FreezePoint AS INT) < 540
				THEN (540 - cast(@FreezePoint AS INT)) / 5.4
			END

	RETURN cast(@WaterResult AS NVARCHAR(10))
END
GO

/****** Object:  UserDefinedFunction [dbo].[WestbyCustomerCodes]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[WestbyCustomerCodes] (@CustomerCode AS NVARCHAR(20))
RETURNS NVARCHAR(20)
AS
BEGIN
	DECLARE @ReturnValue AS NVARCHAR(20)

	SELECT @ReturnValue = CASE 
			WHEN @CustomerCode IN ('A00384060ZNO')
				THEN '1'
			ELSE '0'
			END

	RETURN @ReturnValue
END
GO

/****** Object:  Table [dbo].[AgropurData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[AgropurData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [varchar](1) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](4) NOT NULL,
	[Fixed14] [varchar](4) NOT NULL,
	[Casein] [nvarchar](10) NULL,
	[FixedGeo] [nvarchar](4) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[AmpiData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[AmpiData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [nvarchar](20) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](1) NOT NULL,
	[Fixed14] [varchar](1) NOT NULL,
	[Casein] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[CaprineCentralData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CaprineCentralData] (
	[Prod #] [nvarchar](10) NULL,
	[Tank #] [nvarchar](5) NULL,
	[Sample Date] [nvarchar](20) NULL,
	[Test Date] [nvarchar](20) NULL,
	[ButterFat] [nvarchar](10) NULL,
	[Protein] [nvarchar](10) NULL,
	[OtherSolids] [nvarchar](10) NULL,
	[DMSCC] [nvarchar](20) NULL,
	[GI] [nvarchar](20) NULL,
	[CRYO] [nvarchar](20) NULL,
	[PI] [nvarchar](10) NULL,
	[Sampled Temp] [nvarchar](10) NULL,
	[PLC] [nvarchar](10) NULL,
	[MUN] [nvarchar](10) NULL,
	[LPC] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[CropData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CropData] (
	[ProducerCode] [nvarchar](20) NULL,
	[Tank] [nvarchar](20) NULL,
	[Seq] [nvarchar](20) NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](10) NULL,
	[IsOfficialYN] [nvarchar](4) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[AntiConfirmation] [nvarchar](4) NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[CropLoadData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[CropLoadData] (
	[ProducerCode] [nvarchar](20) NULL,
	[Blank1] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](10) NULL,
	[Blank2] [varchar](1) NOT NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Blank3] [varchar](1) NOT NULL,
	[Blank4] [varchar](1) NOT NULL,
	[Blank5] [varchar](1) NOT NULL,
	[Blank6] [varchar](1) NOT NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[CulturesData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CulturesData] (
	[CustomerCode] [nvarchar](100) NULL,
	[CustomerFramework] [nvarchar](100) NULL,
	[CFWInternalName] [nvarchar](150) NULL,
	[SampleID] [nvarchar](150) NULL,
	[CommercialOrderId] [nvarchar](150) NULL,
	[Barcode] [nvarchar](50) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[Tank] [nvarchar](5) NULL,
	[RegistrationTime] [nvarchar](200) NULL,
	[SamplingTime] [nvarchar](200) NULL,
	[MeasuredTime] [nvarchar](200) NULL,
	[ValidationTime] [nvarchar](200) NULL,
	[TestCode] [nvarchar](50) NULL,
	[TestName] [nvarchar](200) NULL,
	[ParameterCode] [nvarchar](100) NULL,
	[ParameterName] [nvarchar](200) NULL,
	[ResultValue] [nvarchar](100) NULL,
	[UoM] [nvarchar](20) NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[CustomerDetails]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CustomerDetails] (
	[CustomerCode] [nvarchar](100) NOT NULL,
	[Description] [nvarchar](250) NULL,
	[DeliveryMode] [nvarchar](10) NOT NULL,
	[eMails] [nvarchar](max) NULL,
	[FTPUrl] [nvarchar](200) NULL,
	[FTPUserName] [nvarchar](200) NULL,
	[FTPPassword] [nvarchar](200) NULL,
	[FTPType] [nvarchar](200) NULL,
	[SendFileName] [nvarchar](50) NULL,
	[IsFullName] [bit] NULL CONSTRAINT [isFullName_Default_0] DEFAULT((0)),
	[Id] [int] IDENTITY(1, 1) NOT NULL,
	[OverRidePreFTPChecks] [bit] NULL
	) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DaviscoData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[DaviscoData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [varchar](1) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](1) NOT NULL,
	[Fixed14] [varchar](1) NOT NULL,
	[Casein] [nvarchar](10) NULL,
	[FixedGeo] [nvarchar](4) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[DeansData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DeansData] (
	[Division] [nvarchar](20) NULL,
	[ProducerCode] [nvarchar](10) NULL,
	[Tank] [nvarchar](5) NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Somatic cells] [nvarchar](20) NULL,
	[Lactose] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[Other Solids] [nvarchar](10) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](10) NULL,
	[INH] [nvarchar](4) NULL,
	[PI] [nvarchar](10) NULL,
	[LPC] [nvarchar](10) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fixed1] [nvarchar](10) NOT NULL,
	[Blank] [nvarchar](10) NOT NULL,
	[Seq] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DFAData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[DFAData] (
	[SampleDate] [nvarchar](20) NULL,
	[Division] [nvarchar](20) NULL,
	[ProducerCode] [nvarchar](10) NULL,
	[SampleID] [nvarchar](100) NULL,
	[Fixed1] [varchar](8) NOT NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](10) NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Fixed4] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](10) NULL,
	[AntiCharConfirmationNP] [nvarchar](4) NULL,
	[Sediment] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[BacteriaTestType1] [varchar](1) NOT NULL,
	[BacteriaValue1] [nvarchar](10) NULL,
	[BacteriaTestType2] [varchar](1) NOT NULL,
	[BacteriaValue2] [nvarchar](10) NULL,
	[BacteriaTestType3] [varchar](1) NOT NULL,
	[BacteriaValue3] [nvarchar](10) NULL,
	[Blank1] [varchar](1) NOT NULL,
	[Tank] [nvarchar](10) NULL,
	[Barcode] [nvarchar](50) NULL,
	[Seq] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[DFAEastData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[DFAEastData] (
	[SampleDate] [nvarchar](20) NULL,
	[Division] [nvarchar](20) NULL,
	[ProducerCode] [nvarchar](10) NULL,
	[SampleID] [nvarchar](100) NULL,
	[Fixed1] [varchar](8) NOT NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](10) NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Fixed4] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](10) NULL,
	[AntiCharConfirmationNP] [nvarchar](4) NULL,
	[Sediment] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[BacteriaTestType1] [varchar](1) NOT NULL,
	[BacteriaValue1] [nvarchar](10) NULL,
	[BacteriaTestType2] [varchar](1) NOT NULL,
	[BacteriaValue2] [nvarchar](10) NULL,
	[BacteriaTestType3] [varchar](1) NOT NULL,
	[BacteriaValue3] [nvarchar](10) NULL,
	[Blank1] [varchar](1) NOT NULL,
	[Tank] [nvarchar](10) NULL,
	[Barcode] [nvarchar](50) NULL,
	[Seq] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[ForemostData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ForemostData] (
	[ProducerCode] [nvarchar](10) NULL,
	[Tank] [nvarchar](5) NULL,
	[Seq] [nvarchar](3) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[SampleDate] [nvarchar](10) NULL,
	[TestDate] [nvarchar](10) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Fixed1] [varchar](3) NOT NULL,
	[Water] [nvarchar](10) NULL,
	[AntiCharConfirmation] [nvarchar](4) NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](4) NOT NULL,
	[AlternateID] [nvarchar](20) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[TestTime] [nvarchar](20) NULL,
	[Fixed4] [varchar](2) NOT NULL,
	[Blank1] [varchar](1) NOT NULL,
	[Blank2] [varchar](1) NOT NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[GrandeCheeseData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[GrandeCheeseData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [nvarchar](20) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](1) NOT NULL,
	[Fixed14] [varchar](1) NOT NULL,
	[Casein] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[HorizonOrganicData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[HorizonOrganicData] (
	[Member #] [nvarchar](10) NULL,
	[Location] [nvarchar](5) NULL,
	[Member Name] [nvarchar](255) NULL,
	[Sample Date] [nvarchar](20) NULL,
	[SCC] [nvarchar](10) NOT NULL,
	[OSCC] [nvarchar](10) NOT NULL,
	[ESCC] [nvarchar](10) NULL,
	[PLC] [nvarchar](10) NULL,
	[PLC ACT] [nvarchar](10) NULL,
	[SPC] [nvarchar](10) NOT NULL,
	[SPC ACT] [nvarchar](10) NOT NULL,
	[PI] [nvarchar](20) NULL,
	[LPC] [nvarchar](20) NULL,
	[COLI] [nvarchar](20) NULL,
	[E COLI] [nvarchar](10) NOT NULL,
	[CRYO] [nvarchar](10) NULL,
	[DMC] [nvarchar](10) NOT NULL,
	[FAT] [nvarchar](10) NULL,
	[PROT] [nvarchar](10) NULL,
	[MUN] [nvarchar](10) NULL,
	[SNF] [nvarchar](10) NULL,
	[SEDI] [nvarchar](20) NULL,
	[GI] [nvarchar](10) NOT NULL,
	[TEMP] [nvarchar](10) NULL,
	[LAC] [nvarchar](10) NULL,
	[TS] [nvarchar](10) NULL,
	[OSOL] [nvarchar](10) NULL,
	[Lab Unique ID] [nvarchar](50) NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[LakenLoadsData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[LakenLoadsData] (
	[Manifest#] [nvarchar](20) NULL,
	[SampleDate] [nvarchar](20) NULL,
	[Fat] [nvarchar](20) NULL,
	[Protein] [nvarchar](20) NULL,
	[Lactose] [nvarchar](20) NULL,
	[Cells(Somatic)] [nvarchar](20) NULL,
	[BacteriaCount] [nvarchar](10) NULL,
	[FreezingPoint] [nvarchar](20) NULL,
	[OtherSolids] [nvarchar](20) NULL,
	[TotalSolids] [nvarchar](20) NULL,
	[Casein] [nvarchar](20) NULL,
	[LPC] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[LesueurLoadsData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[LesueurLoadsData] (
	[Manifest#] [nvarchar](20) NULL,
	[SampleDate] [nvarchar](20) NULL,
	[Fat] [nvarchar](20) NULL,
	[Protein] [nvarchar](20) NULL,
	[Lactose] [nvarchar](20) NULL,
	[Cells(Somatic)] [nvarchar](20) NULL,
	[BacteriaCount] [nvarchar](20) NULL,
	[FreezingPoint] [nvarchar](20) NULL,
	[OtherSolids] [nvarchar](20) NULL,
	[TotalSolids] [nvarchar](20) NULL,
	[Casein] [nvarchar](20) NULL,
	[LPC] [nvarchar](20) NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[LOLData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[LOLData] (
	[ProducerCode] [nvarchar](10) NULL,
	[Tank] [nvarchar](5) NULL,
	[Seq] [nvarchar](3) NULL,
	[Fixed1] [nvarchar](10) NOT NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[Fixed2] [nvarchar](10) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[Total Solids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](10) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](10) NULL,
	[Base FP] [nvarchar](10) NOT NULL,
	[Water] [nvarchar](10) NULL,
	[AntiCharConfirmation] [nvarchar](4) NULL,
	[Sediment] [nvarchar](20) NULL,
	[Blank1] [nvarchar](10) NOT NULL,
	[Blank2] [nvarchar](10) NOT NULL,
	[RecordIndicator] [nvarchar](10) NULL,
	[Fixed3] [nvarchar](10) NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[DMCFixed] [nvarchar](10) NOT NULL,
	[Blank3] [nvarchar](10) NOT NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[M2MFileData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[M2MFileData] (
	[Id] [uniqueidentifier] NULL,
	[CustomerCode] [nvarchar](max) NULL,
	[CustomerFramework] [nvarchar](max) NULL,
	[RowStatus] [int] NOT NULL CONSTRAINT [RowStatus_New] DEFAULT((0)),
	[SampleID] [nvarchar](max) NULL,
	[Barcode] [nvarchar](max) NULL,
	[CommercialOrderId] [nvarchar](max) NULL,
	[RegistrationTime] [nvarchar](max) NULL,
	[SamplingTime] [nvarchar](max) NULL,
	[ProducerCode] [nvarchar](max) NULL,
	[HerdId] [nvarchar](max) NULL,
	[AnimalId] [nvarchar](max) NULL,
	[AnalysisStatus] [nvarchar](max) NULL,
	[TestName] [nvarchar](max) NULL,
	[TestCode] [nvarchar](max) NULL,
	[ParameterName] [nvarchar](max) NULL,
	[ParameterCode] [nvarchar](max) NULL,
	[ResultValue] [nvarchar](max) NULL,
	[UoM] [nvarchar](max) NULL,
	[Reason] [nvarchar](max) NULL,
	[Comments] [nvarchar](max) NULL,
	[ResultStatus] [nvarchar](max) NULL,
	[CommercialOrderLineStatus] [nvarchar](max) NULL,
	[CommercialOrderStatus] [nvarchar](max) NULL,
	[CancellationReason] [nvarchar](max) NULL,
	[RetestPerformed] [nvarchar](max) NULL,
	[RegistrationTemperature] [nvarchar](max) NULL,
	[SamplingTemperature] [nvarchar](max) NULL,
	[ValidationTime] [nvarchar](max) NULL,
	[MeasuredTime] [nvarchar](max) NULL,
	[Company] [nvarchar](max) NULL,
	[Division] [nvarchar](max) NULL,
	[BTUNumber] [nvarchar](max) NULL,
	[AlternateID] [nvarchar](max) NULL,
	[PermitNumber] [nvarchar](max) NULL,
	[Tank] [nvarchar](max) NULL,
	[UncertaintyValues] [nvarchar](max) NULL,
	[MethodReference] [nvarchar](max) NULL,
	[BillingWeek] [nvarchar](max) NULL,
	[SampleTaker] [nvarchar](max) NULL,
	[FinalFileName] [nvarchar](max) NULL,
	[RouteNumber] [nvarchar](max) NULL,
	[ProducerName] [nvarchar](max) NULL,
	[CFWInternalName] [nvarchar](max) NULL,
	[ProducerGrade] [nvarchar](max) NULL,
	[RecordReceived] [datetime2](3) NULL CONSTRAINT [DF_M2MFileData_Created] DEFAULT(sysdatetime()),
	[LastModified] [datetime2](3) NULL,
	[OfficialDataSent] [bit] NULL,
	[CultureDataSent] [bit] NULL
	) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

/****** Object:  Table [dbo].[MarigoldData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[MarigoldData] (
	[CustomerCode] [nvarchar](50) NULL,
	[ProducerCode] [nvarchar](14) NULL,
	[Blank1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](50) NULL,
	[TestDate] [nvarchar](50) NULL,
	[Blank2] [varchar](1) NOT NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[Other Solids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[NDPEastData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[NDPEastData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](10) NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [nvarchar](20) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](1) NOT NULL,
	[Fixed14] [varchar](1) NOT NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[NFOData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[NFOData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [nvarchar](20) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](1) NOT NULL,
	[Fixed14] [varchar](1) NOT NULL,
	[Casein] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[OfficialsData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[OfficialsData] (
	[ProducerCode] [nvarchar](20) NULL,
	[Tank] [nvarchar](20) NULL,
	[Seq] [nvarchar](max) NULL,
	[Barcode] [nvarchar](50) NULL,
	[SampleDate] [nvarchar](150) NULL,
	[TestDate] [nvarchar](150) NULL,
	[ComponentsTestDate] [nvarchar](150) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[Other Solids] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](10) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Delvo Inhibitor] [nvarchar](50) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Analysis Temperature] [nvarchar](10) NULL,
	[CustomerCode] [nvarchar](150) NULL,
	[CustomerFramework] [nvarchar](150) NULL,
	[OfficialCode] [nvarchar](55) NULL
	) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

/****** Object:  Table [dbo].[PlainviewData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[PlainviewData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [nvarchar](20) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](1) NOT NULL,
	[Fixed14] [varchar](1) NOT NULL,
	[Casein] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[ReadingtonData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ReadingtonData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [nvarchar](20) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](1) NOT NULL,
	[Fixed14] [varchar](1) NOT NULL,
	[Casein] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[ReportTransactions]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ReportTransactions] (
	[CustomerCode] [nvarchar](100) NOT NULL,
	[Mode] [nvarchar](10) NOT NULL,
	[emailList] [nvarchar](max) NULL,
	[FTPURL] [nvarchar](200) NULL,
	[FTPType] [nvarchar](200) NULL,
	[FTPUserName] [nvarchar](200) NULL,
	[FTPPassword] [nvarchar](200) NULL,
	[FileNameToSend] [nvarchar](max) NULL,
	[ActionName] [nvarchar](100) NULL,
	[LastUpdatedDate] [datetime] NOT NULL,
	[SendFileName] [nvarchar](255) NULL,
	[IsFullName] [bit] NULL,
	[OverRidePreFTPChecks] [bit] NULL,
	[CustomerSpecificFileName] [nvarchar](500) NULL
	) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

/****** Object:  Table [dbo].[RockDellData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[RockDellData] (
	[Company] [nvarchar](20) NULL,
	[Division] [nvarchar](20) NULL,
	[ProducerCode] [nvarchar](10) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed] [nvarchar](10) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](20) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Blank] [nvarchar](10) NOT NULL,
	[AntiCharConformation] [nvarchar](10) NULL,
	[Sediment] [nvarchar](20) NULL,
	[Blank1] [nvarchar](10) NOT NULL,
	[Tank] [nvarchar](5) NULL,
	[Blank0] [nvarchar](10) NOT NULL,
	[Fixed0] [nvarchar](10) NOT NULL,
	[Coliforms] [nvarchar](20) NULL,
	[Thermoduric Plate Count] [nvarchar](20) NULL,
	[Bacteria Count(PI)] [nvarchar](20) NULL,
	[Blank2] [nvarchar](10) NOT NULL,
	[Seq] [nvarchar](4) NULL,
	[LoadId] [nvarchar](10) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[Fixed1] [nvarchar](10) NOT NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed2] [nvarchar](10) NOT NULL,
	[Blank3] [nvarchar](10) NOT NULL,
	[Blank4] [nvarchar](10) NOT NULL,
	[Blank5] [nvarchar](10) NOT NULL,
	[Blank6] [nvarchar](10) NOT NULL,
	[Blank7] [nvarchar](10) NOT NULL,
	[Casein] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[SaputoData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[SaputoData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [nvarchar](20) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](1) NOT NULL,
	[Fixed14] [varchar](1) NOT NULL,
	[Casein] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[SaputoLoadsData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[SaputoLoadsData] (
	[PRODUCER] [nvarchar](14) NULL,
	[SAMPLE DATE] [nvarchar](50) NULL,
	[TEST DATE] [nvarchar](50) NULL,
	[OFFICIAL] [varchar](1) NOT NULL,
	[FAT] [decimal](5, 2) NULL,
	[PROTEIN] [decimal](5, 2) NULL,
	[LAC] [decimal](5, 2) NULL,
	[SNF] [decimal](5, 2) NULL,
	[OS] [nvarchar](20) NULL,
	[ESCC] [nvarchar](20) NULL,
	[PLC] [nvarchar](20) NULL,
	[FP] [nvarchar](10) NULL,
	[%WATER] [nvarchar](10) NULL,
	[INB] [nvarchar](4) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[ScenicData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ScenicData] (
	[Company] [nvarchar](10) NULL,
	[Division] [nvarchar](10) NULL,
	[ProducerCode] [nvarchar](20) NULL,
	[IsLoad] [nvarchar](2) NULL,
	[Fixed1] [varchar](1) NOT NULL,
	[SampleDate] [nvarchar](20) NULL,
	[TestDate] [nvarchar](20) NULL,
	[OfficialCode] [nvarchar](5) NULL,
	[Fat] [nvarchar](10) NULL,
	[True Protein] [nvarchar](10) NULL,
	[Lactose] [nvarchar](10) NULL,
	[Solids not fat (SNF)] [nvarchar](10) NULL,
	[TotalSolids] [nvarchar](10) NULL,
	[Somatic Cells] [nvarchar](20) NULL,
	[SamplingTemperature] [nvarchar](10) NULL,
	[Bacteria Count] [nvarchar](10) NULL,
	[Freezing Point] [nvarchar](20) NULL,
	[Water] [nvarchar](10) NULL,
	[Fixed2] [varchar](1) NOT NULL,
	[Fixed3] [varchar](1) NOT NULL,
	[Sediment] [nvarchar](20) NULL,
	[Fixed4] [varchar](1) NOT NULL,
	[Tank] [nvarchar](20) NULL,
	[Fixed5] [varchar](1) NOT NULL,
	[Fixed6] [varchar](4) NOT NULL,
	[Coliforms] [nvarchar](10) NULL,
	[Thermoduric Plate Count] [nvarchar](10) NULL,
	[Bacteria Count(PI)] [nvarchar](10) NULL,
	[Fixed7] [varchar](1) NOT NULL,
	[Seq] [nvarchar](20) NULL,
	[LoadId] [nvarchar](20) NULL,
	[Milk Urea Nitrogen (MUN)] [nvarchar](10) NULL,
	[AntiNumConfirmation] [nvarchar](2) NULL,
	[Fixed8] [varchar](1) NOT NULL,
	[ComponentsTestDate] [nvarchar](20) NULL,
	[Fixed9] [varchar](1) NOT NULL,
	[Fixed10] [varchar](1) NOT NULL,
	[Fixed11] [varchar](1) NOT NULL,
	[Fixed12] [varchar](1) NOT NULL,
	[Fixed13] [varchar](1) NOT NULL,
	[Fixed14] [varchar](1) NOT NULL,
	[Casein] [nvarchar](10) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[SchroederData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[SchroederData] (
	[PRODUCER] [nvarchar](14) NULL,
	[SAMPLE DATE] [nvarchar](50) NULL,
	[TEST DATE] [nvarchar](50) NULL,
	[OFFICIAL] [varchar](1) NOT NULL,
	[FAT] [decimal](5, 2) NULL,
	[PROTEIN] [decimal](5, 2) NULL,
	[LAC] [decimal](5, 2) NULL,
	[SNF] [decimal](5, 2) NULL,
	[OS] [nvarchar](20) NULL,
	[ESCC] [nvarchar](20) NULL,
	[PLC] [nvarchar](20) NULL,
	[FP] [nvarchar](10) NULL,
	[%WATER] [nvarchar](10) NULL,
	[INB] [nvarchar](4) NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[StickneyData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[StickneyData] (
	[PRODUCER] [nvarchar](10) NULL,
	[TANK] [nvarchar](5) NULL,
	[SEQ] [nvarchar](3) NULL,
	[ROUTE] [nvarchar](10) NULL,
	[SAMPLE DATE] [nvarchar](20) NULL,
	[TEST DATE] [nvarchar](20) NULL,
	[OFFICIAL] [nvarchar](4) NULL,
	[FAT] [nvarchar](10) NULL,
	[PROTEIN] [nvarchar](10) NULL,
	[LACTOSE] [nvarchar](10) NULL,
	[SNF] [nvarchar](10) NULL,
	[OS] [nvarchar](10) NULL,
	[ESCC] [nvarchar](20) NULL,
	[PLC] [nvarchar](20) NULL,
	[PI] [nvarchar](20) NULL,
	[MUN] [nvarchar](10) NULL,
	[COLI] [nvarchar](20) NULL,
	[LPC] [nvarchar](20) NULL,
	[FP] [nvarchar](20) NULL,
	[%WATER] [nvarchar](10) NULL,
	[IHB] [nvarchar](4) NULL
	) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[ValleyMilkData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[ValleyMilkData] (
	[BARCODE] [nvarchar](20) NULL,
	[DATE] [nvarchar](20) NULL,
	[FAT] [nvarchar](20) NULL,
	[PROTEIN] [nvarchar](20) NULL,
	[SNF] [nvarchar](20) NULL,
	[LACTOSE] [nvarchar](20) NULL,
	[SCC] [nvarchar](10) NULL,
	[LPC] [nvarchar](10) NULL,
	[COLI] [nvarchar](10) NULL,
	[SPC] [nvarchar](10) NULL,
	[TATS] [varchar](1) NOT NULL,
	[FSS] [varchar](1) NOT NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[WestbyData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[WestbyData] (
	[Producer Plant] [nvarchar](50) NULL,
	[Producer No] [nvarchar](50) NULL,
	[Unit Id] [nvarchar](10) NULL,
	[Grade] [nvarchar](10) NULL,
	[Record Date] [nvarchar](50) NULL,
	[Sample Date] [nvarchar](50) NULL,
	[TimeTested] [nvarchar](50) NULL,
	[SampleTemperature] [nvarchar](10) NULL,
	[BF] [nvarchar](10) NULL,
	[PT] [nvarchar](10) NULL,
	[LT] [nvarchar](10) NULL,
	[OS] [nvarchar](10) NULL,
	[TS] [nvarchar](10) NULL,
	[SC] [nvarchar](10) NULL,
	[OSSC] [nvarchar](10) NULL,
	[PC] [nvarchar](10) NULL,
	[PLC] [nvarchar](10) NULL,
	[IsAntibioticTested] [varchar](5) NULL,
	[IsAntibioticPos] [varchar](5) NOT NULL,
	[Cryoscope] [nvarchar](10) NULL,
	[Sediment] [nvarchar](10) NULL,
	[PI] [nvarchar](10) NULL,
	[CS] [nvarchar](10) NULL,
	[MUN] [nvarchar](10) NULL,
	[IsOfficial] [varchar](5) NOT NULL,
	[Lab Id] [varchar](10) NOT NULL,
	[Lab Test Id] [nvarchar](50) NULL,
	[IsInvalid] [varchar](5) NOT NULL
	) ON [PRIMARY]
GO

SET ANSI_PADDING OFF
GO

/****** Object:  StoredProcedure [dbo].[GetAgropurData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetAgropurData]
AS
--Agropur DSI File Format
SET NOCOUNT OFF

TRUNCATE TABLE dbo.AgropurData

SELECT *
INTO #TempAgropur1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[AgropurCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempAgropur1
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
	[dbo].[AntiNumConfirmation]([Delvo Inhibitor]) AntiNumConfirmation,
	[dbo].[GeoMeanFlag](TestCode, [Delvo Inhibitor], [Bacteria Count]) FixedGeo
INTO #TempAgropur2
FROM #TempAgropur1
WHERE len(Barcode) >= 10

--select * from #TempAgropur2
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
	max(AntiNumConfirmation) AntiNumConfirmation,
	max(FixedGeo) FixedGeo
INTO #TempAgropur3
FROM #TempAgropur2
GROUP BY Barcode

--select * from #TempAgropur3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT dbo.PadRight2(Company) Company,
	dbo.PadRight3(Division) Division,
	dbo.ProducerOrLoadId(ProducerCode, Barcode, CustomerCode, CustomerFramework) ProducerCode,
	'' IsLoad, --[dbo].IsLoad(CustomerFramework) IsLoad,
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
	'7134' Fixed13,
	'7134' Fixed14,
	[dbo].[RemoveDecPoint](Casein) Casein,
	FixedGeo
INTO #TempAgropur4
FROM #TempAgropur3

--select * into dbo.AgropurData from #TempAgropur4 -- Use this if the table is deleted.
INSERT INTO dbo.AgropurData
SELECT *
FROM #TempAgropur4

DROP TABLE #TempAgropur1

DROP TABLE #TempAgropur2

DROP TABLE #TempAgropur3

DROP TABLE #TempAgropur4

--select ?=count(1) from dbo.AgropurData   
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT ON
	--SELECT *
	--FROM dbo.AgropurData
	--WHERE len(ProducerCode) > 0
	--ORDER BY ProducerCode,
	--	Tank,
	--	SampleDate
GO

/****** Object:  StoredProcedure [dbo].[GetCultureData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetCultureData] @dataFor AS NVARCHAR(20)
AS
SET NOCOUNT ON;

TRUNCATE TABLE CulturesData;

-- Get data for AMPI only, otherwise get the data fro the rest of the clients.
IF (@dataFor = 'AMPI')
BEGIN
	SELECT *
	INTO #TempCulturesData1
	FROM M2MFileData
	WHERE TestCode IN ('MVQVP22', 'MVQVP22b', 'MVQVP22m', 'MVQVP22mb', 'MVQVP22p', 'MVQVP22pb', 'MVQVP22pm', 'MVQVP22pmb')
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

--UPDATE M2M
--SET M2M.CultureDataSent = 1
--FROM M2MFileData M2M
--INNER JOIN CulturesData CD ON (
--		M2M.Barcode = CD.Barcode
--		AND M2M.SamplingTime = CD.SamplingTime
--		);
SELECT *
FROM CulturesData
ORDER BY CustomerCode,
	ProducerCode,
	Tank,
	SamplingTime;
GO

/****** Object:  StoredProcedure [dbo].[GetDaviscoData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetDaviscoData]
AS
--Davisco DSI File Format
SET NOCOUNT ON

TRUNCATE TABLE dbo.DaviscoData

SELECT *
INTO #TempDavisco1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliforms, Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[DaviscoCustomerCodes](CustomerCode, CustomerFramework) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempDavisco1
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
	coalesce(Coliform, Coliforms) Coliforms,
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
	[dbo].[AntiNumConfirmation]([Delvo Inhibitor]) AntiNumConfirmation,
	[dbo].[GeoMeanFlag](TestCode, [Delvo Inhibitor], [Bacteria Count]) FixedGeo
INTO #TempDavisco2
FROM #TempDavisco1
WHERE len(Barcode) >= 10

--select * from #TempDavisco2
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
	max(AntiNumConfirmation) AntiNumConfirmation,
	max(FixedGeo) FixedGeo
INTO #TempDavisco3
FROM #TempDavisco2
GROUP BY Barcode

--select * from #TempDavisco3
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
	[dbo].[RemoveDecPoint](Casein) Casein,
	FixedGeo
INTO #TempDavisco4
FROM #TempDavisco3

--select * into dbo.DaviscoData from #TempDavisco4 -- Use this if the table is deleted.
INSERT INTO dbo.DaviscoData
SELECT *
FROM #TempDavisco4

DROP TABLE #TempDavisco1

DROP TABLE #TempDavisco2

DROP TABLE #TempDavisco3

DROP TABLE #TempDavisco4

-- select ? = count(1) from dbo.DaviscoData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF
	--SELECT *
	--FROM dbo.DaviscoData
	--WHERE len(ProducerCode) > 0
	--ORDER BY ProducerCode,
	--	Tank,
	--	SampleDate
GO

/****** Object:  StoredProcedure [dbo].[GetDeansData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetDeansData]
AS
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
	Coliform AS Coliforms,
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
	--SELECT *
	--FROM DeansData
	--WHERE len(ProducerCode) > 0
	--ORDER BY ProducerCode,
	--	Tank,
	--	SampleDate,
	--	Seq
GO

/****** Object:  StoredProcedure [dbo].[GetDFAData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetDFAData]
AS
-- DFA File Format.
SET NOCOUNT ON

TRUNCATE TABLE dbo.DFAData

-- Remove previous data.
SELECT *
INTO #TempDFA1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE (
		[dbo].[DFACustomerCodes](CustomerCode, CustomerFramework) = '1'
		AND RowStatus = 0
		)
	OR (
		[dbo].[DFACustomerCodes](CustomerCode, CustomerFramework) = '2'
		AND RowStatus = 1
		)
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempDFA1
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
INTO #TempDFA2
FROM #TempDFA1
WHERE len(Barcode) >= 10

--select * from #TempDFA2
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
INTO #TempDFA3
FROM #TempDFA2
GROUP BY Barcode

--select * from #TempDFA3
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
	CustomerFramwork,
	ParameterCode
INTO #TempDFA4
FROM #TempDFA3

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE #TempDFA4
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

UPDATE #TempDFA4
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

UPDATE #TempDFA4
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

--select * from #TempDFA4
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
INTO #TempDFA5
FROM #TempDFA4

--select * into dbo.DFAData from #TempDFA5
INSERT INTO dbo.DFAData
SELECT *
FROM #TempDFA5

DROP TABLE #TempDFA1

DROP TABLE #TempDFA2

DROP TABLE #TempDFA3

DROP TABLE #TempDFA4

DROP TABLE #TempDFA5

--select ? = count(1)  from dbo.DFAData
---------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF
	--SELECT *
	--FROM dbo.DFADAta
	--WHERE len(ProducerCode) > 0
	--ORDER BY ProducerCode,
	--	Tank,
	--	SampleDate
GO

/****** Object:  StoredProcedure [dbo].[GetDFAEastData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetDFAEastData]
AS
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
	--SELECT *
	--FROM dbo.DFAEastData
	--WHERE len(ProducerCode) > 0
	--ORDER BY ProducerCode,
	--	Tank,
	--	SampleDate
GO

/****** Object:  StoredProcedure [dbo].[GetLakenLoads]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetLakenLoads]
AS
-- Laken loads File Format (Davisco Loads)
SET NOCOUNT ON

TRUNCATE TABLE dbo.LakenLoadsData

SELECT *
INTO #TempLakenLoads1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[DaviscoCustomerCodes](CustomerCode, CustomerFramework) = '2'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempLakenLoads1
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
	right(Barcode, 4) Seq,
	'' LoadId,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	CustomerCode,
	[dbo].[CastAsDecNullValues]([Other Solids]) [Other Solids],
	[dbo].[ComponentsTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) ComponentsTestDate,
	Barcode,
	[Delvo Inhibitor],
	ParameterCode,
	TestCode
INTO #TempLakenLoads2
FROM #TempLakenLoads1
WHERE len(Barcode) >= 10

--select * from #TempLakenLoads2
----------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(Fat) Fat,
	Max([True Protein]) [True Protein],
	max(Lactose) Lactose,
	max([Solids not fat (SNF)]) [Solids not fat (SNF)],
	Barcode,
	max(SampleDate) SampleDate,
	min(TestDate) TestDate,
	max([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	max([Other Solids]) [Other Solids],
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
INTO #TempLakenLoads3
FROM #TempLakenLoads2
GROUP BY Barcode

--select * from #TempLakenLoads3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT dbo.ProducerOrLoadID(ProducerCode, Barcode, CustomerCode, CustomerFramework) Manifest#,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(Fat AS NVARCHAR(20)) Fat,
	cast([True Protein] AS NVARCHAR(20)) Protein,
	cast(Lactose AS NVARCHAR(20)) Lactose,
	[Somatic Cells] [Cells(Somatic)],
	dbo.RemoveCharacters([Bacteria Count]) BacteriaCount,
	[Freezing Point] FreezingPoint,
	cast([Other Solids] AS NVARCHAR(20)) OtherSolids,
	cast(dbo.TotalSolids([Solids not fat (SNF)], [True Protein]) AS NVARCHAR(20)) TotalSolids,
	cast(Casein AS NVARCHAR(20)) Casein,
	dbo.RemoveCharacters([Thermoduric Plate Count]) LPC
INTO #TempLakenLoads4
FROM #TempLakenLoads3

--select * from #TempLakenLoads4
------------------------------------------------------------------------------------------------
-- Use this if the table is deleted.
--select *
--into dbo.LakenLoadsData
--from #TempLakenLoads4
INSERT INTO dbo.LakenLoadsData
SELECT *
FROM #TempLakenLoads4

DROP TABLE #TempLakenLoads1

DROP TABLE #TempLakenLoads2

DROP TABLE #TempLakenLoads3

DROP TABLE #TempLakenLoads4

-- select ? = count(1) from dbo.LakenLoadsData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF
	--SELECT *
	--FROM dbo.LakenLoadsData
	--WHERE len(Manifest#) > 0
	--ORDER BY Manifest#,
	--	SampleDate
GO

/****** Object:  StoredProcedure [dbo].[GetLesueurLoadsData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetLesueurLoadsData]
AS
-- Lesueur loads File Format (Davisco Loads)
SET NOCOUNT ON

TRUNCATE TABLE dbo.LesueurLoadsData

SELECT *
INTO #TempLesueurLoads1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[DaviscoCustomerCodes](CustomerCode, CustomerFramework) = '3'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempLesueurLoads1
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
	right(Barcode, 4) Seq,
	'' LoadId,
	[dbo].[CastAsDecNullValues]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	CustomerCode,
	[dbo].[CastAsDecNullValues]([Other Solids]) [Other Solids],
	[dbo].[ComponentsTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) ComponentsTestDate,
	Barcode,
	[Delvo Inhibitor],
	ParameterCode,
	TestCode
INTO #TempLesueurLoads2
FROM #TempLesueurLoads1
WHERE len(Barcode) >= 10

--select * from #TempLesueurLoads2
----------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(Fat) Fat,
	Max([True Protein]) [True Protein],
	max(Lactose) Lactose,
	max([Solids not fat (SNF)]) [Solids not fat (SNF)],
	Barcode,
	max(SampleDate) SampleDate,
	min(TestDate) TestDate,
	max([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	max([Other Solids]) [Other Solids],
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
INTO #TempLesueurLoads3
FROM #TempLesueurLoads2
GROUP BY Barcode

--select * from #TempLesueurLoads3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT dbo.ProducerOrLoadID(ProducerCode, Barcode, CustomerCode, CustomerFramework) Manifest#,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(Fat AS NVARCHAR(20)) Fat,
	cast([True Protein] AS NVARCHAR(20)) Protein,
	cast(Lactose AS NVARCHAR(20)) Lactose,
	[Somatic Cells] [Cells(Somatic)],
	dbo.RemoveCharacters([Bacteria Count]) BacteriaCount,
	[Freezing Point] FreezingPoint,
	cast([Other Solids] AS NVARCHAR(20)) OtherSolids,
	cast(dbo.TotalSolids([Solids not fat (SNF)], [True Protein]) AS NVARCHAR(20)) TotalSolids,
	cast(Casein AS NVARCHAR(20)) Casein,
	dbo.RemoveCharacters([Thermoduric Plate Count]) LPC
INTO #TempLesueurLoads4
FROM #TempLesueurLoads3

--select *
--into dbo.LesueurLoadsData
--from #TempLesueurLoads4
INSERT INTO dbo.LesueurLoadsData
SELECT *
FROM #TempLesueurLoads4

DROP TABLE #TempLesueurLoads1

DROP TABLE #TempLesueurLoads2

DROP TABLE #TempLesueurLoads3

DROP TABLE #TempLesueurLoads4

-- select ? = count(1) from dbo.LesueurLoadsData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF
	--SELECT *
	--FROM dbo.LesueurLoadsData
	--WHERE len(Manifest#) > 0
	--ORDER BY Manifest#,
	--	SampleDate
GO

/****** Object:  StoredProcedure [dbo].[GetLOLData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetLOLData]
AS
-- LOL file format.
SET NOCOUNT OFF

TRUNCATE TABLE dbo.LOLdata

SELECT *
INTO #TempLOL1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliform, [Bulk Tank Coliforms], [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE [dbo].[LOLCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempLOL
-------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT ProducerCode,
	Tank,
	substring(Barcode, 9, 3) Seq,
	[dbo].[CastAsDecLOL]([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecLOL](Fat) Fat,
	[dbo].[CastAsDecLOL]([True Protein]) [True Protein],
	[dbo].[CastAsDecLOL](Lactose) Lactose,
	[dbo].[CastAsDecLOL]([Other Solids]) [Other Solids],
	[Somatic cells],
	SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	[Thermoduric Plate Count],
	[Bacteria Count(PI)],
	Barcode,
	[dbo].[OfficialCode](TestCode, [Delvo Inhibitor], [Bacteria Count]) OfficialCode,
	[dbo].[CastAsDecLOL]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	ParameterCode,
	TestCode,
	[Delvo Inhibitor],
	coalesce(Coliform, [Bulk Tank Coliforms]) AS Coliforms,
	CustomerFramework,
	CustomerCode,
	[dbo].[AntiCharConfirmation]([Delvo Inhibitor], [Bacteria Count], TestCode) AntiCharConfirmation
INTO #TempLOL2
FROM #TempLOL1
WHERE len(Barcode) >= 10

--Select * from #TempLOL2
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
	Barcode,
	max(CustomerFrameWork) CustomerFrameWork,
	max(CustomerCode) CustomerCode,
	max(AntiCharConfirmation) AntiCharConfirmation
INTO #TempLOL3
FROM #TempLOL2
GROUP BY Barcode

--select * from #TempLOL3
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT CASE 
		WHEN dbo.IsLoad(CustomerFrameWork) = '1'
			THEN [dbo].[PadRight10]([dbo].[ProducerOrLoadID](ProducerCode, Barcode, CustomerCode, CustomerFramework))
		ELSE [dbo].[PadRight6]([dbo].[ProducerOrLoadID](ProducerCode, Barcode, CustomerCode, CustomerFramework))
		END ProducerCode,
	CASE 
		WHEN dbo.IsLoad(CustomerFrameWork) = '1'
			THEN ''
		ELSE dbo.PassZeroIfNull(Tank)
		END Tank,
	CASE 
		WHEN [dbo].[IsLoad](CustomerFrameWork) = '1'
			THEN ''
		ELSE [dbo].[PadRight3]([dbo].[PassZeroIfNull](Seq))
		END Seq,
	CASE 
		WHEN dbo.IsLoad(CustomerFramework) != '1'
			THEN right(Barcode, 1)
		ELSE '2'
		END Fixed1,
	dbo.PadRight4(replace(cast([Milk Urea Nitrogen (MUN)] AS DECIMAL(4, 1)), '.', '')) [Milk Urea Nitrogen (MUN)],
	'0000' Fixed2,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(cast(nullif(TestDate, '') AS DATE) AS NVARCHAR(20)) TestDate,
	CASE 
		WHEN OfficialCode IS NULL
			OR OfficialCode != 99
			THEN '  ' -- Need 2 spaces if null
		ELSE OfficialCode
		END OfficialCode,
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint](Fat))), 4) Fat,
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint]([True Protein]))), 4) [True Protein],
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint](Lactose))), 4) Lactose,
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint]([Solids not fat (SNF)]))), 4) [Solids not fat (SNF)],
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint]([dbo].[TotalSolids]([Solids not fat (SNF)], Fat)))), 4) [Total Solids],
	[dbo].[LOLFieldLen]([dbo].[PadRight5]([dbo].[PassZeroIfNull]([Somatic cells])), 5) [Somatic Cells],
	[dbo].[LOLFieldLen]([dbo].[PadRight2]([dbo].[PassZeroIfNull]([dbo].[FormatTemp](SamplingTemperature))), 2) SamplingTemperature,
	[dbo].[LOLFieldLen]([dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveCharacters]([Bacteria Count]))), 4) [Bacteria Count],
	[dbo].[LOLFieldLen]([dbo].[PadRight3]([dbo].[PassZeroIfNull]([Freezing Point])), 3) [Freezing Point],
	'540' [Base FP],
	[dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveDecPoint]([dbo].[Water]([Freezing Point])))) Water,
	CASE 
		WHEN AntiCharConfirmation = 'NT'
			OR AntiCharConfirmation IS NULL
			OR AntiCharConfirmation NOT IN ('NF', 'AL')
			THEN '  '
		ELSE AntiCharConfirmation
		END AntiCharConfirmation,
	'0' Sediment,
	' ' Blank1,
	' ' Blank2,
	[dbo].[TestTypeIndicator](ParameterCode, [dbo].[RemoveCharacters]([Bacteria Count])) RecordIndicator,
	'DQCI' Fixed3,
	[dbo].[PadRight6]([dbo].[PassZeroIfNull]([dbo].[RemoveCharacters](Coliforms))) Coliforms,
	[dbo].[PadRight6]([dbo].[PassZeroIfNull]([dbo].[RemoveCharacters]([Thermoduric Plate Count]))) [Thermoduric Plate Count],
	[dbo].[PadRight4]([dbo].[PassZeroIfNull]([dbo].[RemoveCharacters]([Bacteria Count(PI)]))) [Bacteria Count(PI)],
	'0000' DMCFixed,
	' ' Blank3
INTO #TempLOL4
FROM #TempLOL3

--select * from #TempLOL4
--select * into dbo.LOLData from #TempLOL4 -- Need to use this insert statement if the table was droped.
INSERT INTO dbo.LOLData
SELECT *
FROM #TempLOL4

DROP TABLE #TempLOL1

DROP TABLE #TempLOL2

DROP TABLE #TempLOL3

DROP TABLE #TempLOL4

-- SELECT ? = count(1)
-- FROM dbo.LOLdata
-----------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT ON
	--SELECT *
	--FROM LOLData
	--WHERE len(ProducerCode) > 0
	--ORDER BY ProducerCode,
	--	Tank,
	--	Sampledate
GO

/****** Object:  StoredProcedure [dbo].[GetNDPEastData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetNDPEastData]
AS
--NDP East DSI File Format
SET NOCOUNT ON

TRUNCATE TABLE dbo.NDPEastData

SELECT *
INTO #TempNDPEast1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[NDPEastCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempNDPEast1
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
	[dbo].[CastAsDecNullValues]([Other Solids]) [Other Solids],
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
INTO #TempNDPEast2
FROM #TempNDPEast1
WHERE len(Barcode) >= 10

--select * from #TempNDPEast2
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
	max(AntiNumConfirmation) AntiNumConfirmation,
	max([other Solids]) [other Solids]
INTO #TempNDPEast3
FROM #TempNDPEast2
GROUP BY Barcode

--select * from #TempNDPEast3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT dbo.PadRight2(Company) Company,
	dbo.PadRight3(Division) Division,
	dbo.ProducerOrLoadId(ProducerCode, Barcode, CustomerCode, CustomerFramework) ProducerCode,
	'2' IsLoad, -- Pickup fixed value of 2.
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
	[dbo].[RemoveDecPoint]([other Solids]) Fixed2, -- NDP needs Other Solids.
	'' Fixed3,
	'1' Sediment, -- Always 1
	'1' Fixed4, -- Alwsy 1
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
	'' Seq, -- NDP does not use sequence numbers.
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
	'' Fixed14
INTO #TempNDPEast4
FROM #TempNDPEast3

--select * into dbo.NDPEastData from #TempNDPEast4 -- Use this if the table is deleted.
INSERT INTO dbo.NDPEastData
SELECT *
FROM #TempNDPEast4

DROP TABLE #TempNDPEast1

DROP TABLE #TempNDPEast2

DROP TABLE #TempNDPEast3

DROP TABLE #TempNDPEast4

-- select ? = count(1) from dbo.NDPEastData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF
	----Column ends at "AO".
	--SELECT *
	--FROM dbo.NDPEastData
	--WHERE len(ProducerCode) > 0
	--ORDER BY ProducerCode,
	--	Tank,
	--	SampleDate
GO

/****** Object:  StoredProcedure [dbo].[GetOfficialsData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetOfficialsData]
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
WHERE CAST(RecordReceived AS DATE) >= CAST(DATEADD(DAY, - 3, GETDATE()) AS DATE)
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

--UPDATE M2M
--SET M2M.OfficialDataSent = 1
--FROM M2MFileData M2M
--INNER JOIN OfficialsData OD ON (
--		M2M.Barcode = OD.Barcode
--		AND M2M.SamplingTime = OD.SampleDate
--		);
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
GO

/****** Object:  StoredProcedure [dbo].[GetPlainviewData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetPlainviewData]
AS
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
	--SELECT *
	--FROM dbo.PlainviewData
	--ORDER BY ProducerCode,
	--	Tank,
	--	SampleDate;
GO

/****** Object:  StoredProcedure [dbo].[GetRedingtonData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetRedingtonData]
AS
--Readington DSI File Format
SET NOCOUNT ON;

TRUNCATE TABLE dbo.ReadingtonData

SELECT *
INTO #TempReadington1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], Coliforms, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[ReadingtonCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempReadington1
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
INTO #TempReadington2
FROM #TempReadington1
WHERE len(Barcode) >= 10

--select * from #TempReadington2
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
INTO #TempReadington3
FROM #TempReadington2
GROUP BY Barcode

--select * from #TempReadington3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT dbo.PadRight2(Company) Company,
	dbo.PadRight3(Division) Division,
	dbo.ProducerOrLoadId(ProducerCode, Barcode, CustomerCode, CustomerFramework) ProducerCode,
	[dbo].IsLoad(CustomerFramework) IsLoad,
	'0' Fixed1,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) SampleDate,
	cast(Cast(TestDate AS DATE) AS NVARCHAR(20)) TestDate,
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
	cast(cast(ComponentsTestDate AS DATE) AS NVARCHAR(20)) ComponentsTestDate,
	'0' Fixed9,
	'' Fixed10,
	'' Fixed11,
	'' Fixed12,
	'' Fixed13,
	'' Fixed14,
	[dbo].[RemoveDecPoint](Casein) Casein
INTO #TempReadington4
FROM #TempReadington3

--select * into dbo.ReadingtonData from #TempReadington4 -- Use this if the table is deleted.
INSERT INTO dbo.ReadingtonData
SELECT *
FROM #TempReadington4

DROP TABLE #TempReadington1

DROP TABLE #TempReadington2

DROP TABLE #TempReadington3

DROP TABLE #TempReadington4

-- select ? = count(1) from dbo.ReadingtonData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF;
	--SELECT *
	--FROM dbo.ReadingtonData
	--WHERE len(ProducerCode) > 0
	--ORDER BY ProducerCode,
	--	Tank,
	--	SampleDate
GO

/****** Object:  StoredProcedure [dbo].[GetSaputoData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetSaputoData]
AS
-- Saputo DSI File Format
SET NOCOUNT ON

TRUNCATE TABLE dbo.SaputoData

SELECT *
INTO #TempSaputo1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [True Protein], [Total Protein], Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[SaputoCustomerCodes](CustomerCode) = '1'
	AND dbo.IsLoad(CustomerFramework) != '1' -- Prevent the loads from sending to the DSI format.
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempSaputo1
---------------------------------------------------------------------------------------------------------------------------------------------
SELECT Company,
	Division,
	ProducerCode,
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecNullValues](Fat) Fat,
	[dbo].[CastAsDecNullValues](coalesce([True Protein], [Total Protein])) [True Protein],
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
				len(barcode) = 10
				AND Barcode LIKE '750%'
				)
			THEN '0000'
		ELSE right(Barcode, 4)
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
INTO #TempSaputo2
FROM #TempSaputo1
WHERE len(Barcode) >= 10

--select * from #TempSaputo2
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
INTO #TempSaputo3
FROM #TempSaputo2
GROUP BY Barcode

--select * from #TempSaputo3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
	dbo.ProducerOrLoadId(ProducerCode, Barcode, CustomerCode, CustomerFramework) ProducerCode,
	'' IsLoad,
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
INTO #TempSaputo4
FROM #TempSaputo3

--select * into dbo.SaputoData from #TempSaputo4 -- Use this if the table is deleted.
INSERT INTO dbo.SaputoData
SELECT *
FROM #TempSaputo4

DROP TABLE #TempSaputo1

DROP TABLE #TempSaputo2

DROP TABLE #TempSaputo3

DROP TABLE #TempSaputo4

-- select ? = count(1) from SaputoData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT ON
	--SELECT [Company],
	--	[Division],
	--	[ProducerCode],
	--	[IsLoad],
	--	[Fixed1],
	--	[SampleDate],
	--	[TestDate],
	--	[OfficialCode],
	--	[Fat],
	--	[True Protein],
	--	[Lactose],
	--	[Solids not fat (SNF)],
	--	[TotalSolids],
	--	[Somatic Cells],
	--	[SamplingTemperature],
	--	[Bacteria Count],
	--	[Freezing Point],
	--	[Water],
	--	[Fixed2],
	--	[Fixed3],
	--	[Sediment],
	--	[Fixed4],
	--	[Tank],
	--	[Fixed5],
	--	[Fixed6],
	--	[Coliforms],
	--	[Thermoduric Plate Count],
	--	[Bacteria Count(PI)],
	--	[Fixed7],
	--	[Seq],
	--	[LoadId],
	--	[Milk Urea Nitrogen (MUN)],
	--	[AntiNumConfirmation],
	--	[Fixed8],
	--	[ComponentsTestDate],
	--	[Fixed9],
	--	[Fixed10]
	--FROM [eLimsMilkReportExports].[dbo].[SaputoData]
	--WHERE len(ProducerCode) > 0
	--	AND (
	--		len(Company) > 0
	--		AND len(Division) > 0
	--		)
	--ORDER BY ProducerCode,
	--	Tank,
	--	SampleDate
GO

/****** Object:  StoredProcedure [dbo].[GetSaputoLoadsData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetSaputoLoadsData]
AS
-- SaputoLoadsData file format.
SET NOCOUNT ON

TRUNCATE TABLE dbo.SaputoLoadsData

SELECT *
INTO #TempSaputoLoads1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Casein, [Solids not fat (SNF)], [Somatic cells], [True Protein], Sediment, Coliform, [Thermoduric Plate Count], [Bacteria Count(PI)])) piv
WHERE (
		[dbo].[SaputoCustomerCodes](CustomerCode) = '1'
		AND dbo.IsLoad(CustomerFramework) = '1'
		)
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #TempSaputoLoads1
-------------------------------------------------------------------------------------------------------------------------------------------------------
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
	[dbo].[FormatTemp](SamplingTemperature) SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	Tank,
	[dbo].[RemoveCharacters](Coliform) Coliforms,
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
	[dbo].[QualityTestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) QualityTestDate,
	[Other Solids],
	[dbo].[AntiCharConfirmation]([Delvo Inhibitor], [Bacteria Count], TestCode) INB
INTO #TempSaputoLoads2
FROM #TempSaputoLoads1
WHERE len(Barcode) >= 10

--Select * from #TempSaputoLoads2
------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(Fat) Fat,
	Max([True Protein]) [True Protein],
	max(Lactose) Lactose,
	max([Solids not fat (SNF)]) [Solids not fat (SNF)],
	Barcode,
	max(SampleDate) SampleDate,
	min(TestDate) TestDate,
	max([Milk Urea Nitrogen (MUN)]) [Milk Urea Nitrogen (MUN)],
	max([Other Solids]) [Other Solids],
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
	max(QualityTestDate) QualityTestDate,
	max(INB) INB
INTO #TempSaputoLoads3
FROM #TempSaputoLoads2
GROUP BY Barcode

--select * from #TempSaputoLoads3
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT [dbo].[ProducerOrLoadID](ProducerCode, Barcode, CustomerCode, CustomerFramwork) [PRODUCER],
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) [SAMPLE DATE],
	cast(cast(TestDate AS DATE) AS NVARCHAR(50)) [TEST DATE],
	'' [OFFICIAL],
	FAT,
	[True Protein] PROTEIN,
	Lactose [LAC],
	[Solids not fat (SNF)] [SNF],
	[Other Solids] [OS],
	[Somatic Cells] [ESCC],
	[Bacteria Count] PLC,
	[dbo].[FreezePointDec]([Freezing Point], '-') FP,
	CASE 
		WHEN [dbo].[Water]([Freezing Point]) IS NULL
			OR [dbo].[Water]([Freezing Point]) = ''
			THEN '<0.1%'
		ELSE [dbo].[Water]([Freezing Point])
		END [%WATER],
	INB
INTO #TempSaputoLoads4
FROM #TempSaputoLoads3

INSERT INTO dbo.SaputoLoadsData
SELECT *
FROM #TempSaputoLoads4

--select * into dbo.SaputoLoadsData from #TempSaputoLoads4
DROP TABLE #TempSaputoLoads1

DROP TABLE #TempSaputoLoads2

DROP TABLE #TempSaputoLoads3

DROP TABLE #TempSaputoLoads4

--select ? = count(1)  from dbo.SaputoLoadsData
--------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF
	--SELECT *
	--FROM dbo.SaputoLoadsData
	--ORDER BY PRODUCER,
	--	[SAMPLE DATE]
GO

/****** Object:  StoredProcedure [dbo].[GetValleyMilkData]    Script Date: 11/5/2018 7:48:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GetValleyMilkData]
AS
--Valley Milk Fresno producers.
SET NOCOUNT ON

TRUNCATE TABLE dbo.ValleyMilkData

SELECT *
INTO #ValleyMilk1
FROM (
	SELECT *
	FROM M2MFileData
	) src
pivot(max(ResultValue) FOR ParameterName IN ([Bacteria Count], [Delvo Inhibitor], [Fat], [Freezing Point], [Lactose], [Milk Urea Nitrogen (MUN)], [Other Solids], Sediment, [Solids not fat (SNF)], [Somatic cells], [Total Protein], [True Protein], Coliform, [Bulk Tank Coliforms], [Thermoduric Plate Count], [Bacteria Count(PI)], Casein)) piv
WHERE [dbo].[ValleyMilkCustomerCodes](CustomerCode) = '1'
	AND RowStatus = 0
	AND CommercialOrderLineStatus != 'Cancelled'

--select * from #ValleyMilk1
---------------------------------------------------------------------------------------------------------------------------------------------
SELECT Company,
	Division,
	ProducerCode,
	SamplingTime AS SampleDate,
	[dbo].[TestDate](ParameterCode, ValidationTime, MeasuredTime, TestCode) TestDate,
	[dbo].[CastAsDecNullValues](Fat) Fat,
	[dbo].[CastAsDecNullValues](coalesce([Total Protein], [True Protein])) [Total Protein],
	[dbo].[CastAsDecNullValues](Lactose) Lactose,
	[dbo].[CastAsDecNullValues]([Solids not fat (SNF)]) [Solids not fat (SNF)],
	[Somatic cells],
	SamplingTemperature,
	[Bacteria Count],
	[Freezing Point],
	Sediment,
	Tank,
	coalesce(Coliform, [Bulk Tank Coliforms]) AS Coliforms,
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
	[dbo].[AntiNumConfirmation]([Delvo Inhibitor]) AntiNumConfirmation,
	[dbo].[GeoMeanFlag](TestCode, [Delvo Inhibitor], [Bacteria Count]) FixedGeo
INTO #ValleyMilk2
FROM #ValleyMilk1
WHERE len(Barcode) >= 8

--select * from #ValleyMilk2
----------------------------------------------------------------------------------------------------------------------------------------------------
SELECT max(Fat) Fat,
	Max([Total Protein]) [Total Protein],
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
	max(AntiNumConfirmation) AntiNumConfirmation,
	max(FixedGeo) FixedGeo
INTO #ValleyMilk3
FROM #ValleyMilk2
GROUP BY Barcode

--select * from #ValleyMilk3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT BARCODE,
	cast(cast([dbo].[SampleDateOrTestDate](SampleDate, TestDate) AS DATE) AS NVARCHAR(20)) [DATE],
	FAT,
	[Total Protein] PROTEIN,
	[Solids not fat (SNF)] SNF,
	LACTOSE,
	dbo.Times1000([Somatic Cells]) SCC,
	CASE 
		WHEN cast([Thermoduric Plate Count] AS INT) = 0
			THEN '<10EST'
		ELSE dbo.Times10(cast([Thermoduric Plate Count] AS INT))
		END LPC,
	CASE 
		WHEN cast(Coliforms AS INT) = 0
			THEN '<10EST'
		WHEN cast(Coliforms AS INT) > 160
			THEN '>1500EST'
		ELSE dbo.Times10(CAST(Coliforms AS INT))
		END COLI,
	dbo.Times1000([Bacteria Count]) SPC,
	'' TATS,
	'' FSS
INTO #ValleyMilk4
FROM #ValleyMilk3

-- Use this if the table is deleted.
-- SELECT *
-- INTO dbo.ValleyMilkData
-- FROM #ValleyMilk4 
INSERT INTO dbo.ValleyMilkData
SELECT *
FROM #ValleyMilk4

DROP TABLE #ValleyMilk1

DROP TABLE #ValleyMilk2

DROP TABLE #ValleyMilk3

DROP TABLE #ValleyMilk4

-- select ? = count(1) from dbo.ValleyMilkData
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET NOCOUNT OFF
	--SELECT *
	--FROM dbo.ValleyMilkData
	--WHERE len(Barcode) > 0
	--ORDER BY Barcode,
	--	[Date]
GO


