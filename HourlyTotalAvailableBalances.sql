DROP TABLE IF EXISTS #TempDateHours,#ReadyToUpdateData_Transactions,#ReadyToUpdateData_MerchantTransactions
CREATE TABLE #TempDateHours (HourlyDateTime DATETIME);
DECLARE  @inc		AS INT  = 1,
		 @m			AS INT  = 1,
		 @BaseDay   AS DATETIME		= CAST(GETDATE() as DATE)--'2019-09-14' --Enter this while inputing data!

		IF DAY(@BaseDay) = 1
		   BEGIN
		   SET @m = @m + 1
		   END
/*1- First you need to create a dummy hourly table to input all hour for given date interval that basing User by User*/
DECLARE	 @StartDateParameter AS DATETIME = DATEADD(DAY,-1,cast(@BaseDay as DATETIME))--dateadd(day, 1, eomonth(@BaseDay, -@m))
DECLARE  @StartDate			 AS DATETIME = DATEADD(DAY,-1,cast(@StartDateParameter as DATETIME))
		WHILE @StartDate <= @BaseDay
			BEGIN
				IF(@inc <= 23)
					BEGIN
					INSERT INTO #TempDateHours
						SELECT
							DATEADD(hour,@inc,@StartDate) HourlyDateTime
						SET @inc = @inc + 1
					END
				ELSE
					BEGIN
						SET @inc = 0
						SET @StartDate = DATEADD(DAY,1,@StartDate)
					END
			END

			SET @StartDate = @StartDateParameter
			;
DELETE FROM #TempDateHours WHERE @BaseDay < HourlyDateTime
DELETE FROM #TempDateHours WHERE @StartDate > HourlyDateTime;

WITH AvailableBalanceCTE_Transactions AS
	(
	SELECT
		UserKey
	   ,Currency
	   ,AvailableBalance
	   , IIF(DATEPART(HOUR,TransactionDateTime) = 23
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,TransactionDateTime))/*/12*/)/**12*/, dateadd(day, 0, datediff(day, -1, TransactionDateTime))) 
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,TransactionDateTime))/*/12*/)/**12*/, dateadd(day, 0, datediff(day,  0, TransactionDateTime))
						)) ContributedDateHour
	   ,ROW_NUMBER() OVER (PARTITION BY UserKey,Currency,IIF(DATEPART(HOUR,TransactionDateTime) = 23
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,TransactionDateTime))/*/12*/)/**12*/, dateadd(day, 0, datediff(day, -1, TransactionDateTime))) 
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,TransactionDateTime))/*/12*/)/**12*/, dateadd(day, 0, datediff(day,  0, TransactionDateTime))
						   )
						   ) ORDER BY TransactionDateTime DESC) Ranker
	FROM FACT_Transactions With (Nolock)
	WHERE TransactionDateTime > @StartDate and TransactionDateTime <= @BaseDay
		SELECT
		User_Key [UserKey]
	   ,Currency
	   ,EodBalance [AvailableBalance]
	   ,@StartDate ContributedDateHour
	   ,CAST(1 as bigint) Ranker
	FROM FACT_LastUserBalances UB With (Nolock)
	WHERE DATEADD(DAY,1,EndOfTheDay) <= @StartDate
	),
	CrossJoinedDummyData_Transactions AS
	(
			SELECT 
				DH.HourlyDateTime [DateHour], UserKey,Currency
			FROM (SELECT DISTINCT UserKey,Currency FROM AvailableBalanceCTE_Transactions WHERE ContributedDateHour >= @StartDate AND ContributedDateHour < @BaseDay AND Ranker = 1) x
			CROSS JOIN #TempDateHours DH WITH (NOLOCK) 
			WHERE DH.HourlyDateTime >= @StartDate AND DH.HourlyDateTime <= @BaseDay
	), CombiningDummyDataWithFundamental_Transactions AS
	(
			SELECT
				 CAR.DateHour
				,CAR.UserKey
				,CAR.Currency
				,SK.AvailableBalance
			FROM CrossJoinedDummyData_Transactions CAR
	LEFT JOIN  AvailableBalanceCTE_Transactions SK ON SK.ContributedDateHour = CAR.DateHour AND  SK.UserKey = CAR.UserKey AND SK.Currency = CAR.Currency AND Ranker = 1
	)
	select * 
	INTO #ReadyToUpdateData_Transactions
	from CombiningDummyDataWithFundamental_Transactions order by [DateHour]
	
		--UPDATE  #ReadyToUpdateData_Transactions
		--	SET AvailableBalance = 0
		--FROM  #ReadyToUpdateData_Transactions
		--WHERE AvailableBalance is null AND DateHour = dateadd(hour,1,@StartDate)

		WHILE (SELECT COUNT(DateHour) FROM  #ReadyToUpdateData_Transactions WHERE AvailableBalance IS NULL AND DateHour >= @StartDate AND DateHour <= @BaseDay) != 0
		BEGIN
		UPDATE R1
		set R1.AvailableBalance = R2.AvailableBalance    
		from  #ReadyToUpdateData_Transactions R1
		join  #ReadyToUpdateData_Transactions R2 on dateadd(HOUR,1,R2.DateHour) = R1.DateHour AND R1.UserKey = R2.UserKey AND R1.Currency = R2.Currency
		where  R1.AvailableBalance is null AND R2.AvailableBalance IS NOT NULL and R1.DateHour >= @StartDate AND R1.DateHour <= @BaseDay and R2.DateHour >= @StartDate AND R2.DateHour <= @BaseDay
		end
-------------------------------               
;WITH AvailableBalanceCTE_MerchantTransactions AS
	(
	SELECT
		Merchant_Key
	   ,Currency
	   ,AvailableBalance
	   , IIF(DATEPART(HOUR,TransactionDateTime) = 23
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,TransactionDateTime))/*/12*/)/**12*/, dateadd(day, 0, datediff(day, -1, TransactionDateTime))) 
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,TransactionDateTime))/*/12*/)/**12*/, dateadd(day, 0, datediff(day,  0, TransactionDateTime))
						)) ContributedDateHour
	   ,ROW_NUMBER() OVER (PARTITION BY Merchant_Key,Currency,IIF(DATEPART(HOUR,TransactionDateTime) = 23
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,TransactionDateTime))/*/12*/)/**12*/, dateadd(day, 0, datediff(day, -1, TransactionDateTime))) 
						   ,dateadd(hour, (datepart(hour, DATEADD(HH,1,TransactionDateTime))/*/12*/)/**12*/, dateadd(day, 0, datediff(day,  0, TransactionDateTime))
						   )
						   ) ORDER BY TransactionDateTime DESC) Ranker
	FROM FACT_MerchantTransactions With (Nolock)
	WHERE TransactionDateTime > @StartDate and TransactionDateTime <= @BaseDay
		UNION all
		SELECT
		MerchantKey Merchant_Key
	   ,Currency
	   ,EodBalance [AvailableBalance]
	   ,@StartDate ContributedDateHour
	   ,CAST(1 as bigint) Ranker
	FROM FACT_LastMerchantBalances UB With (Nolock)
	WHERE DATEADD(DAY,1,EndOfTheDay) <= @StartDate
	),
	CrossJoinedDummyData_MerchantTransactions AS
	(
			SELECT 
				DH.HourlyDateTime [DateHour], Merchant_Key,Currency
			FROM (SELECT DISTINCT Merchant_Key,Currency FROM AvailableBalanceCTE_MerchantTransactions WHERE ContributedDateHour >= @StartDate AND ContributedDateHour < @BaseDay AND Ranker = 1) x
			CROSS JOIN #TempDateHours DH WITH (NOLOCK) 
			WHERE DH.HourlyDateTime > @StartDate AND DH.HourlyDateTime <= @BaseDay
	), CombiningDummyDataWithFundamental_MerchantTransactions AS
	(
			SELECT
				 CAR.DateHour
				,CAR.Merchant_Key
				,CAR.Currency
				,SK.AvailableBalance
			FROM CrossJoinedDummyData_MerchantTransactions CAR
	LEFT JOIN  AvailableBalanceCTE_MerchantTransactions SK ON SK.ContributedDateHour = CAR.DateHour AND  SK.Merchant_Key = CAR.Merchant_Key AND SK.Currency = CAR.Currency AND Ranker = 1
	)

	select * 
	INTO #ReadyToUpdateData_MerchantTransactions
	from CombiningDummyDataWithFundamental_MerchantTransactions order by [DateHour]
	
		--UPDATE  #ReadyToUpdateData_MerchantTransactions
		--SET AvailableBalance = 0
		--FROM  #ReadyToUpdateData_MerchantTransactions
		--WHERE AvailableBalance is null AND DateHour = dateadd(hour,1,@StartDate)

		WHILE (SELECT COUNT(DateHour) FROM  #ReadyToUpdateData_MerchantTransactions WHERE AvailableBalance IS NULL AND DateHour > @StartDate AND DateHour <= @BaseDay) != 0
		BEGIN
		UPDATE R1
		set R1.AvailableBalance = R2.AvailableBalance    
		from  #ReadyToUpdateData_MerchantTransactions R1
		join  #ReadyToUpdateData_MerchantTransactions R2 on dateadd(HOUR,1,R2.DateHour) = R1.DateHour AND R1.Merchant_Key = R2.Merchant_Key AND R1.Currency = R2.Currency
		where  R1.AvailableBalance is null AND R2.AvailableBalance IS NOT NULL and R1.DateHour >= @StartDate AND R1.DateHour <= @BaseDay and R2.DateHour >= @StartDate AND R2.DateHour <= @BaseDay
		end
		
/*Company Level Balance Indicators*/
		;WITH HourlyCompanyBalanceIndicators_Transactions AS
		(
		SELECT DateHour,Currency,SUM(AvailableBalance) TotalAvailableBalanceDailyActiveUser FROM #ReadyToUpdateData_Transactions where DateHour > @StartDate group by DateHour,Currency
		), HourlyCompanyBalanceIndicators_MerchantTransactions AS
		(

		SELECT DateHour,Currency,SUM(AvailableBalance) TotalAvailableBalanceDailyActiveMerchant FROM #ReadyToUpdateData_MerchantTransactions where DateHour > @StartDate group by DateHour,Currency
		)
		SELECT L.DateHour, l.Currency,TotalAvailableBalanceDailyActiveUser,TotalAvailableBalanceDailyActiveMerchant, ISNULL(TotalAvailableBalanceDailyActiveUser,0) + ISNULL(TotalAvailableBalanceDailyActiveMerchant,0) TotalAvailableBalanceDailyActive
		FROM #TempDateHours TDH With (NOLOCK)
		LEFT JOIN HourlyCompanyBalanceIndicators_Transactions			 L ON L.DateHour = TDH.HourlyDateTime
		LEFT JOIN HourlyCompanyBalanceIndicators_MerchantTransactions ML ON L.Currency = ML.Currency AND L.DateHour = ML.DateHour
		ORDER BY l.Currency, l.DateHour
		drop table #TempDateHours
