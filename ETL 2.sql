DECLARE @DateID INT

SET @DateID  =	(SELECT 
					(CalendarYear - 2) * 10000 + 101
				FROM
					GNR.Calendar
				WHERE
					GregorianDate = CONVERT(DATE, GETDATE()))


EXEC ETL.spFactAccountingVoucher @DateID