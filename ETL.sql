USE YourCompany
GO

DROP PROC IF EXISTS ETL.spFactAccountingVoucher
GO

CREATE PROC ETL.spFactAccountingVoucher
(
	@DateID INT
)
AS
BEGIN

	DELETE YourCompany_DW.BI.FactAccountingVoucher
	WHERE DateID >= @DateID

	MERGE INTO 
		YourCompany_DW.BI.DimAccount AS TARGET
	USING 
		BI.vwDimAccount AS SOURCE
	ON
		TARGET.AccountID = SOURCE.AccountID
	WHEN MATCHED
	THEN UPDATE 
	SET 
		TARGET.AccountCode = SOURCE.AccountCode,
		TARGET.PrimaryCode = SOURCE.PrimaryCode,
		TARGET.GeneralCode = SOURCE.GeneralCode,
		TARGET.SubsIDiaryCode = SOURCE.SubsIDiaryCode,
		TARGET.PrimaryTitle = SOURCE.PrimaryTitle,
		TARGET.GeneralTitle = SOURCE.GeneralTitle,
		TARGET.Title = SOURCE.Title
	WHEN NOT MATCHED BY TARGET
	THEN INSERT (AccountID, AccountCode, PrimaryCode, GeneralCode, SubsIDiaryCode, PrimaryTitle, GeneralTitle, Title)
	VALUES (AccountID, AccountCode, PrimaryCode, GeneralCode, SubsIDiaryCode, PrimaryTitle, GeneralTitle, Title)

	WHEN NOT MATCHED BY SOURCE
	THEN DELETE;


	MERGE INTO 
		YourCompany_DW.BI.DimAccountingVoucherState AS TARGET
	USING
		BI.vwDimAccountingVoucherState AS SOURCE
	ON
		TARGET.AccountingVoucherStateID = SOURCE.AccountingVoucherStateID
	WHEN MATCHED
	THEN UPDATE
	SET 
		TARGET.Title = SOURCE.Title

	WHEN NOT MATCHED BY TARGET
	THEN INSERT (AccountingVoucherStateID, Title)
	VALUES (AccountingVoucherStateID, Title)

	WHEN NOT MATCHED BY SOURCE 
	THEN DELETE;


	--DELETE YourCompany_DW.BI.DimAccountingVoucherType
	--DELETE YourCompany_DW.BI.DimCurrency
	--DELETE YourCompany_DW.BI.DimDetailedAccount
	--DELETE YourCompany_DW.BI.DimSystem


	INSERT INTO YourCompany_DW.BI.FactAccountingVoucher
	SELECT 
		*
	FROM
		BI.vwFactAccountingVoucher
	WHERE 
		DateID >= @DateID
END