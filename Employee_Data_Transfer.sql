/*
This stored procedure refreshes an employee staging table for an Axiom data import. It pulls active, non-deleted Streamline staff hired within the last 7 years and matched to Paylocity by email, 
formats fields (e.g., trims ZIP to 5, strips punctuation from phone, takes middle initial), sets some fields to defaults/nulls (e.g., FullTime=0, HoursPerWeek=NULL), 
and inserts them into `NEW_DataImport.dbo.Employee` with the Streamline `StaffId` as `OldSystemEmpID`. It then removes any rows whose IDs already exist in `EmployeeTotal`
and appends only new IDs from `Employee` into `EmployeeTotal` (a deduping “append new” step). Note: the filter `pa.employmentStatusType <> 'T'` appears to reference the wrong alias 
and likely should be `p.employmentStatusType`.
*/

USE [AnalyticsCustomTables]
GO
/****** Object:  StoredProcedure [dbo].[Axiom_Employee]    Script Date: 10/23/2025 9:13:22 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/* =============================================
-- Author: Sam Tragante
-- Create date: 01/30/2024
-- Description:	Procedure to populate Axiom's PatientContact table in the NEW_DataImport database
-- =============================================*/
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

     --Insert statements for procedure here
	Insert Into NEW_DataImport.dbo.Employee(
		OldSystemEmpID,
		LastName,
		FirstName,
		Initial,
		Address,
		City,
		State,
		Zipcode,
		Phone,
		DOB,
		SSN,
		HoursPerWeek,
		FullTime,
		EmployeeStatusId,
		Gender,
		JobClassification,
		EmployeeType,
		Email
		)

SELECT distinct 

--s.staffid 
	s.staffid  as OldSystemEmpID,
		s.lastname as LastName,
		s.firstname as FirstName,
		LEFT(s.middlename,1) as Initial,
		s.Address as Address,			--changed by BJM to paylocity table 2/1/2024
		s.City as City,				--changed by BJM to paylocity table 2/1/2024
		s.State as State,				--changed by BJM to paylocity table 2/1/2024
		LEFT(s.Zip,5) as ZipCode,		--changed by BJM to paylocity table 2/1/2024
		LEFT(REPLACE(REPLACE(REPLACE(s.PhoneNumber, ')', ''),'(',''),'-',''),10) as Phone,
		CONVERT(date, s.DOB) as DOB,
		s.SSN as SSN,
		HoursPerWeek = NULL,																
		FullTime= 0,
		EmployeeStatusID= 4,
		Gender = Null,
		JobClassification = Null,
		EmployeeType = Null,
		LEFT(s.email,50) as Email
	FROM StreamlineQuery.dbo.Staff s
	Join AnalyticsCustomTables..PaylocityAPI p
		on p.workEmail = s.Email
	WHERE ISNULL(s.RecordDeleted, 'N') = 'N'
	--and pa.employmentStatusType <> 'T'
	and s.StaffId is not null 
	and s.Active = 'Y'
	and s.EmploymentStart >= DATEADD(year,-7,getdate())
	order by s.LastName

Delete NEW_DataImport.dbo.Employee
WHERE OldSystemEmpID IN (SELECT OldSystemEmpID FROM NEW_DataImport.dbo.EmployeeTotal)

INSERT INTO NEW_DataImport.dbo.EmployeeTotal
SELECT * FROM NEW_DataImport.dbo.Employee
WHERE OldSystemEmpID not IN (SELECT OldSystemEmpID FROM NEW_DataImport.dbo.EmployeeTotal)

END



