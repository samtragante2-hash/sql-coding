/*
The first procedure updates Axiom’s employee staging tables by pulling active Streamline staff hired within the past seven years who match Paylocity records, 
formatting key fields (ZIP, phone, email), assigning defaults, and inserting them into `NEW_DataImport.dbo.Employee`, then appending only new IDs into `EmployeeTotal` to avoid duplicates. 
The second procedure rebuilds the `EmployeeLicense` table by first importing primary license data from Axiom’s source file and then adding additional licenses derived from each 
employee’s Streamline degree and most recent license record. Together, these procedures synchronize employee and license data from Streamline and Paylocity into Axiom’s import tables for consistent,
up-to-date HR integration.
*/

USE [AnalyticsCustomTables]
GO
/****** Object:  StoredProcedure [dbo].[Axiom_EmployeeLicense]    Script Date: 10/23/2025 9:27:18 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Sam Tragante
-- Create date: 01/30/2024
-- Description:	Procedure to populate Axiom's EmployeeLicense table in the NEW_DataImport database
-- =============================================
ALTER PROCEDURE [dbo].[Axiom_EmployeeLicense]
	-- Add the parameters for the stored procedure here
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;



	truncate table NEW_DataImport..EmployeeLicense


Insert Into NEW_DataImport..EmployeeLicense(
[OldSystemEmpd],
[LicenseId],
[LicenseNumber],
[StartDate],
[EndDate],
[ShowInSignature],
[Comments]
)

Select Distinct
e.OldSystemEmpId,
l.[Axiom LookupID] as LicenseID,
l.Number as LicenseNumber,
l.[Effective Date] as StartDate,
l.Expiration as EndDate,
Case
	When l.[Primary] like 'Y'
		Then 1
	Else 0 End as ShowInSignature,
Concat(l.[License/Certificate], ' ~ ', l.[Position Description]) as Comments

From NEW_DataImport..EmployeeTotal e
Join AnalyticsCustomTables..AxiomLicenseImport$ l
	on e.OldSystemEmpId = l.ID
Where e.OldSystemEmpId in (Select Distinct OldSystemEmpId from NEW_DataImport..Employee)
and l.[primary] like 'y'






--    -- Insert statements for procedure here
	Insert Into NEW_DataImport.dbo.EmployeeLicense(
	OldSystemEmpd,
	LicenseID,
	LicenseNumber,
	StartDate,
	EndDate,
	ShowInSignature,
	Comments
	)

select e.OldSystemEmpId as OldSystemEmpd,
CASE WHEN s.Degree = 25201 THEN 1 -- Licensed Associate Counselor
	 WHEN s.Degree in (49524,45177) THEN 5 -- LMSW. Licensed Master Social Worker
	 WHEN s.Degree = 25206 THEN 5 -- MSW. Licensed Master Social Worker
	 WHEN s.Degree = 45178 THEN 6 -- LPN - Licensed Practical Nurse
	 WHEN s.Degree = 25204 THEN 9 -- MD. Medical Doctor
	 WHEN s.Degree = 45163 THEN 10 -- APRN. Nurse Practitioner
	 WHEN s.Degree = 45165 THEN 10 -- APRN-I. Adult Nurse Practitioner-Board Certified
	 WHEN s.Degree = 50956 THEN 10 -- APRN UT-Family - Adult Nurse Practitioner-Board Certified
	 WHEN s.Degree = 25210 THEN 11 -- Physician Assistant
	 WHEN s.Degree = 50957 THEN 12 -- APRN UT-Psych/MH - Psychiatrist                                    
	 WHEN s.Degree = 46672 THEN 13 -- PsyD - Psychologist                                                 
	 WHEN s.Degree = 45190 THEN 14 -- RN. Registrered Nurse
	 WHEN s.Degree = 45180 THEN 18 -- MFT. Licensed Marriage and Family Therapist
	 WHEN s.Degree = 46430 THEN 19 -- AMFT. Licensed Associate Marriage and Family Therapist
	 WHEN s.Degree = 25781 THEN 23 -- D.O - Doctor of Osteopathic Medicine
	 WHEN s.Degree = 45167 THEN 24 -- ASUDC - Licensed Substance Abuse Technician
	 WHEN s.Degree = 45161 THEN 28 -- ACMHC - Associate Clinical Mental Health Counselor
	 WHEN s.Degree = 25889 THEN 33 -- BA/ASSOC - BA/ASSOC
	 WHEN s.Degree = 45169 THEN 34 -- BCBA - Board Certified Behavior Analyst
	 WHEN s.Degree = 45171 THEn 36 -- CM - Case Manager
	 WHEN s.Degree = 45172 THEN 38 -- CMA - Certified Medical Analyst
	 WHEN s.Degree = 46423 THEN 39 -- CMHC - Clinical Mental Health Counselor
	 WHEN s.Degree = 47353 THEN 44 -- LCPC - Licensed Professional Counselor
	 WHEN s.Degree = 25203 THEN 45 -- LPC - Licensed Professional Counselor
	 WHEN s.Degree = 25888 THEN 46 -- MASTERS - MASTERS
	 WHEN s.Degree = 47737 THEN 53 -- Pharmacist - Pharmacist
	 WHEN s.Degree = 46731 THEN 57 -- RBT - Registered Behavior Technician
	 WHEN s.Degree = 45193 THEN 60 -- SSW - Social Service Worker
	 WHEN s.Degree = 45195 THEN 61 -- SUDC - Substance Use Disorder Counseling
	 WHEN s.Degree = 25238 THEN 67 -- Certified Peer Support Specialist
	 WHEN s.Degree = 48065 THEN 68 -- CM-EXP-BACH - Case Manager (Experience or Bachelors Degree)
	 WHEN s.Degree = 45176 THEN 69 -- CMHC Intern - Clinical Mental Health Counselor Intern
	 WHEN s.Degree = 45185 THEN 70 -- Community Hlth Worker - Othr Professional
	 WHEN s.Degree = 45183 THEN 71 -- MSW Student - Master of Social Work - Student
	 WHEN s.Degree = 46442 THEN 72 -- Nurse Extender
	 WHEN s.Degree = 47318 THEN 73 -- Occupational Therapist OT
	 WHEN s.Degree = 47736 THEN 74 -- Occupational Therapist-Student
	 WHEN s.Degree = 47738 THEN 75 -- Peer Support-student
	 WHEN s.Degree = 47767 THEN 76 -- Pharmacy Tech
	 WHEN s.Degree = 45187 THEN 77 -- Psychology Student
	 WHEN s.Degree = 47727 THEN 78 -- Recreational Therapist-Student
	 WHEN s.Degree = 45191 THEN 79 -- Social Work Student
	 WHEN s.Degree = 45192 THEN 80 -- Speech Language Pathologist SLP
	 WHEN s.Degree = 45194 THEN 81 -- SUDC Student/Intern - Substance Use Disorder Counseling Student/Intern
	 WHEN s.Degree = 45196 THEN 82 -- Support Staff
	  ELSE 82 END as LicenseID,										-- Doubled checked on 20250227 by ST
sld.LicenseNumber as LicenseNumber,
isnull(s.EmploymentStart, '01-01-2024') as StartDate,
s.EmploymentEnd as EndDate,
ShowInSignature = 1,
LEFT(CONVERT(VARCHAR, s.Comment), 499) as Comments
from NEW_DataImport.dbo.EmployeeTotal as e
	left join StreamlineQuery.dbo.staff as s
		on e.OldSystemEmpId = s.StaffId
	OUTER APPLY (
			select top 1 LicenseNumber, StartDate, EndDate
				from StreamlineQuery.dbo.StaffLicenseDegrees as sld
					where sld.StaffId = e.OldSystemEmpId
					and ISNULL(sld.RecordDeleted, 'N') = 'N'
					order by sld.ModifiedDate desc
					) as sld

END

