/*
This file accompanies the manuscript titled: 
	Graphical Analysis of Guideline Adherence Finds Systemwide Anomalies in HIV Diagnostic Testing

Specifically it follows the first example from the manuscript's supplement.

The codes creates these tables listed below then it performs analysis on the tables.

The nodes in this example represent tests performed.

Tables: 
	select * from #Nodes
	select * from #NodesClone
	select * from #NodesPartition
	select * from #NodesCloneWithFacility
	select * from #Edges

Ronald "George" Hauser
Accompanied by Ankur Bhargava
2019-05-09
*/

--Data input
IF OBJECT_ID('tempdb..#Nodes', 'U') IS NOT NULL DROP TABLE #Nodes
CREATE TABLE #Nodes
( 
	NodeId int identity(1,1),
	PatientId varchar(100),
	NodeType varchar(100), 
	NodeDt datetime, 
	FacilityId int, --optional
	--ClinicianId int --optional, similar to FacilityId
); 

IF OBJECT_ID('tempdb..#Adjudications', 'U') IS NOT NULL DROP TABLE #Adjudications
CREATE TABLE #Adjudications
(
	NodeType1 varchar(100), 
	NodeType2 varchar(100), 
	AllowedQ bit
); 

insert into #Nodes values --<-- This is table S1 in the manuscript supplement.
(1,1,'2000-01-01', 1),
(1,2,'2000-10-01', 1),
(2,1,'2000-05-01', 1),
(2,2,'2000-06-01', 1),
(3,2,'2000-03-01', 2)

insert into #Adjudications values
('Start','1',1),
('1','2',1),
('2','End',1),
('Start','2',0)

-------------------- Step 1 - Add Start/End nodes
IF OBJECT_ID('tempdb..#StartEnd', 'U') IS NOT NULL DROP TABLE #StartEnd
create table #StartEnd(	StartEnd varchar(100) )
insert into #StartEnd values ('Start'),('End')

IF OBJECT_ID('tempdb..#NodesClone', 'U') IS NOT NULL DROP TABLE #NodesClone
select NodeId, PatientId, NodeType, NodeDt
into #NodesClone
from #Nodes

insert into #NodesClone
select t.PatientId, #StartEnd.StartEnd NodeId, case when #StartEnd.StartEnd = 'Start' then cast('1753-1-1' as datetime) else '9999-12-31 23:59:59.997' end NodeDt
from (
	select distinct PatientId
	from #NodesClone
) t
cross apply #StartEnd
-------------------- Step 2 - Convert nodes to edges
IF OBJECT_ID('tempdb..#NodesPartition', 'U') IS NOT NULL DROP TABLE #NodesPartition
select *, row_number() over(partition by PatientId order by NodeDt) PatientChronoId
into #NodesPartition
from #NodesClone

IF OBJECT_ID('tempdb..#Edges', 'U') IS NOT NULL DROP TABLE #Edges
select np1.NodeId NodeId1, np2.NodeId NodeId2
into #Edges
from #NodesPartition np1
join #NodesPartition np2 on np1.PatientId = np2.PatientId and np1.PatientChronoId+1 = np2.PatientChronoId

/* Check to ensure no two patient NodeDts are the same */
declare @d1 int = (select count(1) [Count] from (select distinct PatientId, NodeDt from #NodesClone) t)
declare @d2 int = (select count(1) [Count] from #NodesClone)
if (@d1 != @d2) throw 50000, 'Nodes for a patient occur at the same time.  The chronological order is ambiguous.', 0

-------------------- Step 3 - Add Facility to #NodesClone table
IF OBJECT_ID('tempdb..#NodesCloneWithFacility', 'U') IS NOT NULL DROP TABLE #NodesCloneWithFacility
select nc.*, 
	case 
		when nc.NodeType = 'Start' then n2.FacilityId 
		when nc.NodeType = 'End' then n3.FacilityId 
		else n.FacilityId end FacilityId
into #NodesCloneWithFacility
from #NodesClone nc 
left join #Nodes n on n.NodeId = nc.NodeId
/*Start node facility*/
left join #NodesPartition np on np.PatientChronoId = 2 and np.PatientId = nc.PatientId
left join #Nodes n2 on n2.NodeId = np.NodeId
/*End node facility*/
left join (
	select t.PatientId, NodeId
	from (
		select PatientId, max(PatientChronoId)-1 PatientChronoIdMaxMinus1
		from #NodesPartition 
		group by PatientId
	) t
	join #NodesPartition np on np.PatientChronoId = t.PatientChronoIdMaxMinus1 and np.PatientId = t.PatientId
) np2 on np2.PatientId = nc.PatientId and nc.NodeType = 'End'
left join #Nodes n3 on n3.NodeId = np2.NodeId

-------------------- Analysis
--Example 1 - Edge level 
--This is figure S3(A) in the manuscipt supplement.
select n1.NodeType NodeType1, n2.NodeType NodeType2, AllowedQ, count(1) [Count]
from #Edges e
join #NodesClone n1 on n1.NodeId = e.NodeId1 
join #NodesClone n2 on n2.NodeId = e.NodeId2
join #Adjudications a on a.NodeType1 = n1.NodeType and a.Nodetype2 = n2.NodeType
group by n1.NodeType, n2.NodeType, AllowedQ

--Example 2 - Edge level + NodeType1 statistics
--This is table S2 in the manuscript supplement
select t.*, t2.[Count] NodeType1Total, round(100*cast(t.[Count] as float)/t2.[Count], 0) EdgeType1TotalPct
from (
	select n1.NodeType NodeType1, n2.NodeType NodeType2, AllowedQ, count(1) [Count]
	from #Edges e
	join #NodesClone n1 on n1.NodeId = e.NodeId1 
	join #NodesClone n2 on n2.NodeId = e.NodeId2
	join #Adjudications a on a.NodeType1 = n1.NodeType and a.Nodetype2 = n2.NodeType
	group by n1.NodeType, n2.NodeType, AllowedQ
) t
left join (
	select n1.NodeType NodeType1, count(1) [Count]
	from #Edges e
	join #NodesClone n1 on n1.NodeId = e.NodeId1 
	join #NodesClone n2 on n2.NodeId = e.NodeId2
	join #Adjudications a on a.NodeType1 = n1.NodeType and a.Nodetype2 = n2.NodeType
	group by n1.NodeType
) t2 on t.NodeType1 = t2.NodeType1


--Example 3 - (Example 1) @ Facility level
select n1.NodeType NodeType1, n2.NodeType NodeType2, AllowedQ, n1.FacilityId NodeType1Facility, count(1) [Count]
from #Edges e
join #NodesCloneWithFacility n1 on n1.NodeId = e.NodeId1 
join #NodesCloneWithFacility n2 on n2.NodeId = e.NodeId2
join #Adjudications a on a.NodeType1 = n1.NodeType and a.Nodetype2 = n2.NodeType
group by n1.NodeType, n2.NodeType, AllowedQ, n1.FacilityId

--Example 4 - (Example 2) @ Facility level
--This is table S3 in the manuscript supplement
select t.*, t2.[Count] NodeType1Total, round(100*cast(t.[Count] as float)/t2.[Count], 0) EdgeType1TotalPct
from (
	select n1.NodeType NodeType1, n2.NodeType NodeType2, AllowedQ, n1.FacilityId NodeType1Facility, count(1) [Count]
	from #Edges e
	join #NodesCloneWithFacility n1 on n1.NodeId = e.NodeId1 
	join #NodesCloneWithFacility n2 on n2.NodeId = e.NodeId2
	join #Adjudications a on a.NodeType1 = n1.NodeType and a.Nodetype2 = n2.NodeType
	group by n1.NodeType, n2.NodeType, AllowedQ, n1.FacilityId
) t
left join (
	select n1.NodeType NodeType1, n1.FacilityId NodeType1Facility, count(1) [Count]
	from #Edges e
	join #NodesCloneWithFacility n1 on n1.NodeId = e.NodeId1 
	join #NodesCloneWithFacility n2 on n2.NodeId = e.NodeId2
	join #Adjudications a on a.NodeType1 = n1.NodeType and a.Nodetype2 = n2.NodeType
	group by n1.NodeType, n1.FacilityId
) t2 on t.NodeType1 = t2.NodeType1 and t.NodeType1Facility=t2.NodeType1Facility
