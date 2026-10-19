.TH "CS324 ASSIGNMENT 2" 1 "October 2026" "Task Breakdown" "A1 CONTENT / A2 REQUIREMENTS / ALLOCATION"
.SH "CS324 ASSIGNMENT 2 - TASK BREAKDOWN"
.PP
\fBGroup members:\fP Boe (Mr Qwero), Ray (Lyndray Duna / Lynsii114), Jah
(s11192806-hub).
.PP
This document lists what the Assignment 1 solution already provides, breaks the
Assignment 2 requirements down into smaller work items, and randomly allocates those
items between the three members.
.SH "1. WHAT ASSIGNMENT 1 PROVIDES"
.IP \(bu 3
A distributed cluster of separate Java worker processes forming an unstructured
worker network, with a coordinator elected among them.
.IP \(bu 3
A Bootstrap Node that registers active workers, tracks membership, and assigns
startup-lottery priorities.
.IP \(bu 3
Support for the job types MAX(numbers), PRIMESUM(start, end) and PRIMECOUNT(numbers),
split into segments and distributed across workers.
.IP \(bu 3
Concurrent execution using Java threads, with JAC-based workload balancing across
workers and a five-job coordinator term.
.IP \(bu 3
Clients that submit jobs and track results, including a desktop GUI.
.IP \(bu 3
A code base already migrated to gRPC and Protocol Buffers, with server-streaming
progress, worker-failure tolerance, coordinator-failover resume and dynamic joining
implemented and verified.
.SH "2. ASSIGNMENT 2 REQUIREMENTS (BROKEN DOWN)"
.ps 8
.vs 10
.TS
center box tab(&);
l l.
#&Work item
_
1&Replace all Java-RMI communication with gRPC.
2&Design the Protocol Buffer schema: services, messages, RPCs, streaming.
3&Client GUI: submit jobs and show the overall completion percentage.
4&Worker-level breakdown when the user clicks the progress bar.
5&Progress based on actual completed segments, streamed live over gRPC.
6&Worker failure detection via heartbeats and timeouts.
7&Worker failure recovery: reassign unfinished work so jobs continue.
8&Allow new workers to join while jobs are being assigned.
9&Detect a failed coordinator and establish a replacement.
10&Recover coordinator state: task ids, assignments, results.
11&Separate the client, worker and Bootstrap node project files.
12&New repository (initial commit = submitted A1 code) and README for configure / start / run.
.TE
.ps 10
.vs 12
.SH "3. RANDOM ALLOCATION BETWEEN MEMBERS"
.PP
\fBBoe\fP  - work items #1, #6, #9, #12: gRPC migration of all RMI endpoints; worker
failure detection (heartbeats / timeouts); coordinator failure detection; repository
setup with the A1 initial commit plus the README.
.PP
\fBRay\fP  - work items #3, #4, #7, #11: client GUI overall progress bar; worker-level
breakdown on progress-bar click; worker failure recovery and task reassignment;
separating the client, worker and Bootstrap node project files.
.PP
\fBJah\fP  - work items #2, #5, #8, #10: Protocol Buffer schema design (services,
messages, RPCs, streaming); segment-based progress aggregation streamed over gRPC;
dynamic joining of new workers; coordinator state recovery (identifiers, assignments,
results).
.TS
center box tab(&);
l l.
Member&Work items
_
Boe&1, 6, 9, 12
Ray&3, 4, 7, 11
Jah&2, 5, 8, 10
.TE
.PP
Four items each, so the workload is evenly balanced across the group.