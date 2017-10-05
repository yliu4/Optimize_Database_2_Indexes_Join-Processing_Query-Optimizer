# Optimize Database System --- Develop Indexes, Join Processing Methods and Query Optimizer Strategies
System

## About SimpleDB System
SimpleDB database system - a multi-user transactional database server written in Java, which interacts with Java client programs via JDBC. SimpleDB is
a stripped-down open-source system developed by E. Sciore for pedagogical use. We have
chosen this as foundation for the CS4432 projects to give you a chance to gain experience with
database systems internals – while attempting to avoid the inherent complexity (aka steep
learning curve) that would come with any industrial-strength system. Instead, the code for
SimpleDB is clean and compact. Thus, the learning curve is small. Everything about SimpleDB
is intentionally bare bone. It implements only a small fraction of SQL and JDBC, and does little
or no error checking.

## Finished Tasks
### Index Design and Development
In the old system, IndexInfo.open() always creates a static hash index. We optimize it, so that IndexInfo.open() would instead create the correct index type based on the type information that had been indicated in the earlier create index statement. In this task, we implemented extensible hash index and B-Tree index.  
### Develop SmartMergeJoin in SimpleDB
The old SimpleDB MergeJoin will always re-sort all records of the two participating tables regardless of the records are already sorted or not. This is not very efficient. In this task, we improved that. As first step, we kept track of whether a table has been sorted or
not. At the beginning, none of the tables are sorted. The old SortScan of the sortmergejoin will sort the table and write the records back out to a temporary table for later use (materialization).  
We extent the SortScan class, so that our modified SortScan sorts the base table as well and then sets the Sorted flag to true, thus indicating the table has been sorted.  
we also extent the MergeJoin class, so it uses our improved SortScan. Thus our improved SmartMergeJoin Operator would sort the tables first only if the tables are not sorted already. So that if it gets lucky, it can completely skip the first phase of the SortJoin method,the sorting phase.  
### Testing
