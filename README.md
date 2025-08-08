# sql2dsq

Library for converting `sql` queiries to [dsql](https://github.com/HealthSamurai/dsql) structures.

## Overview

This library provides a simple way to parse PostgreSQL queries in Clojure applications. It uses the [libpg_query](https://github.com/pganalyze/libpg_query) library under the hood to parse queries into a JSON representation of the PostgreSQL parse tree. To use this library in Java, [JNA](https://github.com/java-native-access/jna) is utilized. You can find the JNA wrapper code in the `java` folder.

## Installation

At the moment, the resources include a prebuilt `libpq_query` library for macOS to facilitate a quick start for developers.

Go to `java` folder and run:
```
mvn clean package
```

After that you can start REPL :)

## Development

All code is located in `src/sql2dsql/core`. The `parse-sql` function returns an AST in EDN format. The multimethod `stmt->dsql` takes this AST and transforms it into dsql structure.
You can find a list of all AST node types [here](https://github.com/pganalyze/libpg_query/blob/17-latest/src/postgres/include/nodes/parsenodes.h). For each node, there is a corresponding stmt->dsql method implementation; however, not all AST nodes are currently supported.

You can play with core functions in `user` namespace:

```clojure
(ns user
  (:require [sql2dsql.transpiler :as transpiler])
  (:import
   (sql2dsql.pgquery PgQueryLibInterface)))

(def native-lib (PgQueryLibInterface/load "libpg_query.dylib"))
(def parse-sql (transpiler/make-parser native-lib))
(def ->dsql (transpiler/make native-lib))


(parse-sql "select * from user where id = '1'")
(->dsql "select * from user where id = '1'")
```

To run tests:
```bash
clojure -X:test
```

## Requirements

- JDK 11 or later
- Clojure 1.11 or later
- libpg_query native library must be available in your system's library path

## Docker

```
docker build -t sql2dsql .
docker run -ti -p 3000:3000 --env-file ./.env.example sql2dsql
```

## AST Progress

| Keyword | Implemented |
| ---- | ------------ |
| :RangeVar | 🟢 |
| :TableFunc | 🔴 |
| :IntoClause | 🔴 |
| :Var | 🔴 |
| :Param | 🟢 |
| :Aggref | 🔴 |
| :GroupingFunc | 🔴 |
| :WindowFunc | 🔴 |
| :WindowFuncRunCondition | 🔴 |
| :MergeSupportFunc | 🔴 |
| :SubscriptingRef | 🔴 |
| :FuncExpr | 🔴 |
| :NamedArgExpr | 🔴 |
| :OpExpr | 🔴 |
| :DistinctExpr | 🔴 |
| :NullIfExpr | 🔴 |
| :ScalarArrayOpExpr | 🔴 |
| :BoolExpr | 🟢 |
| :SubLink | 🟢 |
| :SubPlan | 🔴 |
| :AlternativeSubPlan | 🔴 |
| :FieldSelect | 🔴 |
| :FieldStore | 🔴 |
| :RelabelType | 🔴 |
| :CoerceViaIO | 🔴 |
| :ArrayCoerceExpr | 🔴 |
| :ConvertRowtypeExpr | 🔴 |
| :CollateExpr | 🔴 |
| :CaseExpr | 🟢 |
| :CaseWhen | 🟢 |
| :CaseTestExpr | 🔴 |
| :ArrayExpr | 🔴 |
| :RowExpr | 🟢 |
| :RowCompareExpr | 🔴 |
| :CoalesceExpr | 🟢 |
| :MinMaxExpr | 🟢 |
| :SQLValueFunction | 🟢 |
| :XmlExpr | 🔴 |
| :JsonFormat | 🔴 |
| :JsonReturning | 🔴 |
| :JsonValueExpr | 🔴 |
| :JsonConstructorExpr | 🔴 |
| :JsonIsPredicate | 🔴 |
| :JsonBehavior | 🔴 |
| :JsonExpr | 🔴 |
| :JsonTablePath | 🔴 |
| :JsonTablePathScan | 🔴 |
| :JsonTableSiblingJoin | 🔴 |
| :NullTest | 🟢 |
| :BooleanTest | 🔴 |
| :MergeAction | 🔴 |
| :CoerceToDomain | 🔴 |
| :CoerceToDomainValue | 🔴 |
| :SetToDefault | 🟢 |
| :CurrentOfExpr | 🔴 |
| :NextValueExpr | 🔴 |
| :InferenceElem | 🔴 |
| :TargetEntry | 🔴 |
| :RangeTblRef | 🔴 |
| :JoinExpr | 🟢 |
| :FromExpr | 🔴 |
| :OnConflictExpr | 🔴 |
| :Query | 🔴 |
| :TypeName | 🔴 |
| :ColumnRef | 🟢 |
| :ParamRef | 🟢 |
| :A_Expr | 🟢 |
| :TypeCast | 🟢 |
| :CollateClause | 🔴 |
| :RoleSpec | 🔴 |
| :FuncCall | 🟢 |
| :A_Star | 🔴 |
| :A_Indices | 🔴 |
| :A_Indirection | 🔴 |
| :A_ArrayExpr | 🟢 |
| :ResTarget | 🟢 |
| :MultiAssignRef | 🔴 |
| :SortBy | 🟢 |
| :WindowDef | 🔴 |
| :RangeSubselect | 🟢 |
| :RangeFunction | 🔴 |
| :RangeTableFunc | 🔴 |
| :RangeTableFuncCol | 🔴 |
| :RangeTableSample | 🔴 |
| :ColumnDef | 🟢 |
| :TableLikeClause | 🔴 |
| :IndexElem | 🟢 |
| :DefElem | 🟢 |
| :LockingClause | 🔴 |
| :XmlSerialize | 🔴 |
| :PartitionElem | 🔴 |
| :PartitionSpec | 🔴 |
| :PartitionBoundSpec | 🔴 |
| :PartitionRangeDatum | 🔴 |
| :SinglePartitionSpec | 🔴 |
| :PartitionCmd | 🔴 |
| :RangeTblEntry | 🔴 |
| :RTEPermissionInfo | 🔴 |
| :RangeTblFunction | 🔴 |
| :TableSampleClause | 🔴 |
| :WithCheckOption | 🔴 |
| :SortGroupClause | 🔴 |
| :GroupingSet | 🔴 |
| :WindowClause | 🔴 |
| :RowMarkClause | 🔴 |
| :WithClause | 🔴 |
| :InferClause | 🔴 |
| :OnConflictClause | 🔴 |
| :CTESearchClause | 🔴 |
| :CTECycleClause | 🔴 |
| :CommonTableExpr | 🔴 |
| :MergeWhenClause | 🔴 |
| :TriggerTransition | 🔴 |
| :JsonOutput | 🔴 |
| :JsonArgument | 🔴 |
| :JsonFuncExpr | 🔴 |
| :JsonTablePathSpec | 🔴 |
| :JsonTable | 🔴 |
| :JsonTableColumn | 🔴 |
| :JsonKeyValue | 🔴 |
| :JsonParseExpr | 🔴 |
| :JsonScalarExpr | 🔴 |
| :JsonSerializeExpr | 🔴 |
| :JsonObjectConstructor | 🔴 |
| :JsonArrayConstructor | 🔴 |
| :JsonArrayQueryConstructor | 🔴 |
| :JsonAggConstructor | 🔴 |
| :JsonObjectAgg | 🔴 |
| :JsonArrayAgg | 🔴 |
| :RawStmt | 🔴 |
| :InsertStmt | 🟢 |
| :DeleteStmt | 🔴 |
| :UpdateStmt | 🟢 |
| :MergeStmt | 🔴 |
| :SelectStmt | 🟢 |
| :SetOperationStmt | 🔴 |
| :ReturnStmt | 🔴 |
| :PLAssignStmt | 🔴 |
| :CreateSchemaStmt | 🔴 |
| :AlterTableStmt | 🔴 |
| :ReplicaIdentityStmt | 🔴 |
| :AlterTableCmd | 🔴 |
| :AlterCollationStmt | 🔴 |
| :AlterDomainStmt | 🔴 |
| :GrantStmt | 🔴 |
| :ObjectWithArgs | 🔴 |
| :AccessPriv | 🔴 |
| :GrantRoleStmt | 🔴 |
| :AlterDefaultPrivilegesStmt | 🔴 |
| :CopyStmt | 🔴 |
| :VariableSetStmt | 🔴 |
| :VariableShowStmt | 🔴 |
| :CreateStmt | 🟢 |
| :Constraint | 🟢 |
| :CreateTableSpaceStmt | 🔴 |
| :DropTableSpaceStmt | 🔴 |
| :AlterTableSpaceOptionsStmt | 🔴 |
| :AlterTableMoveAllStmt | 🔴 |
| :CreateExtensionStmt | 🟢 |
| :AlterExtensionStmt | 🔴 |
| :AlterExtensionContentsStmt | 🔴 |
| :CreateFdwStmt | 🔴 |
| :AlterFdwStmt | 🔴 |
| :CreateForeignServerStmt | 🔴 |
| :AlterForeignServerStmt | 🔴 |
| :CreateForeignTableStmt | 🔴 |
| :CreateUserMappingStmt | 🔴 |
| :AlterUserMappingStmt | 🔴 |
| :DropUserMappingStmt | 🔴 |
| :ImportForeignSchemaStmt | 🔴 |
| :CreatePolicyStmt | 🔴 |
| :AlterPolicyStmt | 🔴 |
| :CreateAmStmt | 🔴 |
| :CreateTrigStmt | 🔴 |
| :CreateEventTrigStmt | 🔴 |
| :AlterEventTrigStmt | 🔴 |
| :CreatePLangStmt | 🔴 |
| :CreateRoleStmt | 🔴 |
| :AlterRoleStmt | 🔴 |
| :AlterRoleSetStmt | 🔴 |
| :DropRoleStmt | 🔴 |
| :CreateSeqStmt | 🔴 |
| :AlterSeqStmt | 🔴 |
| :DefineStmt | 🔴 |
| :CreateDomainStmt | 🔴 |
| :CreateOpClassStmt | 🔴 |
| :CreateOpClassItem | 🔴 |
| :CreateOpFamilyStmt | 🔴 |
| :AlterOpFamilyStmt | 🔴 |
| :DropStmt | 🔴 |
| :TruncateStmt | 🔴 |
| :CommentStmt | 🔴 |
| :SecLabelStmt | 🔴 |
| :DeclareCursorStmt | 🔴 |
| :ClosePortalStmt | 🔴 |
| :FetchStmt | 🔴 |
| :IndexStmt | 🟢 |
| :CreateStatsStmt | 🔴 |
| :StatsElem | 🔴 |
| :AlterStatsStmt | 🔴 |
| :CreateFunctionStmt | 🔴 |
| :FunctionParameter | 🔴 |
| :AlterFunctionStmt | 🔴 |
| :DoStmt | 🔴 |
| :InlineCodeBlock | 🔴 |
| :CallStmt | 🔴 |
| :CallContext | 🔴 |
| :RenameStmt | 🔴 |
| :AlterObjectDependsStmt | 🔴 |
| :AlterObjectSchemaStmt | 🔴 |
| :AlterOwnerStmt | 🔴 |
| :AlterOperatorStmt | 🔴 |
| :AlterTypeStmt | 🔴 |
| :RuleStmt | 🔴 |
| :NotifyStmt | 🔴 |
| :ListenStmt | 🔴 |
| :UnlistenStmt | 🔴 |
| :TransactionStmt | 🔴 |
| :CompositeTypeStmt | 🔴 |
| :CreateEnumStmt | 🔴 |
| :CreateRangeStmt | 🔴 |
| :AlterEnumStmt | 🔴 |
| :ViewStmt | 🔴 |
| :LoadStmt | 🔴 |
| :CreatedbStmt | 🔴 |
| :AlterDatabaseStmt | 🔴 |
| :AlterDatabaseRefreshCollStmt | 🔴 |
| :AlterDatabaseSetStmt | 🔴 |
| :DropdbStmt | 🔴 |
| :AlterSystemStmt | 🔴 |
| :ClusterStmt | 🔴 |
| :VacuumStmt | 🔴 |
| :VacuumRelation | 🔴 |
| :ExplainStmt | 🟢 |
| :CreateTableAsStmt | 🟢 |
| :RefreshMatViewStmt | 🔴 |
| :CheckPointStmt | 🔴 |
| :DiscardStmt | 🔴 |
| :LockStmt | 🔴 |
| :ConstraintsSetStmt | 🔴 |
| :ReindexStmt | 🔴 |
| :CreateConversionStmt | 🔴 |
| :CreateCastStmt | 🔴 |
| :CreateTransformStmt | 🔴 |
| :PrepareStmt | 🔴 |
| :ExecuteStmt | 🔴 |
| :DeallocateStmt | 🔴 |
| :DropOwnedStmt | 🔴 |
| :ReassignOwnedStmt | 🔴 |
| :AlterTSDictionaryStmt | 🔴 |
| :AlterTSConfigurationStmt | 🔴 |
| :PublicationTable | 🔴 |
| :PublicationObjSpec | 🔴 |
| :CreatePublicationStmt | 🔴 |
| :AlterPublicationStmt | 🔴 |
| :CreateSubscriptionStmt | 🔴 |
| :AlterSubscriptionStmt | 🔴 |
| :DropSubscriptionStmt | 🔴 |
| :Integer | 🔴 |
| :Float | 🔴 |
| :Boolean | 🔴 |
| :String | 🔴 |
| :BitString | 🔴 |
| :List | 🟢 |
| :IntList | 🔴 |
| :OidList | 🔴 |
| :A_Const | 🟢 |
