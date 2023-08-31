/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

grammar FlintSparkSqlExtensions;

import SparkSqlBase;


// Flint SQL Syntax Extension

singleStatement
    : statement SEMICOLON* EOF
    ;

statement
    : skippingIndexStatement
    | coveringIndexStatement
    ;

skippingIndexStatement
    : createSkippingIndexStatement
    | refreshSkippingIndexStatement
    | describeSkippingIndexStatement
    | dropSkippingIndexStatement
    ;

createSkippingIndexStatement
    : CREATE SKIPPING INDEX ON tableName=multipartIdentifier
        LEFT_PAREN indexColTypeList RIGHT_PAREN
        (WITH LEFT_PAREN propertyList RIGHT_PAREN)?
    ;

refreshSkippingIndexStatement
    : REFRESH SKIPPING INDEX ON tableName=multipartIdentifier
    ;

describeSkippingIndexStatement
    : (DESC | DESCRIBE) SKIPPING INDEX ON tableName=multipartIdentifier
    ;

dropSkippingIndexStatement
    : DROP SKIPPING INDEX ON tableName=multipartIdentifier
    ;

coveringIndexStatement
    : createCoveringIndexStatement
    ;

createCoveringIndexStatement
    : CREATE INDEX indexName=identifier ON tableName=multipartIdentifier
        LEFT_PAREN indexColumns=multipartIdentifierPropertyList RIGHT_PAREN
        (WITH LEFT_PAREN propertyList RIGHT_PAREN)?
    ;

indexColTypeList
    : indexColType (COMMA indexColType)*
    ;

indexColType
    : identifier skipType=(PARTITION | VALUE_SET | MIN_MAX)
    ;