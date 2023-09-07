package org.opensearch.sql.ppl.utils;


import org.apache.spark.sql.types.ByteType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * translate the PPL ast expressions data-types into catalyst data-types 
 */
public interface DataTypeTransformer {
    static DataType translate(org.opensearch.sql.ast.expression.DataType source) {
        switch (source.getCoreType()) {
            case TIME:
                return DateType$.MODULE$;
            case INTEGER:
                return IntegerType$.MODULE$;
            case BYTE:
                return ByteType$.MODULE$;
            default:
                return StringType$.MODULE$;
        }
    }
    
    static Object translate(Object value, org.opensearch.sql.ast.expression.DataType source) {
        switch (source.getCoreType()) {
            case STRING:
                return UTF8String.fromString(value.toString());
            default:
                return value;
        }
    }
}