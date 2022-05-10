package com.github.zabetak.calcite.tutorial.DataType;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.util.Util;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;


public class JavaTypeFactoryImplUnique extends JavaTypeFactoryImpl implements RelDataTypeFactoryUnique {

    @Override public RelDataType createSqlType(SqlTypeName typeName) {
        if (typeName.allowsPrec()) {
            return createSqlType(typeName, typeSystem.getDefaultPrecision(typeName));
        }
        assertBasic(typeName);
        RelDataType newType = new BasicSqlTypeUnique(typeSystem, typeName);
        return canonize(newType);
    }

    @Override public RelDataType createSqlType(
            SqlTypeName typeName,
            int precision) {
        final int maxPrecision = typeSystem.getMaxPrecision(typeName);
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision;
        }
        if (typeName.allowsScale()) {
            return createSqlType(typeName, precision, typeName.getDefaultScale());
        }
        assertBasic(typeName);
        assert (precision >= 0)
                || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        // Does not check precision when typeName is SqlTypeName#NULL.
        RelDataType newType = precision == RelDataType.PRECISION_NOT_SPECIFIED
                ? new BasicSqlTypeUnique(typeSystem, typeName)
                : new BasicSqlTypeUnique(typeSystem, typeName, precision);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }

    @Override public RelDataType createSqlType(
            SqlTypeName typeName,
            int precision,
            int scale) {
        assertBasic(typeName);
        assert (precision >= 0)
                || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        final int maxPrecision = typeSystem.getMaxPrecision(typeName);
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision;
        }
        RelDataType newType =
                new BasicSqlTypeUnique(typeSystem, typeName, precision, scale);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }

    @Override public RelDataType createTypeWithCharsetAndCollation(
            RelDataType type,
            Charset charset,
            SqlCollation collation) {
        assert SqlTypeUtil.inCharFamily(type) : type;
        requireNonNull(charset, "charset");
        requireNonNull(collation, "collation");
        RelDataType newType;
        if (type instanceof BasicSqlTypeUnique) {
            BasicSqlTypeUnique sqlType = (BasicSqlTypeUnique) type;
            newType = sqlType.createWithCharsetAndCollation(charset, collation);
        } else if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            newType =
                    new JavaType(
                            javaType.getJavaClass(),
                            javaType.isNullable(),
                            charset,
                            collation);
        } else {
            throw Util.needToImplement("need to implement " + type);
        }
        return canonize(newType);
    }

    private static void assertBasic(SqlTypeName typeName) {
        assert typeName != null;
        assert typeName != SqlTypeName.MULTISET
                : "use createMultisetType() instead";
        assert typeName != SqlTypeName.ARRAY
                : "use createArrayType() instead";
        assert typeName != SqlTypeName.MAP
                : "use createMapType() instead";
        assert typeName != SqlTypeName.ROW
                : "use createStructType() instead";
        assert !SqlTypeName.INTERVAL_TYPES.contains(typeName)
                : "use createSqlIntervalType() instead";
    }

    @Override public RelDataType createTypeWithNullability(
            final RelDataType type,
            final boolean nullable) {
        final RelDataType newType;
        if (type instanceof BasicSqlTypeUnique) {
            newType = ((BasicSqlTypeUnique) type).createWithNullability(nullable);
        } else {
            return super.createTypeWithNullability(type, nullable);
        }
        return canonize(newType);
    }

    public RelDataType createTypeWithUnique(
            final RelDataType type,
            final boolean unique) {
        final RelDataType newType;
        newType = ((BasicSqlTypeUnique) type).createWithUnique(unique);
        return newType;
    }

    @Override public Type getJavaClass(RelDataType type) {
        if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            return javaType.getJavaClass();
        }
        if (type instanceof BasicSqlTypeUnique || type instanceof IntervalSqlType) {
            switch (type.getSqlTypeName()) {
                case VARCHAR:
                case CHAR:
                    return String.class;
                case DATE:
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                case INTEGER:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return type.isNullable() ? Integer.class : int.class;
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case BIGINT:
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    return type.isNullable() ? Long.class : long.class;
                case SMALLINT:
                    return type.isNullable() ? Short.class : short.class;
                case TINYINT:
                    return type.isNullable() ? Byte.class : byte.class;
                case DECIMAL:
                    return BigDecimal.class;
                case BOOLEAN:
                    return type.isNullable() ? Boolean.class : boolean.class;
                case DOUBLE:
                case FLOAT: // sic
                    return type.isNullable() ? Double.class : double.class;
                case REAL:
                    return type.isNullable() ? Float.class : float.class;
                case BINARY:
                case VARBINARY:
                    return ByteString.class;
                case GEOMETRY:
                    return Geometries.Geom.class;
                case SYMBOL:
                    return Enum.class;
                case ANY:
                    return Object.class;
                case NULL:
                    return Void.class;
                default:
                    break;
            }
        }
        switch (type.getSqlTypeName()) {
            /*case ROW:
                assert type instanceof RelRecordType;
                if (type instanceof JavaRecordType) {
                    return ((JavaRecordType) type).clazz;
                } else {
                    return createSyntheticType((RelRecordType) type);
                }*/
            case MAP:
                return Map.class;
            case ARRAY:
            case MULTISET:
                return List.class;
            default:
                break;
        }
        return Object.class;
    }
}
