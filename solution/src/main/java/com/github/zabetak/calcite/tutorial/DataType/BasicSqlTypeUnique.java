package com.github.zabetak.calcite.tutorial.DataType;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.SerializableCharset;

import com.google.common.base.Preconditions;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.Objects;

public class BasicSqlTypeUnique extends AbstractSqlType {

    //~ Instance fields --------------------------------------------------------

    private final int precision;
    private final int scale;
    private final RelDataTypeSystem typeSystem;
    private final @Nullable SqlCollation collation;
    private final @Nullable SerializableCharset wrappedCharset;
    public boolean isUnique;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a type with no parameters. This should only be called from a
     * factory method.
     *
     * @param typeSystem Type system
     * @param typeName Type name
     */
    public BasicSqlTypeUnique(RelDataTypeSystem typeSystem, SqlTypeName typeName) {
        this(typeSystem, typeName, false, false, PRECISION_NOT_SPECIFIED,
                SCALE_NOT_SPECIFIED, null, null);
        checkPrecScale(typeName, false, false);
    }



    /** Throws if {@code typeName} does not allow the given combination of
     * precision and scale. */
    protected static void checkPrecScale(SqlTypeName typeName,
                                         boolean precisionSpecified, boolean scaleSpecified) {
        if (!typeName.allowsPrecScale(precisionSpecified, scaleSpecified)) {
            throw new AssertionError("typeName.allowsPrecScale("
                    + precisionSpecified + ", " + scaleSpecified + "): " + typeName);
        }
    }

    /**
     * Constructs a type with precision/length but no scale.
     *
     * @param typeSystem Type system
     * @param typeName Type name
     * @param precision Precision (called length for some types)
     */
    public BasicSqlTypeUnique(RelDataTypeSystem typeSystem, SqlTypeName typeName,
                        int precision) {
        this(typeSystem, typeName, false, false, precision, SCALE_NOT_SPECIFIED, null,
                null);
        checkPrecScale(typeName, true, false);
    }

    /**
     * Constructs a type with precision/length and scale.
     *
     * @param typeSystem Type system
     * @param typeName Type name
     * @param precision Precision (called length for some types)
     * @param scale Scale
     */
    public BasicSqlTypeUnique(RelDataTypeSystem typeSystem, SqlTypeName typeName,
                        int precision, int scale) {
        this(typeSystem, typeName, false, false, precision, scale, null, null);
        checkPrecScale(typeName, true, true);
    }

    /** Internal constructor. */
    private BasicSqlTypeUnique(
            RelDataTypeSystem typeSystem,
            SqlTypeName typeName,
            boolean nullable,
            boolean unique,
            int precision,
            int scale,
            @Nullable SqlCollation collation,
            @Nullable SerializableCharset wrappedCharset) {
        super(typeName, nullable, null);
        this.typeSystem = Objects.requireNonNull(typeSystem, "typeSystem");
        this.precision = precision;
        this.scale = scale;
        this.collation = collation;
        this.wrappedCharset = wrappedCharset;
        this.isUnique = unique;
        computeDigest();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Constructs a type with nullablity.
     */
    BasicSqlTypeUnique createWithNullability(boolean nullable) {
        if (nullable == this.isNullable) {
            return this;
        }
        return new BasicSqlTypeUnique(this.typeSystem, this.typeName, nullable, this.isUnique,
                this.precision, this.scale, this.collation, this.wrappedCharset);
    }

    /**
     * Constructs a type with unique.
     */
    BasicSqlTypeUnique createWithUnique(boolean unique) {
        if (unique == this.isUnique) {
            return this;
        }
        return new BasicSqlTypeUnique(this.typeSystem, this.typeName, this.isNullable, unique,
                this.precision, this.scale, this.collation, this.wrappedCharset);
    }

    /**
     * Constructs a type with charset and collation.
     *
     * <p>This must be a character type.
     */
    BasicSqlTypeUnique createWithCharsetAndCollation(Charset charset,
                                               SqlCollation collation) {
        Preconditions.checkArgument(SqlTypeUtil.inCharFamily(this));
        return new BasicSqlTypeUnique(this.typeSystem, this.typeName, this.isNullable, this.isUnique,
                this.precision, this.scale, collation,
                SerializableCharset.forCharset(charset));
    }

    @Override public int getPrecision() {
        if (precision == PRECISION_NOT_SPECIFIED) {
            return typeSystem.getDefaultPrecision(typeName);
        }
        return precision;
    }

    @Override public int getScale() {
        if (scale == SCALE_NOT_SPECIFIED) {
            switch (typeName) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case DECIMAL:
                    return 0;
                default:
                    // fall through
            }
        }
        return scale;
    }

    @Override public @Nullable Charset getCharset() {
        return wrappedCharset == null ? null : wrappedCharset.getCharset();
    }

    @Override public @Nullable SqlCollation getCollation() {
        return collation;
    }

    // implement RelDataTypeImpl
    @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        // Called to make the digest, which equals() compares;
        // so equivalent data types must produce identical type strings.

        sb.append(typeName.name());
        boolean printPrecision = precision != PRECISION_NOT_SPECIFIED;
        boolean printScale = scale != SCALE_NOT_SPECIFIED;

        if (printPrecision) {
            sb.append('(');
            sb.append(getPrecision());
            if (printScale) {
                sb.append(", ");
                sb.append(getScale());
            }
            sb.append(')');
        }
        if (!withDetail) {
            return;
        }
        if (wrappedCharset != null
                && !SqlCollation.IMPLICIT.getCharset().equals(wrappedCharset.getCharset())) {
            sb.append(" CHARACTER SET \"");
            sb.append(wrappedCharset.getCharset().name());
            sb.append("\"");
        }
        if (collation != null
                && collation != SqlCollation.IMPLICIT && collation != SqlCollation.COERCIBLE) {
            sb.append(" COLLATE \"");
            sb.append(collation.getCollationName());
            sb.append("\"");
        }
    }

    /**
     * Returns a value which is a limit for this type.
     *
     * <p>For example,
     *
     * <table border="1">
     * <caption>Limits</caption>
     * <tr>
     * <th>Datatype</th>
     * <th>sign</th>
     * <th>limit</th>
     * <th>beyond</th>
     * <th>precision</th>
     * <th>scale</th>
     * <th>Returns</th>
     * </tr>
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483647 (2 ^ 31 -1 = MAXINT)</td>
     * </tr>
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>true</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483648 (2 ^ 31 = MAXINT + 1)</td>
     * </tr>
     * <tr>
     * <td>Integer</td>
     * <td>false</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>-2147483648 (-2 ^ 31 = MININT)</td>
     * </tr>
     * <tr>
     * <td>Boolean</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>TRUE</td>
     * </tr>
     * <tr>
     * <td>Varchar</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>10</td>
     * <td>-1</td>
     * <td>'ZZZZZZZZZZ'</td>
     * </tr>
     * </table>
     *
     * @param sign   If true, returns upper limit, otherwise lower limit
     * @param limit  If true, returns value at or near to overflow; otherwise
     *               value at or near to underflow
     * @param beyond If true, returns the value just beyond the limit, otherwise
     *               the value at the limit
     * @return Limit value
     */
    public @Nullable Object getLimit(
            boolean sign,
            SqlTypeName.Limit limit,
            boolean beyond) {
        int precision = typeName.allowsPrec() ? this.getPrecision() : -1;
        int scale = typeName.allowsScale() ? this.getScale() : -1;
        return typeName.getLimit(
                sign,
                limit,
                beyond,
                precision,
                scale);
    }
}