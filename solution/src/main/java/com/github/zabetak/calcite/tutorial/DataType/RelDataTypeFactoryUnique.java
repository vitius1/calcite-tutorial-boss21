package com.github.zabetak.calcite.tutorial.DataType;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface RelDataTypeFactoryUnique extends RelDataTypeFactory {
    RelDataType createTypeWithUnique(
            RelDataType type,
            boolean unique);

    class Builder {
        private final List<String> names = new ArrayList<>();
        private final List<RelDataType> types = new ArrayList<>();
        private StructKind kind = StructKind.FULLY_QUALIFIED;
        private final RelDataTypeFactoryUnique typeFactory;
        private boolean nullableRecord = false;
        private boolean uniqueRecord = false;

        /**
         * Creates a Builder with the given type factory.
         */
        public Builder(RelDataTypeFactory typeFactory) {
            this.typeFactory = (RelDataTypeFactoryUnique) Objects.requireNonNull(typeFactory, "typeFactory");
        }

        /**
         * Returns the number of fields.
         *
         * @return number of fields
         */
        public int getFieldCount() {
            return names.size();
        }

        /**
         * Returns the name of a given field.
         *
         * @param index Ordinal of field
         * @return Name of given field
         */
        public String getFieldName(int index) {
            return names.get(index);
        }

        /**
         * Returns the type of a given field.
         *
         * @param index Ordinal of field
         * @return Type of given field
         */
        public RelDataType getFieldType(int index) {
            return types.get(index);
        }

        /**
         * Adds a field with given name and type.
         * @return
         */
        public Builder add(String name, RelDataType type) {
            names.add(name);
            types.add(type);
            return this;
        }

        /**
         * Adds a field with a type created using
         * {@link org.apache.calcite.rel.type.RelDataTypeFactory#createSqlType(org.apache.calcite.sql.type.SqlTypeName)}.
         * @return
         */
        public Builder add(String name, SqlTypeName typeName) {
            add(name, typeFactory.createSqlType(typeName));
            return this;
        }

        /**
         * Adds a field with a type created using
         * {@link org.apache.calcite.rel.type.RelDataTypeFactory#createSqlType(org.apache.calcite.sql.type.SqlTypeName, int)}.
         * @return
         */
        public Builder add(String name, SqlTypeName typeName, int precision) {
            add(name, typeFactory.createSqlType(typeName, precision));
            return this;
        }

        /**
         * Adds a field with a type created using
         * {@link org.apache.calcite.rel.type.RelDataTypeFactory#createSqlType(org.apache.calcite.sql.type.SqlTypeName, int, int)}.
         * @return
         */
        public Builder add(String name, SqlTypeName typeName, int precision,
                           int scale) {
            add(name, typeFactory.createSqlType(typeName, precision, scale));
            return this;
        }

        /**
         * Adds a field with an interval type.
         * @return
         */
        public Builder add(String name, TimeUnit startUnit, int startPrecision,
                           TimeUnit endUnit, int fractionalSecondPrecision) {
            final SqlIntervalQualifier q =
                    new SqlIntervalQualifier(startUnit, startPrecision, endUnit,
                            fractionalSecondPrecision, SqlParserPos.ZERO);
            add(name, typeFactory.createSqlIntervalType(q));
            return this;
        }

        /**
         * Changes the nullability of the last field added.
         *
         * @throws java.lang.IndexOutOfBoundsException if no fields have been
         *                                             added
         * @return
         */
        public Builder nullable(boolean nullable) {
            RelDataType lastType = types.get(types.size() - 1);
            if (lastType.isNullable() != nullable) {
                final RelDataType type =
                        typeFactory.createTypeWithNullability(lastType, nullable);
                types.set(types.size() - 1, type);
            }
            return this;
        }

        public Builder unique(boolean unique) {
            RelDataType lastType = types.get(types.size() - 1);
            final RelDataType type =
                    typeFactory.createTypeWithUnique(lastType, unique);
            types.set(types.size() - 1, type);
            return this;
        }



        /**
         * Adds a field. Field's ordinal is ignored.
         * @return
         */
        public Builder add(RelDataTypeField field) {
            add(field.getName(), field.getType());
            return this;
        }

        /**
         * Adds all fields in a collection.
         * @return
         */
        public Builder addAll(
                Iterable<? extends Map.Entry<String, RelDataType>> fields) {
            for (Map.Entry<String, RelDataType> field : fields) {
                add(field.getKey(), field.getValue());
            }
            return this;
        }

        public Builder kind(StructKind kind) {
            this.kind = kind;
            return this;
        }

        /** Sets whether the record type will be nullable.
         * @return*/
        public Builder nullableRecord(boolean nullableRecord) {
            this.nullableRecord = nullableRecord;
            return this;
        }

        public Builder uniqueRecord(boolean uniqueRecord) {
            this.uniqueRecord = uniqueRecord;
            return this;
        }

        /**
         * Makes sure that field names are unique.
         * @return
         */
        public Builder uniquify() {
            final List<String> uniqueNames = SqlValidatorUtil.uniquify(names,
                    typeFactory.getTypeSystem().isSchemaCaseSensitive());
            if (uniqueNames != names) {
                names.clear();
                names.addAll(uniqueNames);
            }
            return this;
        }

        /**
         * Creates a struct type with the current contents of this builder.
         */
        public RelDataType build() {
            return typeFactory.createTypeWithNullability(
                    typeFactory.createStructType(kind, types, names),
                    nullableRecord);
        }

        /** Creates a dynamic struct type with the current contents of this
         * builder. */
        public RelDataType buildDynamic() {
            final RelDataType dynamicType = new DynamicRecordTypeImpl(typeFactory);
            final RelDataType type = build();
            dynamicType.getFieldList().addAll(type.getFieldList());
            return dynamicType;
        }

        /** Returns whether a field exists with the given name. */
        public boolean nameExists(String name) {
            return names.contains(name);
        }
    }
}
