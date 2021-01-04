package dev.psmolinski.kafka.connect.jdbc;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider;
import io.confluent.connect.jdbc.dialect.OracleDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.common.config.AbstractConfig;

/**
 * Extension to the regular Oracle database dialect allowing the user to specify the column type.
 * <br/>
 * The original dialect assumes fixed types for given incoming field types. In particular it creates
 * BLOB and CLOB columns for {@link org.apache.kafka.connect.data.Schema.Type#BYTES BYTES} and
 * {@link org.apache.kafka.connect.data.Schema.Type#STRING STRING} respectively.
 * @author <a href="piotr.smolinski@confluent.io">Piotr Smolinski</a>
 */
public class CustomizableOracleDatabaseDialect extends OracleDatabaseDialect {

    public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
        public Provider() {
            super(CustomizableOracleDatabaseDialect.class.getSimpleName());
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new CustomizableOracleDatabaseDialect(config);
        }
    }

    public CustomizableOracleDatabaseDialect(AbstractConfig config) {
        super(config);
    }

    /**
     * Use schema hint or configuration to specify the result column type.
     * <br/>
     * The column type is inferred as:
     * <ul>
     *     <li>if schema field has parameter {@code sql.type}, use the provided value</li>
     *     <li>if schema field has logical name, lookup the SQL type in the connector configuration
     *     under key {@code default.sql.types.&lt;name&gt;}
     *     </li>
     *     <li>lookup the SQL type in the connector configuration
     *     under key {@code default.sql.types.&lt;type-name&gt;}
     *     </li>
     *     <li>fallback to the original implementation otherwise
     *     </li>
     * </ul>
     * @param field
     * @return
     */
    protected String getSqlType(SinkRecordField field) {
        String sqlType = null;
        if (field.schemaParameters()!=null) sqlType = field.schemaParameters().get("sql.type");
        if (sqlType!=null) return sqlType;
        if (field.schemaName()!=null) sqlType = this.config.originalsStrings().get("default.sql.types."+field.schemaName());
        if (sqlType!=null) return sqlType;
        sqlType = this.config.originalsStrings().get("default.sql.types."+field.schemaType().getName());
        if (sqlType!=null) return sqlType;
        return super.getSqlType(field);
    }

}
