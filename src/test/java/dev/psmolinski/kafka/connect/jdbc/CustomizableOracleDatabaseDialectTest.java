package dev.psmolinski.kafka.connect.jdbc;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.QuoteMethod;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class CustomizableOracleDatabaseDialectTest {

    private JdbcSinkConfig config;

    @BeforeEach
    public void beforeEach() {

        Map<String, String> connProps = new HashMap<>();
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
        connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:oracle:thin://something");
        connProps.put(JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_CONFIG, QuoteMethod.NEVER.name());
        connProps.put(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG, "CustomizableOracleDatabaseDialect");

        connProps.put("default.sql.types.string", "VARCHAR2(100)");

        this.config = new JdbcSinkConfig(connProps);

    }

    @Test
    public void testFieldLevelDefinition() {
        CustomizableOracleDatabaseDialect dialect = new CustomizableOracleDatabaseDialect(config);
        Schema schema = SchemaBuilder
                .string()
                .parameter("sql.type", "VARCHAR2(200)")
                .build();
        SinkRecordField field = new SinkRecordField(schema, "test", false);
        Assertions.assertThat(dialect.getSqlType(field))
                .isEqualTo("VARCHAR2(200)");
    }

    @Test
    public void testDefaultMapping() {
        CustomizableOracleDatabaseDialect dialect = new CustomizableOracleDatabaseDialect(config);
        Schema schema = SchemaBuilder
                .string()
                .build();
        SinkRecordField field = new SinkRecordField(schema, "test", false);
        Assertions.assertThat(dialect.getSqlType(field))
                .isEqualTo("VARCHAR2(100)");
    }

    @Test
    public void testResolved() {
        DatabaseDialect dialect = DatabaseDialects.create(config.dialectName, config);
        Assertions.assertThat(dialect)
                .isInstanceOf(CustomizableOracleDatabaseDialect.class);
    }

}
