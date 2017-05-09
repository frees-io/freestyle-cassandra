# Queries, Deserialization, etc.

Queries are managed with a ResultSet. That implies that we should use the `getInt`, `getString`, ...

Also, exists the Mapper:
 * http://docs.datastax.com/en/developer/java-driver/3.2/manual/object_mapper/

For field values, we can use a custom TypeCodec:
 * http://docs.datastax.com/en/drivers/java/3.2/com/datastax/driver/core/TypeCodec.html