\copy "region"     from '<file_directory>/data/csv_data/region.csv'     DELIMITER ',' CSV;
\copy "nation"     from '<file_directory>/data/csv_data/nation.csv'     DELIMITER ',' CSV;
\copy "supplier"   from '<file_directory>/data/csv_data/supplier.csv'   DELIMITER ',' CSV;
\copy "part"       from '<file_directory>/data/csv_data/part.csv'       DELIMITER ',' CSV;
\copy "partsupp"   from '<file_directory>/data/csv_data/partsupp.csv'   DELIMITER ',' CSV;
\copy "customer"   from '<file_directory>/data/csv_data/customer.csv'   DELIMITER ',' CSV;
\copy "orders"     from '<file_directory>/data/csv_data/orders.csv'     DELIMITER ',' CSV;
\copy "lineitem"   from '<file_directory>/data/csv_data/lineitem.csv'   DELIMITER ',' CSV;