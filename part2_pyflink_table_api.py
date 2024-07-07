from pyflink.table import (EnvironmentSettings, TableEnvironment,
                           TableDescriptor, Schema, DataTypes)

#01_batch_csv_process.py
#=======================

def main():
    # Flink can handle batch and streaming data. 
    # This script is to handle batch data in csv. 
    # Download csv from https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k
    # Remove the header, keep 3000 around records, then save it in sensor-source folder
    # In part4_pyflink_kafka_tumbling_window.py, we can see how it handles streaming data. 
    settings = EnvironmentSettings.in_batch_mode()

    # Create table environment, before creating a table. 
    tenv = TableEnvironment.create(settings)

    # Create table schema
    field_names = ['ts', 'device', 'co', 'humidity',
                   'light', 'lpg', 'motion', 'smoke', 'temp']
    field_types = [DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING(), DataTypes.STRING(),
                   DataTypes.STRING()]
    schema = Schema.new_builder().from_fields(field_names, field_types).build()
 
    # Source data path. Also can use the relative directory path: source_path_tableapi = 'sensor-source'
    source_path_tableapi = 'D:\Projects\GitHub\Kafka_PyFlink_Getting_Started\sensor-source'  
    
    # A TableEnvironment maintains a map of catalogs of tables which are created with an identifier. 
    # Each identifier consists of 3 parts: catalog name, database name and object name. 
    # If a catalog or database is not specified, the current default value will be used.

    # Tables can be either virtual (VIEWS) or regular (TABLES). 
    # VIEWS can be created from an existing Table object, usually the result of a Table API or SQL query. 
    # TABLES describe external data, such as a file, database table, or message queue.
    
    # table name is device_data
    # catalog name and database name are not specified, default value will be used. Here it is transient catalog.
    tenv.create_table(
        'device_data',
        TableDescriptor.for_connector('filesystem')
        .schema(schema)
        .option('path', f'{source_path_tableapi}')
        .format('csv')
        .build()
    )

    # tenv.from_path() scans the registered devide_data table from the catalog and returns the reference in the device_tab handle.
    device_tab = tenv.from_path('device_data')
    # print(device_tab.print_schema())
    # print(device_tab.to_pandas().head())

    # DSL(Domain-Specific Language) query
    distinct_devices = device_tab.select(device_tab.device).distinct()
    # print(distinct_devices.to_pandas())

    high_temp_devices = device_tab.select(device_tab.ts, device_tab.device, device_tab.temp) \
                                    .where(device_tab.temp >= "20")

    print(high_temp_devices.to_pandas())
    print('\n')
    print("Explain plan for high_temp_device query \n")
    print(high_temp_devices.explain())


if __name__ == '__main__':
    main()