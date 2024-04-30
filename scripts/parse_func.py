# Import necessary modules
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
import pymysql
import json

# Define column mappings for each table
table_column_mappings = {
    'users': {
        'UNKNOWN_COL0': 'id',
        'UNKNOWN_COL1': 'name',
        'UNKNOWN_COL2': 'email',
        'UNKNOWN_COL3': 'address',
        'UNKNOWN_COL4': 'phone_number'
    },
    'products': {
        'UNKNOWN_COL0': 'id',
        'UNKNOWN_COL1': 'name',
        'UNKNOWN_COL2': 'price',
        'UNKNOWN_COL3': 'description',
        'UNKNOWN_COL4': 'category'
    },
    'payments': {
        'UNKNOWN_COL0': 'id',
        'UNKNOWN_COL1': 'user_id',
        'UNKNOWN_COL2': 'product_id',
        'UNKNOWN_COL3': 'amount',
        'UNKNOWN_COL4': 'order_time'
    }
}

# Define mapping for converting table name
table_name_convertor_dict = {
    'users': 'users_dim',
    'products': 'products_dim',
    'payments': 'order_fact'
}

def parse_source_changes(event_value, source_table_name):
    """
    Parse the changes in the source event data based on column mappings.

    Args:
    - event_value (dict): Event data dictionary.
    - source_table_name (str): Name of the source table.

    Returns:
    - dict: Parsed event data dictionary.
    """
    parsed_event_dict = {}
    column_mapping = table_column_mappings.get(source_table_name, {})
    for key, value in event_value.items():
        column_name = column_mapping.get(key)
        if column_name:
            parsed_event_dict[column_name] = value
    return parsed_event_dict

def parse_insert_event(parsed_event, source_table_name):
    """
    Parse insert event and generate SQL query for insertion.

    Args:
    - parsed_event (dict): Parsed event data dictionary.
    - source_table_name (str): Name of the source table.

    Returns:
    - tuple: SQL query and values tuple.
    """
    columns = ', '.join(parsed_event.keys())
    placeholders = ', '.join(['%s'] * len(parsed_event))
    sql = f"INSERT INTO {table_name_convertor_dict[source_table_name]} ({columns}) VALUES ({placeholders})"
    values = tuple(parsed_event.values())
    return sql, values

def parse_edit_event(parsed_event, source_table_name):
    """
    Parse update event and generate SQL query for update.

    Args:
    - parsed_event (dict): Parsed event data dictionary.
    - source_table_name (str): Name of the source table.

    Returns:
    - tuple: SQL query and values tuple.
    """
    update_values = ', '.join([f"{column} = %s" for column in parsed_event.keys()])
    sql = f"UPDATE {table_name_convertor_dict[source_table_name]} SET {update_values} WHERE id = %s"
    return sql, tuple(parsed_event.values()) + (parsed_event['id'],)

def parse_delete_event(parsed_event, source_table_name):
    """
    Parse delete event and generate SQL query for deletion.

    Args:
    - parsed_event (dict): Parsed event data dictionary.
    - source_table_name (str): Name of the source table.

    Returns:
    - tuple: SQL query and values tuple.
    """
    sql = f"DELETE FROM {table_name_convertor_dict[source_table_name]} WHERE id = %s"
    return sql, (parsed_event['id'],)

def apply_tansformation(row_event, cursor, table_name):
    """
    Apply transformation to the row event data and execute SQL queries accordingly.

    Args:
    - row_event: Row event object from the binary log stream.
    - cursor: Cursor object for executing SQL queries.
    - table_name: Name of the source table.

    Returns:
    - None
    """
    if isinstance(row_event, WriteRowsEvent):
        # Insert operation
        for row in row_event.rows:
            values = row["values"]
            parsed_dict = parse_source_changes(values, table_name)
            sql, values = parse_insert_event(parsed_dict, table_name)
            cursor.execute(sql, values)
            
    elif isinstance(row_event, UpdateRowsEvent):
        # Update operation
        for row in row_event.rows:
            old_values = row["before_values"]
            new_values = row["after_values"]
            old_parsed_dict = parse_source_changes(old_values, table_name)
            new_parsed_dict = parse_source_changes(new_values, table_name)
            sql, values = parse_edit_event(new_parsed_dict, table_name)
            cursor.execute(sql, values)
            
    elif isinstance(row_event, DeleteRowsEvent):
        # Delete operation
        for row in row_event.rows:
            values = row["values"]
            parsed_dict = parse_source_changes(values, table_name)
            sql, values = parse_delete_event(parsed_dict, table_name)
            cursor.execute(sql, values)

    # Commit the changes after applying all operations
    cursor.connection.commit()

