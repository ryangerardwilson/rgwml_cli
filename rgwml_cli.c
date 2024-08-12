#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql/mysql.h>
#include <cjson/cJSON.h>
#include "fort.h" // Include libfort for table formatting

// Structure to store query results
typedef struct {
    char **rows; // Array of strings
    char **headers; // Array of column headers
    char **mysql_types; // Array of MySQL column types
    char **c_types; // Array of C column types
    int rows_count;
    int cols_count;
} QueryResult;

void free_query_result(QueryResult *result) {
    if (!result) return;
    for (int i = 0; i < result->rows_count * result->cols_count; i++) {
        free(result->rows[i]);
    }
    for (int i = 0; i < result->cols_count; i++) {
        free(result->headers[i]);
        free(result->mysql_types[i]);
        free(result->c_types[i]);
    }
    free(result->rows);
    free(result->headers);
    free(result->mysql_types);
    free(result->c_types);
    free(result);
}

char* read_file(const char* filename) {
    FILE* file = fopen(filename, "rb");
    if (!file) {
        fprintf(stderr, "Could not open file %s\n", filename);
        return NULL;
    }
    fseek(file, 0, SEEK_END);
    long length = ftell(file);
    fseek(file, 0, SEEK_SET);
    char* content = (char*)malloc(length + 1);
    if (!content) {
        fprintf(stderr, "Memory allocation failed\n");
        fclose(file);
        return NULL;
    }
    fread(content, 1, length, file);
    fclose(file);
    content[length] = '\0';
    return content;
}

cJSON* get_db_preset(cJSON *json, const char* preset_name) {
    cJSON *db_presets = cJSON_GetObjectItemCaseSensitive(json, "db_presets");
    cJSON *preset = NULL;
    cJSON_ArrayForEach(preset, db_presets) {
        cJSON *name = cJSON_GetObjectItemCaseSensitive(preset, "name");
        if (cJSON_IsString(name) && (strcmp(name->valuestring, preset_name) == 0)) {
            return preset;
        }
    }
    return NULL;
}

typedef struct {
    const char *mysql_type;
    const char *c_type;
} TypeMapping;

TypeMapping mysql_type_to_c_type(enum enum_field_types type) {
    switch (type) {
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            return (TypeMapping){"DECIMAL", "double"};
        case MYSQL_TYPE_TINY:
            return (TypeMapping){"TINYINT", "int8_t"};
        case MYSQL_TYPE_SHORT:
            return (TypeMapping){"SMALLINT", "int16_t"};
        case MYSQL_TYPE_LONG:
            return (TypeMapping){"INT", "int32_t"};
        case MYSQL_TYPE_FLOAT:
            return (TypeMapping){"FLOAT", "float"};
        case MYSQL_TYPE_DOUBLE:
            return (TypeMapping){"DOUBLE", "double"};
        case MYSQL_TYPE_NULL:
            return (TypeMapping){"NULL", "void"};
        case MYSQL_TYPE_TIMESTAMP:
            return (TypeMapping){"TIMESTAMP", "char*"};
        case MYSQL_TYPE_LONGLONG:
            return (TypeMapping){"BIGINT", "int64_t"};
        case MYSQL_TYPE_INT24:
            return (TypeMapping){"MEDIUMINT", "int32_t"};
        case MYSQL_TYPE_DATE:
            return (TypeMapping){"DATE", "char*"};
        case MYSQL_TYPE_TIME:
            return (TypeMapping){"TIME", "char*"};
        case MYSQL_TYPE_DATETIME:
            return (TypeMapping){"DATETIME", "char*"};
        case MYSQL_TYPE_YEAR:
            return (TypeMapping){"YEAR", "int"};
        case MYSQL_TYPE_NEWDATE:
            return (TypeMapping){"NEWDATE", "char*"};
        case MYSQL_TYPE_VARCHAR:
            return (TypeMapping){"VARCHAR", "char*"};
        case MYSQL_TYPE_BIT:
            return (TypeMapping){"BIT", "uint8_t"};
        case MYSQL_TYPE_JSON:
            return (TypeMapping){"JSON", "char*"};
        case MYSQL_TYPE_ENUM:
            return (TypeMapping){"ENUM", "char*"};
        case MYSQL_TYPE_SET:
            return (TypeMapping){"SET", "char*"};
        case MYSQL_TYPE_TINY_BLOB:
            return (TypeMapping){"TINYBLOB", "char*"};
        case MYSQL_TYPE_MEDIUM_BLOB:
            return (TypeMapping){"MEDIUMBLOB", "char*"};
        case MYSQL_TYPE_LONG_BLOB:
            return (TypeMapping){"LONGBLOB", "char*"};
        case MYSQL_TYPE_BLOB:
            return (TypeMapping){"BLOB", "char*"};
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            return (TypeMapping){"STRING", "char*"};
        case MYSQL_TYPE_GEOMETRY:
            return (TypeMapping){"GEOMETRY", "char*"};
        default:
            return (TypeMapping){"UNKNOWN", "void"};
    }
}

QueryResult* execute_mysql_query(const char *host, const char *user, const char *password, const char *database, const char *query) {
    MYSQL *conn;
    MYSQL_RES *res;
    MYSQL_ROW row;
    MYSQL_FIELD *fields;
    QueryResult *result;

    conn = mysql_init(NULL);
    if (!conn) {
        fprintf(stderr, "mysql_init() failed\n");
        return NULL;
    }

    if (!mysql_real_connect(conn, host, user, password, database, 0, NULL, 0)) {
        fprintf(stderr, "Connection failed: %s\n", mysql_error(conn));
        return NULL;
    }

    if (mysql_query(conn, query)) {
        fprintf(stderr, "Query failed: %s\n", mysql_error(conn));
        mysql_close(conn);
        return NULL;
    }

    res = mysql_store_result(conn);
    if (!res) {
        fprintf(stderr, "mysql_store_result() failed: %s\n", mysql_error(conn));
        mysql_close(conn);
        return NULL;
    }

    int rows_count = mysql_num_rows(res);
    int cols_count = mysql_num_fields(res);
    fields = mysql_fetch_fields(res);

    result = (QueryResult *)malloc(sizeof(QueryResult));
    if (!result) {
        fprintf(stderr, "Memory allocation for result failed\n");
        mysql_free_result(res);
        mysql_close(conn);
        return NULL;
    }
    result->rows_count = rows_count;
    result->cols_count = cols_count;
    result->rows = (char **)malloc(rows_count * cols_count * sizeof(char *));
    result->headers = (char **)malloc(cols_count * sizeof(char *));
    result->mysql_types = (char **)malloc(cols_count * sizeof(char *));
    result->c_types = (char **)malloc(cols_count * sizeof(char *));

    if (!result->rows || !result->headers || !result->mysql_types || !result->c_types) {
        fprintf(stderr, "Memory allocation for rows, headers, or types failed\n");
        free_query_result(result);
        mysql_free_result(res);
        mysql_close(conn);
        return NULL;
    }

    for (int i = 0; i < cols_count; i++) {
        result->headers[i] = strdup(fields[i].name);
        TypeMapping mapping = mysql_type_to_c_type(fields[i].type);
        result->mysql_types[i] = strdup(mapping.mysql_type);
        result->c_types[i] = strdup(mapping.c_type);
        if (!result->headers[i] || !result->mysql_types[i] || !result->c_types[i]) {
            fprintf(stderr, "strdup failed for header[%d], mysql_type[%d], or c_type[%d]\n", i, i, i);
            free_query_result(result);
            mysql_free_result(res);
            mysql_close(conn);
            return NULL;
        }
    }

    int row_index = 0;
    while ((row = mysql_fetch_row(res))) {
        for (int i = 0; i < cols_count; i++) {
            if (row[i]) {
                result->rows[row_index * cols_count + i] = strdup(row[i]);
            } else {
                result->rows[row_index * cols_count + i] = strdup("NULL");
            }
            if (!result->rows[row_index * cols_count + i]) {
                fprintf(stderr, "strdup failed for row[%d][%d]\n", row_index, i);
                free_query_result(result);
                mysql_free_result(res);
                mysql_close(conn);
                return NULL;
            }
        }
        row_index++;
    }

    mysql_free_result(res);
    mysql_close(conn);

    return result;
}

// Function to safely truncate and format cell data
char *safe_strncpy(char *dest, const char *src, size_t n) {
    if (strlen(src) > n) {
        strncpy(dest, src, n - 3); // Leave space for ellipsis
        strcat(dest, "...");
    } else {
        strncpy(dest, src, n);
        dest[n] = '\0';
    }
    return dest;
}

void print_query_result(QueryResult *result) {
    if (!result) {
        return;
    }

    ft_table_t *table = ft_create_table();
    //ft_set_border_style(table, FT_NICE_STYLE);
    ft_set_cell_prop(table, FT_ANY_ROW, FT_ANY_COLUMN, FT_CPROP_TEXT_ALIGN, FT_ALIGNED_LEFT);
    ft_set_cell_prop(table, 0, FT_ANY_COLUMN, FT_CPROP_ROW_TYPE, FT_ROW_HEADER);

    // Buffer size for cell data
    size_t bufferSize = 1;
    for (int i = 0; i < 3 && i < result->cols_count; i++) {
        bufferSize = (strlen(result->headers[i]) > bufferSize) ? strlen(result->headers[i]) : bufferSize;
    }
    if (result->cols_count > 4) {
        for (int i = result->cols_count - 4; i < result->cols_count; i++) {
            bufferSize = (strlen(result->headers[i]) > bufferSize) ? strlen(result->headers[i]) : bufferSize;
        }
    }

    bufferSize += 1;  // Extra space for null terminator
    char buffer[bufferSize];

    // Calculate number of hidden columns
    int hidden_columns = (result->cols_count > 7) ? result->cols_count - 7 : 0;
    char hidden_columns_header[50]; // Buffer for hidden columns header
    snprintf(hidden_columns_header, sizeof(hidden_columns_header), "<<+%d cols>>", hidden_columns);

    // Print headers
    for (int j = 0; j < 3 && j < result->cols_count; j++) {
        ft_u8write(table, result->headers[j]);
    }
    if (hidden_columns > 0) {
        ft_u8write(table, hidden_columns_header);
    }
    if (result->cols_count > 4) {
        for (int j = result->cols_count - 4; j < result->cols_count; j++) {
            ft_u8write(table, result->headers[j]);
        }
    }
    ft_ln(table);

    // Print rows
    int rows_to_show = 5;
    for (int i = 0; i < result->rows_count; i++) {
        if (result->rows_count > 10 && i == rows_to_show) {
            // Print a row of "..." if rows exceed 10
            for (int j = 0; j < 3 && j < result->cols_count; j++) {
                ft_u8write(table, "...");
            }
            if (hidden_columns > 0) {
                ft_u8write(table, "...");
            }
            if (result->cols_count > 4) {
                for (int j = result->cols_count - 4; j < result->cols_count; j++) {
                    ft_u8write(table, "...");
                }
            }
            ft_ln(table);
            i = result->rows_count - rows_to_show; // Skip directly to the last 5 rows
        }

        for (int j = 0; j < 3 && j < result->cols_count; j++) {
            safe_strncpy(buffer, result->rows[i * result->cols_count + j], bufferSize - 1);
            ft_u8write(table, buffer);
        }
        if (hidden_columns > 0) {
            ft_u8write(table, "...");
        }
        if (result->cols_count > 4) {
            for (int j = result->cols_count - 4; j < result->cols_count; j++) {
                safe_strncpy(buffer, result->rows[i * result->cols_count + j], bufferSize - 1);
                ft_u8write(table, buffer);
            }
        }
        ft_ln(table);
    }

    // Print the table
    printf("%s\n", (const char *)ft_to_u8string(table));
    ft_destroy_table(table);

    // Print additional information
    printf("Total number of rows: %d\n", result->rows_count);
    // Calculate and print the size of the object in memory in GB
    size_t size = sizeof(QueryResult);
    size += result->rows_count * result->cols_count * sizeof(char *);
    size += result->cols_count * sizeof(char *) * 3; // headers, mysql_types, c_types
    for (int i = 0; i < result->rows_count * result->cols_count; i++) {
         size += strlen(result->rows[i]) + 1;
    }
    for (int i = 0; i < result->cols_count; i++) {
        size += strlen(result->headers[i]) + 1;
        size += strlen(result->mysql_types[i]) + 1;
        size += strlen(result->c_types[i]) + 1;
    }
    double size_in_gb = (double)size / (1024 * 1024 * 1024);
    printf("Size in memory: %.7f GB\n", size_in_gb);

    printf("\nColumn names and data types:\n");
    for (int i = 0; i < result->cols_count; i++) {
        printf("%s (%s => %s)\n", result->headers[i], result->mysql_types[i], result->c_types[i]);
    }

}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <preset_name> <query>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *preset_name = argv[1];
    const char *query = argv[2];
    const char *config_path = "/home/rgw/Documents/rgwml.config";

    char *config_content = read_file(config_path);
    if (!config_content) {
        fprintf(stderr, "Failed to read config file\n");
        return EXIT_FAILURE;
    }

    cJSON *config_json = cJSON_Parse(config_content);
    if (!config_json) {
        fprintf(stderr, "Could not parse JSON\n");
        free(config_content);
        return EXIT_FAILURE;
    }

    cJSON *preset = get_db_preset(config_json, preset_name);
    if (!preset) {
        fprintf(stderr, "Preset not found: %s\n", preset_name);
        cJSON_Delete(config_json);
        free(config_content);
        return EXIT_FAILURE;
    }

    const char *host = cJSON_GetObjectItem(preset, "host")->valuestring;
    const char *user = cJSON_GetObjectItem(preset, "username")->valuestring;
    const char *password = cJSON_GetObjectItem(preset, "password")->valuestring;
    const char *database = cJSON_GetObjectItem(preset, "database")->valuestring;

    QueryResult *result = execute_mysql_query(host, user, password, database, query);
    if (result) {
        print_query_result(result);
        free_query_result(result);
    } else {
        fprintf(stderr, "Query execution failed.\n");
    }

    cJSON_Delete(config_json);
    free(config_content);

    return EXIT_SUCCESS;
}

