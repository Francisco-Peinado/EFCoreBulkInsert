using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

public static class DbContextExtensions
{
    public static async Task BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities, int timeout = 300) where T : class
    {
        var connection = context.Database.GetDbConnection();
        var transaction = context.Database.CurrentTransaction?.GetDbTransaction();

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync();
        }

        var entityType = context.Model.FindEntityType(typeof(T));
        var tableName = entityType.GetTableName();
        var columns = entityType.GetProperties().Select(p => p.GetColumnName()).ToArray();

        using (var bulkCopy = new SqlBulkCopy((SqlConnection)connection, SqlBulkCopyOptions.Default, (SqlTransaction)transaction))
        {
            bulkCopy.DestinationTableName = tableName;
            bulkCopy.BulkCopyTimeout = timeout;

            var dataTable = new DataTable();
            foreach (var column in columns)
            {
                dataTable.Columns.Add(column);
            }

            foreach (var entity in entities)
            {
                var values = columns.Select(c => entityType.FindProperty(c).GetGetter().GetClrValue(entity)).ToArray();
                dataTable.Rows.Add(values);
            }

            await bulkCopy.WriteToServerAsync(dataTable);
        }

        await InsertNavigationPropertiesAsync(context, entities, entityType, timeout);
    }

    private static async Task InsertNavigationPropertiesAsync<T>(DbContext context, IEnumerable<T> entities, IEntityType entityType, int timeout) where T : class
    {
        var navigationProperties = entityType.GetNavigations().ToList();
        foreach (var navigation in navigationProperties)
        {
            var navigationEntities = entities
                .SelectMany(e => (IEnumerable<object>)navigation.GetGetter().GetClrValue(e))
                .ToList();

            if (navigationEntities.Any())
            {
                var targetEntityType = navigation.TargetEntityType;
                var targetTableName = targetEntityType.GetTableName();
                var targetColumns = targetEntityType.GetProperties().Select(p => p.GetColumnName()).ToArray();

                using (var bulkCopy = new SqlBulkCopy((SqlConnection)context.Database.GetDbConnection(), SqlBulkCopyOptions.Default, (SqlTransaction)context.Database.CurrentTransaction?.GetDbTransaction()))
                {
                    bulkCopy.DestinationTableName = targetTableName;
                    bulkCopy.BulkCopyTimeout = timeout;

                    var dataTable = new DataTable();
                    foreach (var column in targetColumns)
                    {
                        dataTable.Columns.Add(column);
                    }

                    foreach (var entity in navigationEntities)
                    {
                        var values = targetColumns.Select(c => targetEntityType.FindProperty(c).GetGetter().GetClrValue(entity)).ToArray();
                        dataTable.Rows.Add(values);
                    }

                    await bulkCopy.WriteToServerAsync(dataTable);
                }

                await InsertNavigationPropertiesAsync(context, navigationEntities, targetEntityType, timeout);
            }
        }
    }
}
