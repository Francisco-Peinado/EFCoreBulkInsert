using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

public static class EfCoreSqlBulkCopyExtensions
{
    public static async Task<int> BulkInsertAsync<T>(
        this DbContext context,
        IEnumerable<T> entities,
        int batchSize = 1000,
        int timeout = 300,
        bool enableStreaming = false)
        where T : class
    {
        var entityType = context.Model.FindEntityType(typeof(T));
        var tableName = entityType.GetTableName();
        var schema = entityType.GetSchema();
        var fullTableName = string.IsNullOrEmpty(schema) ? tableName : $"{schema}.{tableName}";

        var properties = entityType.GetProperties()
            .Where(p => !p.IsPrimaryKey() || !p.ValueGenerated.HasFlag(ValueGenerated.OnAdd))
            .ToList();

        var dataTable = new DataTable();
        foreach (var property in properties)
        {
            dataTable.Columns.Add(property.GetColumnName(), 
                Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType);
        }

        foreach (var entity in entities)
        {
            var row = dataTable.NewRow();
            foreach (var property in properties)
            {
                row[property.GetColumnName()] = property.GetGetter().GetClrValue(entity) ?? DBNull.Value;
            }
            dataTable.Rows.Add(row);
        }

        using var transaction = await context.Database.BeginTransactionAsync();
        try
        {
            using var sqlBulkCopy = new SqlBulkCopy(
                (SqlConnection)context.Database.GetDbConnection(),
                SqlBulkCopyOptions.Default,
                (SqlTransaction)transaction.GetDbTransaction());

            sqlBulkCopy.DestinationTableName = fullTableName;
            sqlBulkCopy.BatchSize = batchSize;
            sqlBulkCopy.BulkCopyTimeout = timeout;
            sqlBulkCopy.EnableStreaming = enableStreaming;

            foreach (var property in properties)
            {
                sqlBulkCopy.ColumnMappings.Add(property.GetColumnName(), property.GetColumnName());
            }

            await sqlBulkCopy.WriteToServerAsync(dataTable);
            await transaction.CommitAsync();

            return dataTable.Rows.Count;
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }
}
