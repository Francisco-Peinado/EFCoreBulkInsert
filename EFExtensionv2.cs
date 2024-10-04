using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

public static class DbContextExtensions
{
    public static async Task BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities) where T : class
    {
        var connection = context.Database.GetDbConnection();
        await connection.OpenAsync();

        using (var transaction = connection.BeginTransaction())
        {
            try
            {
                await BulkInsertEntitiesAsync(context, connection, transaction, entities);
                await context.SaveChangesAsync();
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
            finally
            {
                await connection.CloseAsync();
            }
        }
    }

    private static async Task BulkInsertEntitiesAsync<T>(DbContext context, DbConnection connection, DbTransaction transaction, IEnumerable<T> entities) where T : class
    {
        var table = new DataTable();
        var properties = typeof(T).GetProperties();

        foreach (var property in properties)
        {
            var columnType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
            if (columnType == typeof(Guid))
            {
                table.Columns.Add(property.Name, typeof(Guid));
            }
            else if (columnType == typeof(string))
            {
                table.Columns.Add(property.Name, typeof(string));
            }
            else
            {
                table.Columns.Add(property.Name, columnType);
            }
        }

        foreach (var entity in entities)
        {
            var values = new object[properties.Length];
            for (var i = 0; i < properties.Length; i++)
            {
                values[i] = properties[i].GetValue(entity);
            }
            table.Rows.Add(values);
        }

        using (var bulkCopy = new SqlBulkCopy((SqlConnection)connection, SqlBulkCopyOptions.Default, (SqlTransaction)transaction))
        {
            bulkCopy.DestinationTableName = context.Model.FindEntityType(typeof(T)).GetTableName();
            bulkCopy.EnableStreaming = true;
            await bulkCopy.WriteToServerAsync(table);
        }

        foreach (var entity in entities)
        {
            var navigationProperties = context.Entry(entity).Navigations;
            foreach (var navigation in navigationProperties)
            {
                if (navigation.CurrentValue is IEnumerable<object> navigationEntities)
                {
                    await BulkInsertEntitiesAsync(context, connection, transaction, navigationEntities);
                }
                else if (navigation.CurrentValue != null)
                {
                    await BulkInsertEntitiesAsync(context, connection, transaction, new[] { navigation.CurrentValue });
                }
            }
        }
    }
}