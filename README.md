#EfCore extension
##usage
using (var context = new YourDbContext())
{
    var entities = new List<YourEntity> { /* ... */ };
    int rowsCopied = await context.BulkInsertAsync(entities);
    Console.WriteLine($"Inserted {rowsCopied} rows");
}
