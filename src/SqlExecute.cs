namespace codecrafters_sqlite;

public class SqlExecute
{
    private readonly string _dbPath;

    public SqlExecute(string dbPath)
    {
        _dbPath = dbPath;
    }

    public void ExecuteSql(string sql)
    {
        // Console.WriteLine("Logs from your program will appear here!");

        // Parse SQL command and act accordingly
        if (sql.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
        {
            // Handle SELECT command
            SelectCommand(sql);
            // Implement SELECT logic here
        }
        else if (sql.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase))
        {
            // Handle INSERT command
            Console.WriteLine("Executing INSERT command...");
            // Implement INSERT logic here
        }
        else if (sql.StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase))
        {
            // Handle UPDATE command
            Console.WriteLine("Executing UPDATE command...");
            // Implement UPDATE logic here
        }
        else if (sql.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase))
        {
            // Handle DELETE command
            Console.WriteLine("Executing DELETE command...");
            // Implement DELETE logic here
        }
        else
        {
            Console.WriteLine($"Unsupported SQL command: {sql}");
        }
    }

    private void SelectCommand(string sql)
    {
        using var databaseFile = File.OpenRead(_dbPath);
        // Implement SELECT command logic here
        // Console.WriteLine($"Executing SELECT command: {sql}");
        var parts = sql.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var tableName = parts.Last();
        var columns = parts.Skip(1).Take(parts.Length - 2).ToArray();
        if (columns.Length <= 0 ||
            !columns.Any(c => c.Contains("COUNT", StringComparison.InvariantCultureIgnoreCase))) return;
        using var database = new Database(_dbPath);
        var numCells = database.GetNumberOfRowsFromTable(tableName);
        // page3.ReadCells();
        // Console.WriteLine($"Number of rows in table {tableName}: {page.NumCells}");
        Console.WriteLine($"{numCells}");
    }
}