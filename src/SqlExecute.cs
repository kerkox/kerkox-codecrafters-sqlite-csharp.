using System.Text.RegularExpressions;

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
        if (sql.Contains("COUNT(", StringComparison.InvariantCultureIgnoreCase))
        {
            CountCommand(sql);
            return;
        }
        var parts = sql.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        
        var tableName = GetTableNameFromSql(sql);
        var columns = GetColumnsFromSql(sql);
        // filters are taken with regular expression:
        var filters = sql.Split(new[] { "WHERE", "where" }, StringSplitOptions.RemoveEmptyEntries)
            .Skip(1)
            .SelectMany(f => f.Split(new[] { "AND", "OR" }, StringSplitOptions.RemoveEmptyEntries))
            .Select(f => new { Column = f.Trim().Split('=')[0].Trim(), Value = f.Trim().Split('=')[1].Trim().Replace("'", "") })
            .ToDictionary(k => k.Column, v => v.Value);
        using var database = new Database(_dbPath);
        var values = database.GetFieldValuesFromTableWithBTree(tableName, columns, filters);
        var rows = values.Select(v => string.Join("|", v)).ToList();
        Console.WriteLine($"{string.Join("\n", rows)}");
    }

    private string GetTableNameFromSql(string sql)
    {
        string tableName;
        const string expression = @"(?:FROM\s+([\w]+))";
        var regex = new Regex(expression, RegexOptions.IgnoreCase);
        var match = regex.Match(sql);
        if (match.Success)
        {
            tableName = match.Groups[1].Value;
        }
        else
        {
            throw new Exception("Table name not found in SQL query.");
        }

        return tableName;

    }

    private string[] GetColumnsFromSql(string sql)
    {
        const string expression = @"SELECT(?:\s+DISTINCT)?\s+(.*?)\s+FROM";
        var regex = new Regex(expression, RegexOptions.IgnoreCase);
        var match = regex.Match(sql);
        if (!match.Success) throw new Exception("Columns not found in SQL query.");
        
        var columns = match.Groups[1].Value;
        return columns.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(c => c.Trim()).ToArray();
    }
    


    private void CountCommand(string sql)
    {
        var parts = sql.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var tableName = parts.Last();
        var columns = parts.Skip(1).Take(parts.Length - 2).ToArray();
        if (columns.Length <= 0 ||
            !columns.Any(c => c.Contains("COUNT", StringComparison.InvariantCultureIgnoreCase))) return;
        using var database = new Database(_dbPath);
        var numCells = database.GetNumberOfRowsFromTable(tableName);
        Console.WriteLine($"{numCells}");
    }
}