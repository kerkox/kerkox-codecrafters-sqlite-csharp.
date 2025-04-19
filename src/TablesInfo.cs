using System.Text;

namespace codecrafters_sqlite;

public class TablesInfo
{

    private readonly string _dbPath;
    
    public TablesInfo(string dbPath)
    {
        _dbPath = dbPath;
    }

    public void PrintTables()
    {
        using var database = new Database(_dbPath);
        var page = database.Pages.Where(p => p.PageNumber == 1).ToList().FirstOrDefault();
        if (page == null)
        {
            Console.WriteLine("No pages found in the database.");
            return;
        }
        var tableNames = page.Tables
            .Where(t => !t.Name.StartsWith("sqlite_"))
            .Select(t => t.Name).ToList();
        tableNames.Reverse();
        Console.WriteLine(string.Join(" ", tableNames));
    }
}