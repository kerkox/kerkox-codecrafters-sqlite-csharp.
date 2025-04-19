using codecrafters_sqlite;

// Parse arguments
var (path, command) = args.Length switch
{
    0 => throw new InvalidOperationException("Missing <database path> and <command>"),
    1 => throw new InvalidOperationException("Missing <command>"),
    _ => (args[0], args[1])
};

using var databaseFile = File.OpenRead(path);

switch (command)
{
    // Parse command and act accordingly
    case ".dbinfo":
        DbInfo.PrintDbInfo(path);
        break;
    case ".tables":
        var tablesInfo = new TablesInfo(path);
        tablesInfo.PrintTables();
        break;
    default:
    {
        // trying to run a SQL Query
        var sqlExecute = new SqlExecute(path);
        sqlExecute.ExecuteSql(command);
        break;
    }
}