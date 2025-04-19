namespace codecrafters_sqlite;

public static class DbInfo
{
    public static void PrintDbInfo(string dbPath)
    {
        using var databaseFile = File.OpenRead(dbPath);
        Console.WriteLine("Logs from your program will appear here!");

        // Uncomment this line to pass the first stage
        databaseFile.Seek(16, SeekOrigin.Begin); // Skip the first 16 bytes
        byte[] pageSizeBytes = new byte[2];
        Helpers.ReadExactly(databaseFile, pageSizeBytes, 0, 2);
        var pageSize = Helpers.ReadUInt16BigEndian(pageSizeBytes);
        Console.WriteLine($"database page size: {pageSize}");

        databaseFile.Seek(103, SeekOrigin.Begin);
        byte[] numberOfTables = new byte[2];
        Helpers.ReadExactly(databaseFile, numberOfTables, 0, 2);
        Console.WriteLine($"number of tables: {Helpers.ReadUInt16BigEndian(numberOfTables)}");
    }
}