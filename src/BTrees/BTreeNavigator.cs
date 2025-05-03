using System.Text;
using codecrafters_sqlite.DbObjects;

namespace codecrafters_sqlite.Btrees;

public class BTreeNavigator
{
    private List<List<object>> _results = new List<List<object>>();
    public uint PageNumber { get; set; }
    public long DbPageSize { get; set; }
    public Encoding DbEncoding { get; set; }
    public Table Table { get; set; }
    

    public BTreeNavigator(uint pageNumber, long dbPageSize, Encoding encoding, Table table)
    {
        PageNumber = pageNumber;
        DbPageSize = dbPageSize;
        DbEncoding = encoding;
        Table = table;
    }

    public List<List<object>> ReadData(Stream dbStream, string[] fieldNames, Dictionary<string, string> filters)
    {
        _results.Clear();
        TraverseAndRead(PageNumber, dbStream, fieldNames, filters);
        return SelectFields(fieldNames);        
    }

    private void TraverseAndRead(uint currentPageNumber, Stream dbStream, string[] fieldNames, Dictionary<string, string> filters)
    {
        var stackPages = new Stack<uint>();
        var filtersByIndexColumn = filters.Select(f => new { ColumnName = f.Key, ColumnValue = f.Value })
            .ToDictionary(f => Table.GetColumnIndex(f.ColumnName), f => f.ColumnValue);
        stackPages.Push(currentPageNumber);
        while (stackPages.Count > 0)
        {
            var currentPage = stackPages.Pop();
            var pageOffset = currentPage == 1 ? 100 : (currentPage - 1) * DbPageSize;
            if (pageOffset < 0 || pageOffset >= dbStream.Length)
            {
                Console.Error.WriteLine($"Error: Offset de página inválido: {pageOffset} en la página {currentPageNumber}");
                return;
            }
            var btreePage =
                BTreePageFactory.CreatePage(dbStream, pageOffset, DbEncoding, DbPageSize, currentPage, Table);
            switch (btreePage)
            {
                case BTreeTableLeafPage tableLeafPage:
                    var leafData = tableLeafPage.ReadCells(dbStream, filtersByIndexColumn);
                    _results.AddRange(leafData);
                    break;
                case BTreeTableInteriorPage tableInteriorPage:
                    var childPageNumbers = tableInteriorPage.GetAllChildPageNumbers(dbStream);
                    for (int i = childPageNumbers.Count - 1; i >= 0; i--)
                    {
                        stackPages.Push(childPageNumbers[i]);
                    }
                    break;
            }
        }
    }

    private List<List<object>> SelectFields(string[] fieldNames)
    {
        if (fieldNames == null || fieldNames.Length == 0 || fieldNames.Contains("*"))
        {
            return _results;
        }
        var selectedFieldValues = new List<List<object>>();
        List<int> fieldIndexes = new List<int>();
        try
        {
            fieldIndexes = fieldNames.Select(fieldName => Table.GetColumnIndex(fieldName)).ToList();
        }
        catch (ArgumentException e)
        {
            Console.Error.WriteLine($"Error seleccionando campos: {e.Message}");
            return new List<List<object>>();
        }

        foreach (var row in _results)
        {   
            if(row.Count == 0) continue;
            if (fieldIndexes.Any(fieldIndex => (fieldIndex < 0 || fieldIndex >= row.Count)))
            {
                Console.Error.WriteLine($"Advertencia: La fila no contiene todos los índices de campo solicitados. Fila: {string.Join(",", row)}, Índices: {string.Join(",", fieldIndexes)}");
                continue;
            }
            
            var selectedRow = new List<object>();
            fieldIndexes.ForEach(fieldIndex =>
            {
                var fieldValue = row[fieldIndex];
                selectedRow.Add(fieldValue);
            });
            selectedFieldValues.Add(selectedRow);
        }
        return selectedFieldValues;
    }


    
    public int GetNumberOfRows(Stream dbStream)
    {
        var pageOffset = (long)(PageNumber - 1) * DbPageSize;
        var btreePage = BTreePageFactory.CreatePage(dbStream, pageOffset, DbEncoding, DbPageSize, PageNumber, Table);
        return btreePage.Header.CellCount;
    }


    private List<List<object>> GetFieldValues(BTreeTableLeafPage btree, Stream dbStream, string[] fieldNames,  Dictionary<string, string> filters)
    {
        var selectedFieldValues = new List<List<object>>();
        var filtersByIndexColumn = filters.Select(f => new { ColumnName = f.Key, ColumnValue = f.Value })
            .ToDictionary(f => Table.GetColumnIndex(f.ColumnName), f => f.ColumnValue);
        
        var rows = btree.ReadCells(dbStream, filtersByIndexColumn);
        foreach (var row in rows)
        {
            if (row.Count == 0) continue;
            var fieldIndexes = fieldNames.Select(fieldName => Table.GetColumnIndex(fieldName)).ToList();
            if (fieldIndexes.Any(fieldIndex => (fieldIndex < 0 || fieldIndex >= row.Count))) continue;
            var selectedRow = new List<object>();
            fieldIndexes.ForEach(fieldIndex =>
            {
                var fieldValue = row[fieldIndex];
                selectedRow.Add(fieldValue);
            });
            selectedFieldValues.Add(selectedRow);
        }
        return selectedFieldValues;
    }
}