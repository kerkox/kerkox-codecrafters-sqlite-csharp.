using System.Text;
using codecrafters_sqlite.Utils;

namespace codecrafters_sqlite.Btrees;

public class BTreeTableLeafPage : BTreePage
{
    public const byte Type = (byte)PageTypes.TableLeaf;

    public BTreeTableLeafPage(BTreePageHeader header) : base(header)
    {
    }
    
    public List<List<object>> ReadCells(Stream dbStream, Dictionary<int, string> filters)
    {
        if (Header.PageType != (byte)PageTypes.TableLeaf)
        {
            Console.WriteLine(
                $"Advertencia: Se intentó leer celdas de esquema en una página no válida: {Header.PageType:X2} en offset {Header.PageOffset}");
            return new List<List<object>>();
        }
        long cellPointerArrayStart = Header.PageOffset + Header.BTreePageHeaderSize;
        byte[] cellPointerBytes = new byte[2];
        
        
        // long nextCellPointerOffset = 0;
        List<List<object>> results = new List<List<object>>();
        for (var i = 0; i < Header.CellCount; i++)
        {
            // 1. Calcular la posición del puntero actual
            long pointerLocation = cellPointerArrayStart + (i * 2);
            
            // 2. Leer el valor del puntero (offset relativo de la celda)
            dbStream.Seek(pointerLocation, SeekOrigin.Begin);
            dbStream.ReadExactly(cellPointerBytes, 0, 2);
            ushort relativeCellOffset = Helpers.ReadUInt16BigEndian(cellPointerBytes);
            
            // 3. Calcular el offset absoluto de la celda en el archivo
            //    ¡ESTE ES EL OFFSET DONDE COMIENZA LA CELDA!
            long absoluteCellOffset = Header.PageOffset + relativeCellOffset;
            
            // 4. Ir al inicio de la celda
            dbStream.Seek(absoluteCellOffset, SeekOrigin.Begin);
            var rowIdValue = -1;
            try
            {
                var (payloadSize, payloadBytes) = dbStream.ReadVarint();
                var (rowId, rowIdBytes) = dbStream.ReadVarint();
                rowIdValue = (int)rowId;
                
                var data = dbStream.ReadRecord();
                data[0] = rowIdValue;

                // Here we can filter the data based on the filters
                if (filters != null && filters.Count > 0)
                {
                    var addColumn = true;
                    foreach (var filter in filters)
                    {
                        var columnIndex = filter.Key;
                        var columnValue = filter.Value;
                        
                        if (columnIndex < 0 || columnIndex >= data.Count)
                        {
                            addColumn = false;
                            break;
                        }

                        string dataValue = data[columnIndex].ToString() ?? string.Empty;

                        if (!dataValue.Contains(columnValue))
                        {
                            addColumn = false;
                            break;
                        }
                    }
                    if (!addColumn)
                    {
                        continue;
                    }
                }
                results.Add(data);
                
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer la celda {i}: {ex.Message}");
            }
        }
        
        return results;
    }
}