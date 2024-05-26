namespace TableHunt.Models;

public class DataQuery
{
    /// <summary>
    /// The query to run
    /// </summary>
    public string Query { get; set; }
    
    /// <summary>
    /// Name - is also used for the table name
    /// </summary>
    public string Name { get; set; }
    
    /// <summary>
    /// The index, for uniqueness
    /// </summary>
    public string Index { get; set; }
}