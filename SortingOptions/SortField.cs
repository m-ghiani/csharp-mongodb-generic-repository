using System.Data.SqlTypes;

namespace GH.MongoDb.GenericRepository.SortingOptions
{
    public class SortingField: INullable
    {
        public SortingField()
        {
            
        }

        public SortingField(string fieldName, SortingModes sotinMode = SortingModes.Ascending)
        {
            FieldName = fieldName;
            SortingMode = sotinMode;
        }
        public string FieldName { get; set; }
        public SortingModes SortingMode { get; set; } = SortingModes.Ascending;
        public bool IsNull => string.IsNullOrEmpty(FieldName);
    }
}