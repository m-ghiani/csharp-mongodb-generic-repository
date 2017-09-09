namespace GH.MongoDb.GenericRepository.Interfaces
{
    public interface IDocument<T>
    {
        T Id { get; set; }
    }
}