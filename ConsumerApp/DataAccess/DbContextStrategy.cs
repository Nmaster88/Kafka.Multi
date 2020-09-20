namespace ConsumerApp.DataAccess
{
    public class DbContextStrategy
    {
        public IDbContext _dbContext { get; private set; }

        private object _conn;

        public DbContextStrategy() { }

        public DbContextStrategy(IDbContext dbController)
        {
            _dbContext = dbController;
        }

        public void SetStrategy(IDbContext dbController)
        {
            _dbContext = dbController;
        }

        public void OpenConnection()
        {
            _dbContext.Initialize();
            _dbContext.OpenConn();
        }

        public void CloseConnection()
        {
            _dbContext.CloseConn();
        }

        public void CreateDatabaseIfNotExist()
        {
            _dbContext.Initialize();
            _dbContext.OpenConn();
            _dbContext.CreateDbIfNotExists();
            _dbContext.CloseConn();
        }
    }
}