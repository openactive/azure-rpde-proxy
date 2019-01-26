IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[UPDATE_ITEM]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
            BEGIN
               PRINT '    Dropping stored procedure'
                        DROP PROCEDURE [dbo].[UPDATE_ITEM]
END

GO

IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[UPDATE_ITEM_BATCH]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
            BEGIN
               PRINT '    Dropping stored procedure'
                        DROP PROCEDURE [dbo].[UPDATE_ITEM_BATCH]
END

GO


IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[READ_ITEM_PAGE]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
            BEGIN
               PRINT '    Dropping stored procedure'
                        DROP PROCEDURE [dbo].[READ_ITEM_PAGE]
END


GO

CREATE PROCEDURE [dbo].[UPDATE_ITEM]
(
	@source varchar (50),
    @id nvarchar(50),
    @modified bigint,
	@kind varchar(50),
	@deleted bit,
	@data varchar(max)
)
AS
BEGIN
	IF EXISTS (SELECT 1 FROM [dbo].[items] t WHERE t.[source] = @source AND t.[id] = @id)
		UPDATE [dbo].[items] SET
			[modified] = @modified,
			[kind] = @kind,
			[deleted] = @deleted,
			[data] = @data
			WHERE [source] = @source AND [id] = @id AND @modified >= [modified]
	ELSE
		INSERT INTO [dbo].[items] ([source], [id], [modified], [kind], [deleted], [data]) 
		VALUES (@source, @id, @modified, @kind, @deleted, @data);
END

GO

IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[DELETE_SOURCE]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
            BEGIN
               PRINT '    Dropping stored procedure'
                        DROP PROCEDURE [dbo].[DELETE_SOURCE]
END

GO

CREATE PROCEDURE [dbo].[DELETE_SOURCE]
(
	@source varchar (200)
)
AS
BEGIN
	BEGIN TRAN
	EXEC sp_getapplock @Resource='BatchUpdateNonParallelLock', @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout = 60000
	DELETE TOP (1000) FROM [dbo].[items] WHERE [source] = @source;
	COMMIT
	RETURN @@rowcount;
END

GO

IF OBJECT_ID('[dbo].[items]') IS NOT NULL DROP TABLE [dbo].[items]
CREATE TABLE [dbo].[items]
(
	[source] varchar (50) NOT NULL,
    [id] nvarchar(50) NOT NULL,
    [modified] bigint NOT NULL,
	[kind] varchar(50) NOT NULL,
	[deleted] bit NOT NULL,
	[data] varchar(max) NULL,
	PRIMARY KEY NONCLUSTERED ([source], [id])
)

CREATE NONCLUSTERED INDEX IX_items_modified ON [dbo].[items]([source] ASC, [modified] ASC, [id] ASC) INCLUDE
    ([kind]
    ,[deleted]
	,[data]
	);

GO

IF type_id('[dbo].[ItemTableType]') IS NOT NULL
        DROP TYPE [dbo].[ItemTableType];
	
GO

CREATE TYPE ItemTableType AS TABLE 
(   [source] varchar (50),
    [id] nvarchar(50),
    [modified] bigint,
	[kind] varchar(50),
	[deleted] bit,
	[data] varchar(max) );

GO

CREATE PROCEDURE [dbo].[UPDATE_ITEM_BATCH]
(
	@Tvp as ItemTableType READONLY
)
AS
BEGIN
	BEGIN TRAN
	--EXEC sp_getapplock @Resource='BatchUpdateNonParallelLock', @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout = 60000
	MERGE INTO [dbo].[items] AS t
	USING (SELECT [source], [id], [modified], [kind], [deleted], [data] FROM @Tvp)
		AS s ([source], [id], [modified], [kind], [deleted], [data])
		ON t.[source] = s.[source] AND t.[id] = s.[id]
	WHEN MATCHED AND s.[modified] >= t.[modified] THEN  
		UPDATE SET
			[modified] = s.[modified],
			[kind] = s.[kind],
			[deleted] = s.[deleted],
			[data] = s.[data]
	WHEN NOT MATCHED THEN
		INSERT ([source], [id], [modified], [kind], [deleted], [data]) 
		VALUES (s.[source], s.[id], s.[modified], s.[kind], s.[deleted], s.[data]);
	COMMIT
END

GO

CREATE PROCEDURE [dbo].[READ_ITEM_PAGE]
(
	@source varchar (50),
    @id nvarchar(50) = NULL,
    @modified bigint = NULL
)
AS
BEGIN
   SELECT TOP 500 [data], [modified], [id] from [dbo].[items] u WHERE source = @source AND ((modified = @modified AND id >= @id) OR (modified > @modified)) ORDER BY modified, id;
END
