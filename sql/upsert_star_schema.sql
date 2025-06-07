CREATE OR ALTER PROC dbo.upsert_star_schema AS
BEGIN
  -- dim_state
  IF OBJECT_ID('dbo.dim_state') IS NULL
    CREATE TABLE dbo.dim_state(
      state_key   INT IDENTITY PRIMARY KEY,
      addr_state  NVARCHAR(2) UNIQUE
    );
  MERGE dbo.dim_state AS tgt
    USING (
      SELECT DISTINCT addr_state
      FROM dbo.stg_loans
      WHERE load_dt = CAST(GETDATE() AS DATE)
    ) AS src
    ON tgt.addr_state = src.addr_state
  WHEN NOT MATCHED THEN
    INSERT(addr_state) VALUES(src.addr_state);

  -- fact_loans
  IF OBJECT_ID('dbo.fact_loans') IS NULL
    CREATE TABLE dbo.fact_loans(
      loan_key    INT IDENTITY PRIMARY KEY,
      state_key   INT,
      loan_amnt   MONEY,
      int_rate    DECIMAL(5,2),
      issue_d     DATE,
      annual_inc  DECIMAL(18,2)
    );
  INSERT INTO dbo.fact_loans(state_key,loan_amnt,int_rate,issue_d,annual_inc)
  SELECT d.state_key, s.loan_amnt, s.int_rate, s.issue_d, s.annual_inc
    FROM dbo.stg_loans AS s
    JOIN dbo.dim_state AS d ON d.addr_state = s.addr_state
  WHERE 
s.load_dt = CAST(GETDATE() AS DATE);
END;
GO
