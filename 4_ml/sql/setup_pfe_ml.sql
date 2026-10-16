-- Run in SSMS as dbo (or a user with CREATE SCHEMA rights)
-- Grants airbyte_user write access to the ML predictions schema

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'PFE_ML')
    EXEC('CREATE SCHEMA PFE_ML');

IF OBJECT_ID(N'PFE_ML.ml_customer_predictions', N'U') IS NULL
BEGIN
    CREATE TABLE PFE_ML.ml_customer_predictions (
        prediction_id       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        customer_id         BIGINT NOT NULL,
        customer_sk         BIGINT NULL,
        risk_probability    FLOAT NOT NULL,
        risk_prediction     INT NOT NULL,
        rules_risk_tier     NVARCHAR(20) NULL,
        rules_global_score  FLOAT NULL,
        disagreement_flag   INT NOT NULL DEFAULT 0,
        is_pep              INT NULL,
        total_working_balance FLOAT NULL,
        dao_area            NVARCHAR(100) NULL,
        model_name          NVARCHAR(100) NOT NULL,
        model_version       NVARCHAR(50) NOT NULL,
        prediction_threshold FLOAT NOT NULL,
        prediction_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        scoring_date        DATE NULL
    );
    CREATE INDEX IX_ml_predictions_customer ON PFE_ML.ml_customer_predictions(customer_id);
    CREATE INDEX IX_ml_predictions_disagreement ON PFE_ML.ml_customer_predictions(disagreement_flag);
END;

GRANT SELECT, INSERT, DELETE, UPDATE, ALTER ON SCHEMA::PFE_ML TO airbyte_user;
