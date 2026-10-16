-- ATB BI Dashboard — authentication & RBAC (run in SSMS as dbo)
-- Schema: PFE_AUTH

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'PFE_AUTH')
    EXEC('CREATE SCHEMA PFE_AUTH');

-- Roles (bank hierarchy — NOT retail customers)
IF OBJECT_ID(N'PFE_AUTH.dashboard_roles', N'U') IS NULL
CREATE TABLE PFE_AUTH.dashboard_roles (
    role_code       NVARCHAR(40)  NOT NULL PRIMARY KEY,
    role_label_fr   NVARCHAR(120) NOT NULL,
    role_label_en   NVARCHAR(120) NOT NULL,
    description_fr  NVARCHAR(400) NULL,
    dao_scoped      BIT NOT NULL DEFAULT 0,   -- 1 = chef agence, filtered by branch
    is_read_only    BIT NOT NULL DEFAULT 0
);

IF OBJECT_ID(N'PFE_AUTH.dashboard_role_pages', N'U') IS NULL
CREATE TABLE PFE_AUTH.dashboard_role_pages (
    role_code   NVARCHAR(40) NOT NULL,
    page_key    NVARCHAR(60) NOT NULL,
    PRIMARY KEY (role_code, page_key),
    FOREIGN KEY (role_code) REFERENCES PFE_AUTH.dashboard_roles(role_code)
);

IF OBJECT_ID(N'PFE_AUTH.dashboard_users', N'U') IS NULL
CREATE TABLE PFE_AUTH.dashboard_users (
    user_id         INT IDENTITY(1,1) PRIMARY KEY,
    username        NVARCHAR(80)  NOT NULL UNIQUE,
    password_hash   NVARCHAR(128) NOT NULL,
    display_name    NVARCHAR(120) NOT NULL,
    role_code       NVARCHAR(40)  NOT NULL,
    dao_area        NVARCHAR(100) NULL,       -- branch scope for CHEF_AGENCE
    is_active       BIT NOT NULL DEFAULT 1,
    created_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    FOREIGN KEY (role_code) REFERENCES PFE_AUTH.dashboard_roles(role_code)
);

-- Password = SHA256(salt + password). Salt: ATB_BI_2026
-- dg.central        -> ATB_DG_2026!
-- conformite.chief  -> ATB_CONF_2026!
-- agence.tunis      -> ATB_TUNIS_2026!
-- agence.sfax       -> ATB_SFAX_2026!
-- risque.analyst    -> ATB_RISK_2026!
-- audit.control     -> ATB_AUDIT_2026!

MERGE PFE_AUTH.dashboard_roles AS t
USING (VALUES
    (N'DG_CENTRAL',       N'Direction Générale',        N'General Management',        N'Accès complet — toutes agences et modules', 0, 0),
    (N'CHEF_CONFORMITE',  N'Chef Conformité',           N'Chief Compliance Officer',  N'Conformité, alertes, ML — vue nationale', 0, 0),
    (N'CHEF_AGENCE',      N'Chef d''Agence',            N'Branch Manager',            N'Vue limitée à son agence (DAO)', 1, 0),
    (N'ANALYSTE_RISQUE',  N'Analyste Risque',           N'Risk Analyst',              N'Risque, ML, études — pas d''explorateur brut', 0, 0),
    (N'AUDIT',            N'Audit Interne',             N'Internal Audit',            N'Lecture seule — conformité et rapports', 0, 1)
) AS s(role_code, role_label_fr, role_label_en, description_fr, dao_scoped, is_read_only)
ON t.role_code = s.role_code
WHEN NOT MATCHED THEN INSERT VALUES (s.role_code, s.role_label_fr, s.role_label_en, s.description_fr, s.dao_scoped, s.is_read_only);

DELETE FROM PFE_AUTH.dashboard_role_pages;

INSERT INTO PFE_AUTH.dashboard_role_pages (role_code, page_key) VALUES
-- DG: everything
(N'DG_CENTRAL', N'page_overview'), (N'DG_CENTRAL', N'page_studies'), (N'DG_CENTRAL', N'page_compliance'),
(N'DG_CENTRAL', N'page_alert_inbox'), (N'DG_CENTRAL', N'page_comparison'), (N'DG_CENTRAL', N'page_risk'),
(N'DG_CENTRAL', N'page_accounts'), (N'DG_CENTRAL', N'page_customers'), (N'DG_CENTRAL', N'page_dao'),
(N'DG_CENTRAL', N'page_calendar'), (N'DG_CENTRAL', N'page_sectors'), (N'DG_CENTRAL', N'page_industries'),
(N'DG_CENTRAL', N'page_targets'), (N'DG_CENTRAL', N'page_currencies'), (N'DG_CENTRAL', N'page_mlflow'),
(N'DG_CENTRAL', N'page_ml'), (N'DG_CENTRAL', N'page_ml_guide'), (N'DG_CENTRAL', N'page_explorer'),
(N'DG_CENTRAL', N'page_dg_console'),
-- Conformité
(N'CHEF_CONFORMITE', N'page_overview'), (N'CHEF_CONFORMITE', N'page_compliance'),
(N'CHEF_CONFORMITE', N'page_alert_inbox'), (N'CHEF_CONFORMITE', N'page_mlflow'),
(N'CHEF_CONFORMITE', N'page_ml'), (N'CHEF_CONFORMITE', N'page_studies'),
-- Chef agence (branch)
(N'CHEF_AGENCE', N'page_overview'), (N'CHEF_AGENCE', N'page_accounts'),
(N'CHEF_AGENCE', N'page_risk'), (N'CHEF_AGENCE', N'page_compliance'),
(N'CHEF_AGENCE', N'page_alert_inbox'), (N'CHEF_AGENCE', N'page_dao'),
-- Analyste risque
(N'ANALYSTE_RISQUE', N'page_overview'), (N'ANALYSTE_RISQUE', N'page_studies'),
(N'ANALYSTE_RISQUE', N'page_risk'), (N'ANALYSTE_RISQUE', N'page_comparison'),
(N'ANALYSTE_RISQUE', N'page_alert_inbox'), (N'ANALYSTE_RISQUE', N'page_mlflow'),
(N'ANALYSTE_RISQUE', N'page_ml'), (N'ANALYSTE_RISQUE', N'page_ml_guide'),
-- Audit (read-only subset)
(N'AUDIT', N'page_overview'), (N'AUDIT', N'page_compliance'),
(N'AUDIT', N'page_alert_inbox'), (N'AUDIT', N'page_studies');

-- Users: run Python seed for real password hashes (SHA256 with salt ATB_BI_2026)
--   python 5_reporting/streamlit/scripts/seed_auth.py

-- GRANT SELECT ON SCHEMA::PFE_AUTH TO airbyte_user;
