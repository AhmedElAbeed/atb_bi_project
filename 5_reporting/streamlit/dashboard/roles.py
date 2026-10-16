"""Role definitions and page permissions for ATB BI dashboard."""
from __future__ import annotations

# page_key must match keys in app.py NAV_GROUPS
ROLE_META: dict[str, dict] = {
    "DG_CENTRAL": {
        "label_fr": "Direction Générale",
        "label_en": "General Management",
        "dao_scoped": False,
        "read_only": False,
        "pages": "ALL",
    },
    "CHEF_CONFORMITE": {
        "label_fr": "Chef Conformité",
        "label_en": "Chief Compliance",
        "dao_scoped": False,
        "read_only": False,
        "pages": {
            "page_overview", "page_studies", "page_compliance", "page_alert_inbox",
            "page_mlflow", "page_ml", "page_ml_guide",
        },
    },
    "CHEF_AGENCE": {
        "label_fr": "Chef d'Agence",
        "label_en": "Branch Manager",
        "dao_scoped": True,
        "read_only": False,
        "pages": {
            "page_overview", "page_accounts", "page_risk", "page_compliance",
            "page_alert_inbox", "page_dao",
        },
    },
    "ANALYSTE_RISQUE": {
        "label_fr": "Analyste Risque",
        "label_en": "Risk Analyst",
        "dao_scoped": False,
        "read_only": False,
        "pages": {
            "page_overview", "page_studies", "page_risk", "page_comparison",
            "page_alert_inbox", "page_mlflow", "page_ml", "page_ml_guide",
        },
    },
    "AUDIT": {
        "label_fr": "Audit Interne",
        "label_en": "Internal Audit",
        "dao_scoped": False,
        "read_only": True,
        "pages": {
            "page_overview", "page_compliance", "page_alert_inbox", "page_studies",
        },
    },
}

# Demo credentials when PFE_AUTH tables are not yet created
DEMO_USERS: dict[str, dict] = {
    "dg.central": {
        "password": "ATB_DG_2026!",
        "display_name": "M. Karim Ben Salah",
        "role_code": "DG_CENTRAL",
        "dao_area": None,
    },
    "conformite.chief": {
        "password": "ATB_CONF_2026!",
        "display_name": "Mme. Leila Trabelsi",
        "role_code": "CHEF_CONFORMITE",
        "dao_area": None,
    },
    "agence.tunis": {
        "password": "ATB_TUNIS_2026!",
        "display_name": "M. Youssef Miled",
        "role_code": "CHEF_AGENCE",
        "dao_area": "TUNIS CENTRE",
    },
    "agence.sfax": {
        "password": "ATB_SFAX_2026!",
        "display_name": "Mme. Amira Gharbi",
        "role_code": "CHEF_AGENCE",
        "dao_area": "SFAX",
    },
    "risque.analyst": {
        "password": "ATB_RISK_2026!",
        "display_name": "M. Omar Jebali",
        "role_code": "ANALYSTE_RISQUE",
        "dao_area": None,
    },
    "audit.control": {
        "password": "ATB_AUDIT_2026!",
        "display_name": "Mme. Salma Ayari",
        "role_code": "AUDIT",
        "dao_area": None,
    },
}


def allowed_pages(role_code: str) -> set[str] | None:
    """Return allowed page keys, or None meaning ALL."""
    meta = ROLE_META.get(role_code, {})
    pages = meta.get("pages")
    if pages == "ALL":
        return None
    return set(pages or [])


def is_dao_scoped(role_code: str) -> bool:
    return bool(ROLE_META.get(role_code, {}).get("dao_scoped"))


def is_read_only(role_code: str) -> bool:
    return bool(ROLE_META.get(role_code, {}).get("read_only"))
