"""Internal help for authzee configuration.
"""

__all__ = [
    "default_config",
    "override_config"
]

from authzee.types.config import AuthzeeConfig


default_config: AuthzeeConfig = {
    "authzee": {
        "raise_crits": True
    },
    "start": {
        "compute_start": {
            "storage": {}
        },
        "storage_start": {}
    },
    "shutdown": {
        "compute_shutdown": {
            "storage": {}
        },
        "storage_shutdown": {}
    },
    "construct": {
        "compute_construct": {},
        "storage_construct": {}
    },
    "destroy": {
        "compute_destroy": {},
        "storage_destroy": {}
    },
    "validate_context_def": {},
    "list_context_defs": {
        "page_size": 100,
        "use_cache": False
    },
    "get_context_def": {
        "use_cache": False
    },
    "put_context_def": {},
    "delete_context_def": {},
    "validate_identity_def": {},
    "list_identity_defs": {
        "page_size": 100,
        "use_cache": False
    },
    "get_identity_def": {
        "use_cache": False
    },
    "put_identity_def": {},
    "delete_identity_def": {},
    "validate_resource_def": {},
    "list_resource_defs": {
        "page_size": 100,
        "use_cache": False
    },
    "get_resource_def": {
        "use_cache": False
    },
    "put_resource_def": {},
    "delete_resource_def": {},
    "validate_grant": {},
    "list_grants": {
        "page_size": 100,
        "use_cache": False
    },
    "get_grant": {
        "use_cache": False
    },
    "enact": {},
    "repeal": {},
    "list_grant_refs": {
        "page_size": 10,
        "use_cache": False
    },
    "cleanup_latches": {},
    "validate_request": {
        "get_context_def": {
            "use_cache": True
        },
        "use_list_context_defs": True,
        "list_context_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "get_identity_def": {
            "use_cache": True
        },
        "use_list_identity_defs": True,
        "list_identity_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "get_resource_def": {
            "use_cache": True
        },
        "use_list_resource_defs": True,
        "list_resource_defs": {
            "page_size": 100,
            "use_cache": True
        }
    },
    "validate_batch_request": {
        "get_context_def": {
            "use_cache": True
        },
        "use_list_context_defs": True,
        "list_context_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "get_identity_def": {
            "use_cache": True
        },
        "use_list_identity_defs": True,
        "list_identity_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "get_resource_def": {
            "use_cache": True
        },
        "use_list_resource_defs": True,
        "list_resource_defs": {
            "page_size": 100,
            "use_cache": True
        }
    },
    "audit": {
        "validate_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": True
        }
    },
    "batch_audit": {
        "validate_batch_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": True
        }
    },
    "authorize": {
        "validate_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": True
        },
        "parallel_paging": True,
        "list_grant_refs": {
            "page_size": 10,
            "use_cache": True
        }
    },
    "batch_authorize": {
        "validate_batch_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "validate_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": True
        },
        "parallel_paging": True,
        "list_grant_refs": {
            "page_size": 10,
            "use_cache": True
        }
    }
}


def override_config(override: dict | None, default: dict) -> dict:
    if override is None:
        return default

    full = {}
    for key in default:
        if key in override:
            if type(default[key]) is dict:
                full[key] = override_config(override[key], default[key])
            else:
                full[key] = override[key]

        else:
            full[key] = default[key]

    return full
