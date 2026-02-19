"""
Authentication Manager for iPhone Flipper

This module handles different authentication methods for AI APIs:
1. Google Gemini API Key (AIza...)
2. OpenAI API Key (sk-...)
3. OAuth Access Token (Bearer token from workspace)

It provides utilities to save, load, and refresh these credentials.
"""

import json
import os
from pathlib import Path
from datetime import datetime

AUTH_CONFIG_PATH = Path(__file__).parent / "auth_config.json"
AUTH_INPUT_PATH = Path(__file__).parent / "auth.json"

def detect_api_provider(api_key: str) -> str:
    """Detect which AI provider the API key belongs to."""
    if api_key.startswith("AIza"):
        return "gemini"
    elif api_key.startswith("sk-"):
        return "openai"
    else:
        return "unknown"

def save_auth_config(config_data):
    """Save authentication configuration to a JSON file."""
    # If the input is a string (pasted JSON), try to parse it
    if isinstance(config_data, str):
        try:
            config_data = json.loads(config_data)
        except json.JSONDecodeError:
            return {"success": False, "error": "Invalid JSON format"}

    # Standardize the format
    standard_config = {
        "auth_type": "api_key",
        "provider": "gemini",  # Default to Gemini
        "api_key": None,
        "oauth_token": None,
        "refresh_token": None,
        "expires_at": None,
        "updated_at": datetime.now().isoformat()
    }

    # Detect if it's the specific OAuth JSON provided by the user
    if "profiles" in config_data:
        # It's the user's OAuth JSON format (OpenAI)
        profile_key = list(config_data["profiles"].keys())[0]
        profile = config_data["profiles"][profile_key]
        standard_config["auth_type"] = "oauth"
        standard_config["provider"] = "openai"
        standard_config["oauth_token"] = profile.get("access")
        standard_config["refresh_token"] = profile.get("refresh")
        standard_config["expires_at"] = profile.get("expires")
    elif "access" in config_data and "refresh" in config_data:
        # Simplified OAuth format
        standard_config["auth_type"] = "oauth"
        standard_config["provider"] = "openai"
        standard_config["oauth_token"] = config_data["access"]
        standard_config["refresh_token"] = config_data.get("refresh")
        standard_config["expires_at"] = config_data.get("expires")
    elif "api_key" in config_data:
        standard_config["auth_type"] = "api_key"
        standard_config["api_key"] = config_data["api_key"]
        standard_config["provider"] = detect_api_provider(config_data["api_key"])
    elif isinstance(config_data, dict) and len(config_data) == 0:
        return {"success": False, "error": "Empty configuration"}
    else:
        # Assume it might be a raw API key if it's a simple dict with one string
        first_val = list(config_data.values())[0] if config_data else ""
        if isinstance(first_val, str) and (first_val.startswith("sk-") or first_val.startswith("AIza")):
            standard_config["auth_type"] = "api_key"
            standard_config["api_key"] = first_val
            standard_config["provider"] = detect_api_provider(first_val)
        else:
            return {"success": False, "error": "Unrecognized configuration format"}

    with open(AUTH_CONFIG_PATH, "w") as f:
        json.dump(standard_config, f, indent=4)
    
    return {"success": True, "config": standard_config}

def load_auth_config():
    """Load the current authentication configuration."""
    # Auto-import raw auth.json once, then remove it to avoid leaving secrets around
    if AUTH_INPUT_PATH.exists() and not AUTH_CONFIG_PATH.exists():
        try:
            with open(AUTH_INPUT_PATH, "r") as f:
                raw_data = json.load(f)
                # Auto-import and save it to the internal config format
                result = save_auth_config(raw_data)
                if result.get("success"):
                    try:
                        os.remove(AUTH_INPUT_PATH)
                    except OSError:
                        pass
                    return result["config"]
        except Exception as e:
            print(f"⚠️ Warning: Found auth.json but failed to parse it: {e}")

    if not AUTH_CONFIG_PATH.exists():
        # Check environment variables as fallback
        gemini_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        openai_key = os.getenv("OPENAI_API_KEY")
        
        if gemini_key:
            return {
                "auth_type": "api_key",
                "provider": "gemini",
                "api_key": gemini_key,
                "oauth_token": None
            }
        elif openai_key:
            return {
                "auth_type": "api_key",
                "provider": "openai",
                "api_key": openai_key,
                "oauth_token": None
            }
        return None
    
    with open(AUTH_CONFIG_PATH, "r") as f:
        return json.load(f)

def get_auth_headers():
    """Get the authorization headers for API requests."""
    config = load_auth_config()
    if not config:
        return None
    
    if config["auth_type"] == "api_key" and config["api_key"]:
        return {"Authorization": f"Bearer {config['api_key']}"}
    elif config["auth_type"] == "oauth" and config["oauth_token"]:
        return {"Authorization": f"Bearer {config['oauth_token']}"}
    
    return None

def is_token_expired():
    """Check if the current OAuth token is expired."""
    config = load_auth_config()
    if not config or config["auth_type"] != "oauth" or not config["expires_at"]:
        return False
    
    # expires_at is usually in milliseconds for JS-based OAuth
    expires_ts = config["expires_at"] / 1000 if config["expires_at"] > 1e11 else config["expires_at"]
    return datetime.now().timestamp() > expires_ts

def get_api_client_params():
    """Return parameters needed to initialize the AI client."""
    config = load_auth_config()
    if not config:
        return {}
    
    params = {"provider": config.get("provider", "gemini")}
    
    if config["auth_type"] == "api_key":
        params["api_key"] = config["api_key"]
    elif config["auth_type"] == "oauth":
        # For OAuth, we pass the access token as the API key
        params["api_key"] = config["oauth_token"]
    
    return params

if __name__ == "__main__":
    # Test utility
    print("Auth Manager Test")
    config = load_auth_config()
    if config:
        print(f"Current Provider: {config.get('provider', 'unknown')}")
        print(f"Current Auth Type: {config['auth_type']}")
        if config['auth_type'] == 'oauth':
            print(f"Token Expired: {is_token_expired()}")
    else:
        print("No auth config found.")
