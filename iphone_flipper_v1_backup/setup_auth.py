"""
Setup Utility for iPhone Flipper Authentication

This script helps you set up AI credentials, supporting:
- Pasting OAuth JSON from your workspace
- Entering a Gemini API key
- Entering an OpenAI API key
"""

import sys
from auth_manager import save_auth_config

def main():
    print("=" * 60)
    print("📱 iPHONE FLIPPER - AUTHENTICATION SETUP")
    print("=" * 60)
    print("\nPlease choose your authentication method:")
    print("1. Paste OAuth JSON (from your workspace)")
    print("2. Enter Gemini API Key (AIza...)")
    print("3. Enter OpenAI API Key (sk-...)")
    print("q. Quit")
    
    choice = input("\nChoice: ").strip().lower()
    
    if choice == '1':
        print("\nPlease paste your complete OAuth JSON below.")
        print("Press Ctrl+D (Unix/Mac) or Ctrl+Z (Windows) then Enter when finished:")
        
        try:
            lines = sys.stdin.readlines()
            json_str = "".join(lines).strip()
            
            if not json_str:
                print("❌ Error: No content provided.")
                return
            
            result = save_auth_config(json_str)
            if result.get("success"):
                print("\n✅ OAuth authentication successfully configured!")
                config = result["config"]
                print(f"   Auth Type: {config['auth_type']}")
                if config['expires_at']:
                    import datetime
                    exp_ts = config["expires_at"] / 1000 if config["expires_at"] > 1e11 else config["expires_at"]
                    exp_date = datetime.datetime.fromtimestamp(exp_ts).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"   Token Expires: {exp_date}")
            else:
                print(f"\n❌ Error: {result.get('error')}")
                
        except Exception as e:
            print(f"\n❌ Error parsing JSON: {e}")
            
    elif choice == '2':
        api_key = input("\nEnter your Gemini API Key (AIza...): ").strip()
        if not api_key.startswith("AIza"):
            print("❌ Error: Gemini API key should start with 'AIza'")
            return

        result = save_auth_config({"api_key": api_key})
        if result.get("success"):
            print("\n✅ Gemini API key authentication successfully configured!")
        else:
            print(f"\n❌ Error: {result.get('error')}")

    elif choice == '3':
        api_key = input("\nEnter your OpenAI API Key (sk-...): ").strip()
        if not api_key.startswith("sk-"):
            print("❌ Error: OpenAI API key should start with 'sk-'")
            return

        result = save_auth_config({"api_key": api_key})
        if result.get("success"):
            print("\n✅ OpenAI API key authentication successfully configured!")
        else:
            print(f"\n❌ Error: {result.get('error')}")
            
    elif choice == 'q':
        print("Setup cancelled.")
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
