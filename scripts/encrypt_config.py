#!/usr/bin/env python3
"""
Configuration File Encryption Tool for Sparkplug Zerobus Connector.

This script provides a CLI for encrypting sensitive fields in configuration files.

Usage:
    python scripts/encrypt_config.py --identify config.yaml
    python scripts/encrypt_config.py --encrypt config.yaml
    python scripts/encrypt_config.py --decrypt config.encrypted.yaml
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sparkplug_connector.core.config_encryption import ConfigEncryption


def main():
    """Main entry point for configuration encryption."""
    parser = argparse.ArgumentParser(
        description='Configuration File Encryption Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Identify sensitive fields in configuration
  python scripts/encrypt_config.py --identify sparkplug_connector/config/config.yaml

  # Encrypt sensitive fields
  python scripts/encrypt_config.py --encrypt sparkplug_connector/config/config.yaml

  # Encrypt with custom output path
  python scripts/encrypt_config.py --encrypt config.yaml --output config.encrypted.yaml

  # Decrypt configuration
  python scripts/encrypt_config.py --decrypt config.encrypted.yaml --output config.yaml

Encrypted values are marked with ENC[...] prefix for easy identification.

Sensitive fields include:
  - password, secret, token, api_key
  - client_secret, session_secret_key
  - private_key, encryption_key
  - access_token, refresh_token

Environment variables (${env:VAR}) and credential references (${credential:key})
are NOT encrypted, as they are resolved at runtime.
        """
    )

    # Action arguments (mutually exclusive)
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument(
        '--identify',
        type=Path,
        metavar='CONFIG',
        help='Identify sensitive fields in configuration file'
    )
    action_group.add_argument(
        '--encrypt',
        type=Path,
        metavar='CONFIG',
        help='Encrypt sensitive fields in configuration file'
    )
    action_group.add_argument(
        '--decrypt',
        type=Path,
        metavar='CONFIG',
        help='Decrypt encrypted configuration file'
    )

    # Output option
    parser.add_argument(
        '--output',
        type=Path,
        help='Output file path (default: <input>.encrypted.yaml for encrypt, <input>.decrypted.yaml for decrypt)'
    )

    # Backup option
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Skip creating backup before encryption'
    )

    args = parser.parse_args()

    config_encryption = ConfigEncryption()

    try:
        if args.identify:
            print("🔍 Identifying Sensitive Fields")
            print("=" * 60)

            if not args.identify.exists():
                print(f"❌ File not found: {args.identify}")
                sys.exit(1)

            config = config_encryption.load_config(args.identify, decrypt=False)
            sensitive = config_encryption.identify_sensitive_fields(config)

            print(f"Found {len(sensitive)} sensitive field(s):")
            print()
            for field in sensitive:
                print(f"  • {field}")

            print()
            print("💡 Tip: Use --encrypt to encrypt these fields automatically")
            print()

        elif args.encrypt:
            print("🔐 Encrypting Configuration File")
            print("=" * 60)

            if not args.encrypt.exists():
                print(f"❌ File not found: {args.encrypt}")
                sys.exit(1)

            # Determine output path
            output_path = args.output or args.encrypt.with_suffix('.encrypted.yaml')

            # Show what will be encrypted
            config = config_encryption.load_config(args.encrypt, decrypt=False)
            sensitive = config_encryption.identify_sensitive_fields(config)

            print(f"Input:  {args.encrypt}")
            print(f"Output: {output_path}")
            print(f"Fields: {len(sensitive)} sensitive field(s) to encrypt")
            print()

            # Encrypt
            success = config_encryption.migrate_to_encrypted(args.encrypt, output_path)

            if success:
                print()
                print("✅ Configuration Encrypted Successfully!")
                print("=" * 60)
                print(f"Encrypted file: {output_path}")
                print()
                print("📝 Next Steps:")
                print("1. Verify encrypted configuration:")
                print(f"   cat {output_path}")
                print()
                print("2. Test loading encrypted configuration:")
                print(f"   python -c 'from sparkplug_connector.core.config_loader import ConfigLoader; c = ConfigLoader(\"{output_path}\"); print(c.load())'")
                print()
                print("3. If verification successful, replace original:")
                if not args.no_backup:
                    print(f"   mv {args.encrypt} {args.encrypt}.backup")
                print(f"   mv {output_path} {args.encrypt}")
                print()
                print("⚠️  Make sure CONNECTOR_MASTER_PASSWORD is set, or decryption will fail!")
                print()
            else:
                print("❌ Encryption failed")
                sys.exit(1)

        elif args.decrypt:
            print("🔓 Decrypting Configuration File")
            print("=" * 60)

            if not args.decrypt.exists():
                print(f"❌ File not found: {args.decrypt}")
                sys.exit(1)

            # Determine output path
            output_path = args.output or args.decrypt.with_suffix('.decrypted.yaml')

            print(f"Input:  {args.decrypt}")
            print(f"Output: {output_path}")
            print()

            # Decrypt
            config = config_encryption.load_config(args.decrypt, decrypt=True)
            backup = not args.no_backup
            config_encryption.save_config(config, output_path, encrypt=False, backup=backup)

            print()
            print("✅ Configuration Decrypted Successfully!")
            print("=" * 60)
            print(f"Decrypted file: {output_path}")
            print()
            print("⚠️  This file contains plaintext secrets - handle with care!")
            print()

    except Exception as e:
        print()
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
