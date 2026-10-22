#!/usr/bin/env python3
"""
TLS Certificate Generation Tool for Sparkplug Zerobus Connector.

This script provides a simple CLI for generating self-signed TLS certificates
for HTTPS support in the web UI.

Usage:
    python scripts/generate_tls_cert.py --generate
    python scripts/generate_tls_cert.py --info
    python scripts/generate_tls_cert.py --validate
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sparkplug_connector.core.tls_manager import TLSManager


def main():
    """Main entry point for TLS certificate management."""
    parser = argparse.ArgumentParser(
        description='TLS Certificate Manager for Sparkplug Zerobus Connector',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate new certificate (default: localhost, 365 days)
  python scripts/generate_tls_cert.py --generate

  # Generate certificate with custom common name and SANs
  python scripts/generate_tls_cert.py --generate \\
    --common-name connector.example.com \\
    --san localhost 127.0.0.1 192.168.1.100 connector.example.com \\
    --days 730

  # Validate existing certificate
  python scripts/generate_tls_cert.py --validate

  # Show certificate information
  python scripts/generate_tls_cert.py --info

After generating:
  1. Update config.yaml: set web_ui.tls.enabled = true
  2. Restart the connector
  3. Access UI at https://localhost:8082 (or your configured hostname)
        """
    )

    # Action arguments (mutually exclusive)
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument(
        '--generate',
        action='store_true',
        help='Generate new self-signed certificate'
    )
    action_group.add_argument(
        '--validate',
        action='store_true',
        help='Validate existing certificate'
    )
    action_group.add_argument(
        '--info',
        action='store_true',
        help='Show certificate information'
    )

    # Certificate options
    parser.add_argument(
        '--common-name',
        default='localhost',
        help='Certificate Common Name (CN) - default: localhost'
    )
    parser.add_argument(
        '--san',
        nargs='+',
        help='Subject Alternative Names (DNS names or IP addresses)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=365,
        help='Certificate validity in days - default: 365'
    )
    parser.add_argument(
        '--organization',
        default='Sparkplug Zerobus Connector',
        help='Organization name - default: "Sparkplug Zerobus Connector"'
    )
    parser.add_argument(
        '--key-size',
        type=int,
        default=2048,
        choices=[2048, 3072, 4096],
        help='RSA key size in bits - default: 2048'
    )
    parser.add_argument(
        '--cert-dir',
        type=Path,
        help='Certificate directory (default: ~/.sparkplug_connector/certs)'
    )

    args = parser.parse_args()

    # Initialize TLS manager
    tls_manager = TLSManager(cert_dir=args.cert_dir)

    try:
        if args.generate:
            print("🔐 Generating TLS Certificate")
            print("=" * 60)

            # Generate certificate
            cert_file, key_file = tls_manager.generate_self_signed_cert(
                common_name=args.common_name,
                organization=args.organization,
                validity_days=args.days,
                key_size=args.key_size,
                san_list=args.san
            )

            print()
            print("✅ Certificate Generated Successfully!")
            print("=" * 60)
            print(f"Certificate: {cert_file}")
            print(f"Private Key: {key_file}")
            print(f"Valid for: {args.days} days")
            print()

            # Show certificate info
            tls_manager.print_certificate_info()

            print()
            print("📝 Next Steps:")
            print("=" * 60)
            print("1. Update config.yaml:")
            print("   web_ui:")
            print("     tls:")
            print("       enabled: true")
            print()
            print("2. Restart the Sparkplug Zerobus Connector")
            print()
            print("3. Access web UI at:")
            if args.common_name == 'localhost':
                print("   https://localhost:8082")
            else:
                print(f"   https://{args.common_name}:8082")
            print()
            print("⚠️  Self-signed certificates will show a browser warning.")
            print("   This is normal for development. For production, use a")
            print("   certificate from a trusted Certificate Authority (CA).")
            print()

        elif args.validate:
            print("🔍 Validating Certificate")
            print("=" * 60)

            validation = tls_manager.validate_certificate()

            if validation.get('valid', False):
                print("✅ Certificate is VALID")
                print()
                tls_manager.print_certificate_info()
            else:
                print("❌ Certificate is INVALID")
                print(f"Error: {validation.get('error', 'Unknown error')}")
                print()
                print("Run with --generate to create a new certificate")
                sys.exit(1)

        elif args.info:
            print("📋 Certificate Information")
            print("=" * 60)

            if not tls_manager.cert_file.exists():
                print("❌ No certificate found")
                print(f"Expected location: {tls_manager.cert_file}")
                print()
                print("Run with --generate to create a new certificate")
                sys.exit(1)

            tls_manager.print_certificate_info()

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
