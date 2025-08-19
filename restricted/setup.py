#!/usr/bin/env python3
"""
Setup script for SBC scraper.
This script helps set up the environment and dependencies.
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(
            command, shell=True, check=True, capture_output=True, text=True
        )
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"   Command: {command}")
        print(f"   Error: {e.stderr}")
        return False


def check_python_version():
    """Check if Python version is adequate."""
    print("🐍 Checking Python version...")
    version = sys.version_info

    if version.major >= 3 and version.minor >= 8:
        print(
            f"✅ Python {version.major}.{version.minor}.{version.micro} is supported"
        )
        return True
    else:
        print(
            f"❌ Python {version.major}.{version.minor}.{version.micro} is not supported"
        )
        print("   Minimum required version: Python 3.8")
        return False


def install_dependencies():
    """Install Python dependencies."""
    print("📦 Installing Python dependencies...")

    requirements_file = Path(__file__).parent / "requirements.txt"
    if not requirements_file.exists():
        print("❌ requirements.txt not found")
        return False

    command = f"{sys.executable} -m pip install -r {requirements_file}"
    return run_command(command, "Installing Python packages")


def install_playwright():
    """Install Playwright browsers."""
    print("🎭 Installing Playwright browsers...")

    commands = [
        f"{sys.executable} -m playwright install chromium",
        f"{sys.executable} -m playwright install-deps chromium",
    ]

    for command in commands:
        if not run_command(command, "Installing Playwright browsers"):
            return False

    return True


def setup_env_file():
    """Set up .env file if it doesn't exist or needs updating."""
    print("⚙️  Setting up environment file...")

    env_file = Path(__file__).parent / ".env"

    if env_file.exists():
        # Check if credentials are set
        with open(env_file, "r") as f:
            content = f.read()

        if "your_username_here" in content or "your_password_here" in content:
            print("⚠️  Please edit .env file and set your SBC credentials:")
            print(f"   File location: {env_file.absolute()}")
            print(
                "   Set SBC_USERNAME and SBC_PASSWORD to your actual credentials"
            )
            return False
        else:
            print("✅ Environment file is configured")
            return True
    else:
        print("❌ .env file not found")
        print("   The .env file should have been created during project setup")
        return False


def verify_setup():
    """Verify that setup was successful."""
    print("🔍 Verifying setup...")

    try:
        # Try importing our modules
        sys.path.insert(0, str(Path(__file__).parent))

        import config
        import helpers
        import tools
        import main

        print("✅ All modules can be imported successfully")

        # Check configuration
        from config import settings

        print(f"✅ Configuration loaded (headless: {settings.headless})")

        return True

    except ImportError as e:
        print(f"❌ Module import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Setup verification failed: {e}")
        return False


def main():
    """Main setup function."""
    print("🚀 SBC Scraper Setup")
    print("=" * 40)

    steps = [
        ("Check Python version", check_python_version),
        ("Install dependencies", install_dependencies),
        ("Install Playwright", install_playwright),
        ("Setup environment", setup_env_file),
        ("Verify setup", verify_setup),
    ]

    for step_name, step_func in steps:
        print(f"\n📋 {step_name}")
        print("-" * 30)

        if not step_func():
            print(f"\n❌ Setup failed at: {step_name}")
            print("Please fix the issues above and run setup again.")
            return False

    print("\n" + "=" * 40)
    print("🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Edit .env file and set your SBC credentials")
    print("2. Run test script: python test_scraper.py")
    print("3. Start scraping: python main.py --interactive")

    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n👋 Setup interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Setup failed with error: {str(e)}")
        sys.exit(1)
