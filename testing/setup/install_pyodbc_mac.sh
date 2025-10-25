#!/bin/bash
# Script to install pyodbc on macOS

echo "Installing pyodbc on macOS..."
echo "This requires Homebrew and unixODBC"
echo

# Check if brew is installed
if ! command -v brew &> /dev/null; then
    echo "Error: Homebrew is not installed!"
    echo "Please install Homebrew first: https://brew.sh"
    exit 1
fi

# Install unixODBC
echo "Installing unixODBC..."
brew install unixodbc

# Install Microsoft ODBC Driver for SQL Server
echo "Installing Microsoft ODBC Driver 18 for SQL Server..."
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18

# Now install pyodbc with the correct flags
echo "Installing pyodbc Python package..."
export LDFLAGS="-L$(brew --prefix unixodbc)/lib"
export CPPFLAGS="-I$(brew --prefix unixodbc)/include"
pip install --no-cache-dir pyodbc

echo
echo "Installation complete!"
echo "You can verify with: python -c 'import pyodbc; print(pyodbc.version)'"