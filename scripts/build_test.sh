#!/bin/bash
# MicoDB build test script
# Tests building MicoDB on different platforms

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
RESET='\033[0m'

echo -e "${BLUE}${BOLD}MicoDB Build Test${RESET}"
echo ""

# Get OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

echo -e "${BLUE}System information:${RESET}"
echo "OS: $OS"
echo "Architecture: $ARCH"
echo "Rust version: $(rustc --version)"
echo "Cargo version: $(cargo --version)"
echo ""

# Clean previous builds
echo -e "${BLUE}Cleaning previous builds...${RESET}"
cargo clean

# Unset problematic environment variables
echo -e "${BLUE}Unsetting potentially problematic environment variables...${RESET}"
unset CFLAGS
unset CXXFLAGS
unset MACOSX_DEPLOYMENT_TARGET

# Set platform-specific flags
if [[ "$OS" == "darwin" ]]; then
    echo -e "${BLUE}Setting up macOS-specific build environment...${RESET}"
    export MACOSX_DEPLOYMENT_TARGET=10.7
else
    echo -e "${BLUE}Setting up non-macOS build environment...${RESET}"
    # Empty to avoid macOS flags on non-macOS platforms
fi

# Build
echo -e "${BLUE}Building MicoDB...${RESET}"
cargo build --verbose

if [ $? -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Build successful!${RESET}"
else
    echo -e "${RED}${BOLD}Build failed!${RESET}"
    exit 1
fi

# Run tests if build was successful
echo -e "${BLUE}Running tests...${RESET}"
cargo test --verbose

if [ $? -eq 0 ]; then
    echo -e "${GREEN}${BOLD}All tests passed!${RESET}"
else
    echo -e "${RED}${BOLD}Tests failed!${RESET}"
    exit 1
fi

echo -e "${GREEN}${BOLD}Build and tests completed successfully!${RESET}"
