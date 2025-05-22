#!/bin/bash

# MicoDB Installer Script
# This script downloads and installs MicoDB from GitHub

set -e

# Default settings
VERSION="latest"
INSTALL_DIR="${HOME}/.micodb"
REPO_URL="https://github.com/micodb/micodb"
GITHUB_API="https://api.github.com/repos/micodb/micodb"

# Colors for formatting
COLOR_GREEN="\033[0;32m"
COLOR_YELLOW="\033[0;33m"
COLOR_BLUE="\033[0;34m"
COLOR_RED="\033[0;31m"
COLOR_RESET="\033[0m"

# ANSI escape for cursor movement
CURSOR_UP="\033[1A"
CLEAR_LINE="\033[2K"

# Print a formatted message
print_msg() {
    local color=$1
    local msg=$2
    echo -e "${color}${msg}${COLOR_RESET}"
}

# Print a progress message
progress_msg() {
    echo -e "${COLOR_BLUE}[INFO]${COLOR_RESET} $1"
}

# Print a success message
success_msg() {
    echo -e "${COLOR_GREEN}[SUCCESS]${COLOR_RESET} $1"
}

# Print a warning message
warning_msg() {
    echo -e "${COLOR_YELLOW}[WARNING]${COLOR_RESET} $1"
}

# Print an error message
error_msg() {
    echo -e "${COLOR_RED}[ERROR]${COLOR_RESET} $1"
}

# Check for required tools
check_requirements() {
    progress_msg "Checking requirements..."
    
    local missing_tools=()
    
    # Check for curl
    if ! command -v curl &> /dev/null; then
        missing_tools+=("curl")
    fi
    
    # Check for tar
    if ! command -v tar &> /dev/null; then
        missing_tools+=("tar")
    fi
    
    # Check for grep
    if ! command -v grep &> /dev/null; then
        missing_tools+=("grep")
    fi
    
    # Check for awk
    if ! command -v awk &> /dev/null; then
        missing_tools+=("awk")
    fi
    
    if [ ${#missing_tools[@]} -gt 0 ]; then
        error_msg "The following tools are required but missing: ${missing_tools[*]}"
        error_msg "Please install them and try again."
        exit 1
    fi
    
    success_msg "All required tools are available."
}

# Detect the operating system and architecture
detect_platform() {
    progress_msg "Detecting platform..."
    
    # Detect OS
    OS="unknown"
    case "$(uname -s)" in
        Linux*)     OS="linux";;
        Darwin*)    OS="macos";;
        CYGWIN*)    OS="windows";;
        MINGW*)     OS="windows";;
        MSYS*)      OS="windows";;
        *)          OS="unknown";;
    esac
    
    # Detect architecture
    ARCH="unknown"
    case "$(uname -m)" in
        x86_64)     ARCH="x86_64";;
        arm64)      ARCH="arm64";;
        aarch64)    ARCH="arm64";;
        *)          ARCH="unknown";;
    esac
    
    if [ "$OS" == "unknown" ] || [ "$ARCH" == "unknown" ]; then
        error_msg "Unsupported platform: $(uname -s) $(uname -m)"
        error_msg "MicoDB is available for Linux and macOS on x86_64 and arm64 architectures."
        exit 1
    fi
    
    success_msg "Detected platform: $OS-$ARCH"
}

# Get the latest available version
get_latest_version() {
    progress_msg "Getting latest version..."
    
    # Get the latest release from GitHub API
    local response
    response=$(curl -s "${GITHUB_API}/releases/latest")
    
    # Extract the tag name
    LATEST_VERSION=$(echo "$response" | grep -o '"tag_name": *"[^"]*"' | awk -F'"' '{print $4}')
    
    if [ -z "$LATEST_VERSION" ]; then
        error_msg "Failed to determine the latest version."
        exit 1
    fi
    
    success_msg "Latest version: $LATEST_VERSION"
}

# Download and install MicoDB
install_micodb() {
    progress_msg "Installing MicoDB $VERSION..."
    
    # Create install directory
    mkdir -p "$INSTALL_DIR"
    
    # Determine the version to install
    if [ "$VERSION" == "latest" ]; then
        get_latest_version
        VERSION="$LATEST_VERSION"
    fi
    
    # Construct the download URL
    local download_url="${REPO_URL}/releases/download/${VERSION}/micodb-${VERSION}-${OS}-${ARCH}.tar.gz"
    
    # Download the binary
    progress_msg "Downloading from $download_url..."
    if ! curl -L --progress-bar "$download_url" -o "${INSTALL_DIR}/micodb.tar.gz"; then
        error_msg "Failed to download MicoDB from $download_url"
        error_msg "Please check your internet connection and verify that the release exists."
        error_msg "Visit ${REPO_URL}/releases to see available releases."
        exit 1
    fi
    
    # Extract the archive
    progress_msg "Extracting archive..."
    if ! tar -xzf "${INSTALL_DIR}/micodb.tar.gz" -C "$INSTALL_DIR"; then
        error_msg "Failed to extract the archive."
        exit 1
    fi
    rm "${INSTALL_DIR}/micodb.tar.gz"
    
    # Make the binary executable
    chmod +x "${INSTALL_DIR}/bin/micodb"
    
    # Create symlink
    progress_msg "Setting up path..."
    mkdir -p "${HOME}/.local/bin"
    ln -sf "${INSTALL_DIR}/bin/micodb" "${HOME}/.local/bin/micodb"
    
    success_msg "MicoDB installed successfully to ${INSTALL_DIR}"
}

# Setup the configuration
setup_config() {
    progress_msg "Setting up configuration..."
    
    # Create data directory
    mkdir -p "${HOME}/.micodb/data"
    
    # Copy default configuration files
    if [ -f "${INSTALL_DIR}/config/default_config.json" ]; then
        mkdir -p "${HOME}/.micodb/config"
        cp "${INSTALL_DIR}/config/default_config.json" "${HOME}/.micodb/config/config.json"
        cp "${INSTALL_DIR}/config/node_config.json" "${HOME}/.micodb/config/node_config.json"
        cp "${INSTALL_DIR}/config/worker_config.json" "${HOME}/.micodb/config/worker_config.json"
    fi
    
    success_msg "Configuration setup complete"
}

# Add MicoDB to PATH
add_to_path() {
    progress_msg "Adding MicoDB to PATH..."
    
    local path_updated=false
    local shell_rc=""
    
    # Determine shell configuration file
    if [ -n "$BASH_VERSION" ]; then
        shell_rc="$HOME/.bashrc"
    elif [ -n "$ZSH_VERSION" ]; then
        shell_rc="$HOME/.zshrc"
    fi
    
    if [ -n "$shell_rc" ]; then
        # Check if path already added
        if ! grep -q "PATH=.*\\.local/bin" "$shell_rc"; then
            echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$shell_rc"
            path_updated=true
        fi
    fi
    
    if [ "$path_updated" = true ]; then
        success_msg "Added MicoDB to PATH in $shell_rc"
        warning_msg "Please run 'source $shell_rc' or start a new terminal to use MicoDB."
    else
        warning_msg "PATH not updated. Please add $HOME/.local/bin to your PATH manually."
    fi
}

# Print usage information
print_usage() {
    local script_name=$(basename "$0")
    cat << EOF
Usage: ${script_name} [OPTIONS]

Options:
  -v, --version VERSION  Install specific version (default: latest)
  -d, --dir DIR          Installation directory (default: ~/.micodb)
  -h, --help             Display this help message

Examples:
  ${script_name}                   # Install the latest version
  ${script_name} -v 1.0.0          # Install version 1.0.0
  ${script_name} -d /opt/micodb    # Install to /opt/micodb

Report issues at: https://github.com/micodb/micodb/issues
EOF
}

# Parse command-line arguments
parse_args() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            -v|--version)
                VERSION="$2"
                shift 2
                ;;
            -d|--dir)
                INSTALL_DIR="$2"
                shift 2
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                error_msg "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done
}

# Main function
main() {
    print_msg "$COLOR_BLUE" "
╔═╗╔═╗╔═╗╔═╗╔═╗  ╔╦╗┬┌─┐┌─┐╔╦╗╔╗   
╚═╗╚═╗║╣ ╚═╗╚═╗   ║ ││  │ │ ║║╠╩╗  
╚═╝╚═╝╚═╝╚═╝╚═╝   ╩ ┴└─┘└─┘═╩╝╚═╝  
"
    print_msg "$COLOR_BLUE" "Welcome to the MicoDB installer script!"
    print_msg "$COLOR_BLUE" "======================================="
    echo ""

    # Parse command-line arguments
    parse_args "$@"
    
    # Check for required tools
    check_requirements
    
    # Detect platform
    detect_platform
    
    # Install MicoDB
    install_micodb
    
    # Setup configuration
    setup_config
    
    # Add to PATH
    add_to_path
    
    echo ""
    success_msg "Installation complete!"
    print_msg "$COLOR_GREEN" "You can now use MicoDB by running 'micodb' in your terminal."
    print_msg "$COLOR_GREEN" "Quick start: micodb --help"
    echo ""
}

# Run the main function
main "$@"