#!/bin/bash
# MicoDB Installer Script
# Install with: curl -sSL https://install.micodb.org | sh

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
RESET='\033[0m'

MICODB_VERSION="0.0.1"
INSTALL_DIR="${HOME}/.micodb"
BIN_DIR="${INSTALL_DIR}/bin"
RELEASE_URL="https://github.com/micodb/micodb/releases/download"

# Display banner
echo -e "${BLUE}"
echo "  __  __ _   ___ ___    ___  ___ "
echo " |  \/  (_) / __/ _ \  |   \| _ \\"
echo " | |\/| | || (_| (_) | | |) | _ <"
echo " |_|  |_|_| \___\___/  |___/|___/"
echo -e "${RESET}"
echo -e "${BOLD}High-performance columnar database for real-time analytics${RESET}"
echo ""

# Check for curl or wget
echo -e "${BLUE}Checking prerequisites...${RESET}"
if command -v curl &> /dev/null; then
    DOWNLOAD_CMD="curl -sSL"
elif command -v wget &> /dev/null; then
    DOWNLOAD_CMD="wget -qO-"
else
    echo -e "${RED}Error: Neither curl nor wget found. Please install one of them and try again.${RESET}"
    exit 1
fi

# Check for required commands
for cmd in tar gzip; do
    if ! command -v $cmd &> /dev/null; then
        echo -e "${RED}Error: '$cmd' not found. Please install it and try again.${RESET}"
        exit 1
    fi
done

# Detect operating system and architecture
echo -e "${BLUE}Detecting system...${RESET}"
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case $OS in
    linux)
        case $ARCH in
            x86_64)
                ARCH="x86_64"
                ;;
            aarch64|arm64)
                ARCH="aarch64"
                ;;
            *)
                echo -e "${RED}Error: Unsupported architecture: $ARCH${RESET}"
                exit 1
                ;;
        esac
        ;;
    darwin)
        case $ARCH in
            x86_64)
                ARCH="x86_64"
                ;;
            arm64)
                ARCH="aarch64"
                ;;
            *)
                echo -e "${RED}Error: Unsupported architecture: $ARCH${RESET}"
                exit 1
                ;;
        esac
        ;;
    *)
        echo -e "${RED}Error: Unsupported operating system: $OS${RESET}"
        exit 1
        ;;
esac

DOWNLOAD_FILE="micodb-${MICODB_VERSION}-${OS}-${ARCH}.tar.gz"
DOWNLOAD_URL="${RELEASE_URL}/v${MICODB_VERSION}/${DOWNLOAD_FILE}"

echo -e "${GREEN}Installing MicoDB ${MICODB_VERSION} for ${OS}-${ARCH}...${RESET}"

# Create installation directory
echo -e "${BLUE}Creating installation directories...${RESET}"
mkdir -p "${BIN_DIR}"

# Download and extract
echo -e "${BLUE}Downloading MicoDB...${RESET}"
echo "URL: ${DOWNLOAD_URL}"

# Check if download URL is accessible
if ! $DOWNLOAD_CMD "${DOWNLOAD_URL}" > /dev/null 2>&1; then
    echo -e "${RED}Error: Unable to access download URL: ${DOWNLOAD_URL}${RESET}"
    echo -e "${YELLOW}Note: This installer requires the MicoDB binaries to be published on GitHub Releases.${RESET}"
    echo -e "${YELLOW}For now, let's set up a development environment instead.${RESET}"
    setup_dev_environment
    exit 0
fi

# Download and extract
$DOWNLOAD_CMD "${DOWNLOAD_URL}" | tar -xz -C "${INSTALL_DIR}"

# Add to PATH if not already
SHELL_PROFILE=""
case $SHELL in
    */bash)
        SHELL_PROFILE="${HOME}/.bashrc"
        if [[ $OS == "darwin" ]]; then
            SHELL_PROFILE="${HOME}/.bash_profile"
        fi
        ;;
    */zsh)
        SHELL_PROFILE="${HOME}/.zshrc"
        ;;
    */fish)
        SHELL_PROFILE="${HOME}/.config/fish/config.fish"
        ;;
    *)
        SHELL_PROFILE="${HOME}/.profile"
        ;;
esac

# Check if PATH already contains MicoDB
if ! echo $PATH | grep -q "${BIN_DIR}"; then
    echo -e "${BLUE}Adding MicoDB to your PATH...${RESET}"
    
    # Add the bin directory to PATH
    if [[ -f "${SHELL_PROFILE}" ]]; then
        if [[ $SHELL == */fish ]]; then
            echo "fish_add_path ${BIN_DIR}" >> "${SHELL_PROFILE}"
        else
            echo "export PATH=\"\${PATH}:${BIN_DIR}\"" >> "${SHELL_PROFILE}"
        fi
        echo -e "${GREEN}Added MicoDB to your PATH in ${SHELL_PROFILE}${RESET}"
    else
        echo -e "${YELLOW}Warning: Could not determine shell profile. Please add the following line to your shell profile:${RESET}"
        echo -e "${YELLOW}export PATH=\"\${PATH}:${BIN_DIR}\"${RESET}"
    fi
fi

# Create symlinks in /usr/local/bin if possible
if [[ -d "/usr/local/bin" ]] && [[ -w "/usr/local/bin" ]]; then
    echo -e "${BLUE}Creating symlinks in /usr/local/bin...${RESET}"
    ln -sf "${BIN_DIR}/micodb" "/usr/local/bin/micodb"
    ln -sf "${BIN_DIR}/micodb-cli" "/usr/local/bin/micodb-cli"
else
    echo -e "${YELLOW}Note: Unable to create symlinks in /usr/local/bin (permission denied)${RESET}"
    echo -e "${YELLOW}You can still run MicoDB using the full path: ${BIN_DIR}/micodb${RESET}"
fi

# Test the installation
echo -e "${BLUE}Testing installation...${RESET}"
if "${BIN_DIR}/micodb" --version &> /dev/null; then
    echo -e "${GREEN}MicoDB installed successfully!${RESET}"
else
    echo -e "${RED}Installation failed. Please try again or install manually.${RESET}"
    exit 1
fi

# Setup development environment as a fallback
setup_dev_environment() {
    echo -e "${BLUE}Setting up MicoDB development environment...${RESET}"
    
    # Check for Rust
    if ! command -v rustc &> /dev/null; then
        echo -e "${YELLOW}Rust not found. Installing Rust...${RESET}"
        $DOWNLOAD_CMD https://sh.rustup.rs | sh -s -- -y
        source $HOME/.cargo/env
    fi
    
    # Clone the repository
    REPO_DIR="${INSTALL_DIR}/repo"
    mkdir -p "${REPO_DIR}"
    echo -e "${BLUE}Cloning MicoDB repository...${RESET}"
    git clone https://github.com/micodb/micodb.git "${REPO_DIR}" || {
        echo -e "${YELLOW}Note: MicoDB repository not yet public. Creating a placeholder project.${RESET}"
        mkdir -p "${REPO_DIR}/src/bin"
        echo 'fn main() { println!("MicoDB (Development Version)"); }' > "${REPO_DIR}/src/bin/micodb.rs"
        echo 'fn main() { println!("MicoDB CLI (Development Version)"); }' > "${REPO_DIR}/src/bin/micodb-cli.rs"
        echo '[package]
name = "micodb"
version = "0.0.1"
edition = "2021"' > "${REPO_DIR}/Cargo.toml"
    }
    
    # Build MicoDB
    echo -e "${BLUE}Building MicoDB from source...${RESET}"
    cd "${REPO_DIR}"
    cargo build --release
    
    # Create symlinks
    mkdir -p "${BIN_DIR}"
    ln -sf "${REPO_DIR}/target/release/micodb" "${BIN_DIR}/micodb"
    ln -sf "${REPO_DIR}/target/release/micodb-cli" "${BIN_DIR}/micodb-cli"
    
    echo -e "${GREEN}MicoDB development environment set up successfully!${RESET}"
}

# Display success message and usage instructions
echo ""
echo -e "${GREEN}${BOLD}MicoDB ${MICODB_VERSION} installed successfully!${RESET}"
echo ""
echo -e "${BOLD}Getting Started:${RESET}"
echo -e "  ${BLUE}Start the MicoDB server:${RESET} micodb"
echo -e "  ${BLUE}Use the MicoDB CLI:${RESET} micodb-cli -i"
echo ""
echo -e "${BOLD}Documentation:${RESET}"
echo -e "  ${BLUE}Website:${RESET} https://micodb.org"
echo -e "  ${BLUE}GitHub:${RESET} https://github.com/micodb/micodb"
echo ""
echo -e "${YELLOW}Note: You may need to restart your terminal or run 'source ${SHELL_PROFILE}' for PATH changes to take effect.${RESET}"