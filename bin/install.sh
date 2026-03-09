#!/bin/bash

set -e

# ==========================================
# Configuration & Constants
# ==========================================
DEFAULT_VERSION="2.0.0.Alpha1"
GITHUB_REPO="jgroups-extras/jgroups-raft"
INSTALL_DIR="$HOME/.jgroups-raft"
INIT_SCRIPT="$INSTALL_DIR/bin/raft-init.sh"
WRAPPER_NAME="raft"
TARGET_WRAPPER="$INSTALL_DIR/bin/$WRAPPER_NAME"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# ==========================================
# Error Trapping (SDKMAN Style)
# ==========================================
track_last_command() {
    last_command=$current_command
    current_command=$BASH_COMMAND
}
trap track_last_command DEBUG

echo_failed_command() {
    local exit_code="$?"
    if [[ "$exit_code" != "0" ]]; then
        echo -e "${RED}'$last_command': command failed with exit code $exit_code.${NC}"
    fi
}
trap echo_failed_command EXIT

# ==========================================
# Functions
# ==========================================
function check_existing_install() {
    echo -e " > ${BLUE}Checking for previous installation...${NC}"

    # We check if the DIRECTORY exists
    if [ -d "$INSTALL_DIR" ]; then
        # If the binary ALSO exists, the install is likely complete.
        if [ -f "$TARGET_WRAPPER" ]; then
            echo -e "${GREEN}JGroups Raft CLI found.${NC}"
            echo ""
            echo "=================================================================================="
            echo -e " You already have JGroups Raft CLI installed at:"
            echo ""
            echo -e "    ${BLUE}${INSTALL_DIR}${NC}"
            echo ""
            echo " No extra steps needed."
            echo "=================================================================================="
            echo ""
            exit 0
        else
            # Directory exists, but binary is missing. Resume installation.
            echo -e " > ${BLUE}Found installation directory but missing binary. Resuming installation...${NC}"
        fi
    fi
}

function resolve_version() {
    echo -e " > ${BLUE}Resolving version...${NC}"

    # 1. Attempt to fetch latest tag from GitHub API
    LATEST_VERSION=$(curl --max-time 5 -s "https://api.github.com/repos/$GITHUB_REPO/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')

    VALID_VERSION_FOUND=false
    if [ -n "$LATEST_VERSION" ]; then
        echo -e " > Found latest release tag: ${GREEN}$LATEST_VERSION${NC}"

        # 2. Check if the wrapper script exists for this specific tag
        # We construct the URL where the script *should* be.
        CHECK_URL="https://raw.githubusercontent.com/${GITHUB_REPO}/${LATEST_VERSION}/bin/${WRAPPER_NAME}"

        echo -e " > Verifying artifact availability..."

        # --head: fetch headers only
        # --fail: fail with non-zero on 404
        if curl --output /dev/null --silent --head --fail "$CHECK_URL"; then
            VERSION="$LATEST_VERSION"
            BASE_URL="https://raw.githubusercontent.com/${GITHUB_REPO}/${VERSION}/bin"
            VALID_VERSION_FOUND=true
            echo -e " > ${GREEN}Verified. Using version $VERSION.${NC}"
        else
            echo -e " > ${RED}Warning: Release tag exists, but wrapper script is missing at:${NC}"
            echo -e "   $CHECK_URL"
        fi
    fi

    # 3. Fallback if API failed OR if wrapper was missing in the release
    if [ "$VALID_VERSION_FOUND" = false ]; then
        echo -e " > ${RED}Falling back to default: $DEFAULT_VERSION${NC}"
        VERSION="$DEFAULT_VERSION"
        BASE_URL="https://raw.githubusercontent.com/${GITHUB_REPO}/main/bin"
    fi
}

function setup_directories() {
    if [ ! -d "$INSTALL_DIR/bin" ]; then
        echo -e " > Creating directory structure..."
        mkdir -p "$INSTALL_DIR/bin"
    fi
}

function download_wrapper() {
    # Resumability: Check if file already exists
    if [ -f "$TARGET_WRAPPER" ]; then
         echo -e " > ${GREEN}Wrapper script already exists. Skipping download.${NC}"
         return
    fi

    echo -e " > ${BLUE}Downloading CLI wrapper...${NC}"
    WRAPPER_URL="${BASE_URL}/${WRAPPER_NAME}"

    if command -v curl >/dev/null 2>&1; then
        curl -fL -o "$TARGET_WRAPPER" "$WRAPPER_URL"
    elif command -v wget >/dev/null 2>&1; then
        wget -O "$TARGET_WRAPPER" "$WRAPPER_URL"
    else
        echo -e "${RED}Error: Neither curl nor wget found.${NC}"
        exit 1
    fi

    chmod +x "$TARGET_WRAPPER"
}

function generate_init_script() {
    echo -e " > ${BLUE}Generating initialization script...${NC}"

    # We always overwrite this to ensure it matches the current path configuration
    cat <<EOF > "$INIT_SCRIPT"
#!/bin/bash

# JGroups Raft CLI Initialization
export JGROUPS_RAFT_HOME="$INSTALL_DIR"

# Define the alias
alias raft="\$JGROUPS_RAFT_HOME/bin/raft"

# Enable Auto-completion
# Sourced dynamically from the wrapper
if [[ "\$SHELL" == *"zsh"* ]] || [[ "\$SHELL" == *"bash"* ]]; then
    # We suppress stderr to avoid noise during shell startup
    source <("\$JGROUPS_RAFT_HOME/bin/raft" completion 2>/dev/null)
fi
EOF
}

function configure_profile() {
    echo -e " > ${BLUE}Configuring shell profile...${NC}"

    SNIPPET="[[ -s \"$INIT_SCRIPT\" ]] && source \"$INIT_SCRIPT\""
    PROFILE_FILE=""

    if [ -n "$ZSH_VERSION" ]; then
        PROFILE_FILE="$HOME/.zshrc"
    elif [ -n "$BASH_VERSION" ]; then
        PROFILE_FILE="$HOME/.bashrc"
    else
        PROFILE_FILE="$HOME/.profile"
    fi

    if [ -f "$PROFILE_FILE" ]; then
        if grep -q "raft-init.sh" "$PROFILE_FILE"; then
            echo -e " > ${GREEN}Profile already configured in $PROFILE_FILE${NC}"
        else
            echo "" >> "$PROFILE_FILE"
            echo "# JGroups Raft CLI" >> "$PROFILE_FILE"
            echo "$SNIPPET" >> "$PROFILE_FILE"
            echo -e " > ${GREEN}Added init snippet to $PROFILE_FILE${NC}"
        fi
    else
        echo -e " > ${RED}Could not detect profile file. Please add this manually:${NC}"
        echo -e "   $SNIPPET"
    fi
}

function finish() {
    echo -e "${GREEN}Installation complete!${NC}"
    echo -e "Please open a new terminal or run: ${BLUE}source $PROFILE_FILE${NC}"
}

# ==========================================
# Execution Flow
# ==========================================
echo -e "${BLUE}Initializing JGroups Raft CLI Installation${NC}"

check_existing_install
resolve_version
setup_directories
download_wrapper
generate_init_script
configure_profile
finish
