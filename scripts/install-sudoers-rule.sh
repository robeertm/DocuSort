#!/usr/bin/env bash
# Grant the current user permission to restart docusort.service without
# a sudo password. Needed for the in-UI updater's one-click apply.
#
# Run ONCE on the machine that hosts the service:
#     ./scripts/install-sudoers-rule.sh
#
# Removes nothing — the rule is scoped to exactly this one systemctl command
# for this one user, so it's safe to keep.

set -euo pipefail

USER_NAME="${SUDO_USER:-$USER}"
RULE_FILE="/etc/sudoers.d/docusort-restart"
SYSTEMCTL_PATH="$(command -v systemctl || echo /bin/systemctl)"

RULE_LINE="${USER_NAME} ALL=(ALL) NOPASSWD: ${SYSTEMCTL_PATH} restart docusort, ${SYSTEMCTL_PATH} restart docusort.service"

echo "→ Installing passwordless sudo rule at ${RULE_FILE}"
echo "  ${RULE_LINE}"
echo "${RULE_LINE}" | sudo tee "${RULE_FILE}" >/dev/null
sudo chmod 440 "${RULE_FILE}"

# Validate — if this file has a syntax error sudo would break for the whole box.
sudo visudo -cf "${RULE_FILE}" >/dev/null

echo "✓ rule installed. Test with:  sudo -n systemctl restart docusort"
