#!/usr/bin/env bash
# Set up HTTPS for DocuSort via Tailscale's built-in cert issuer.
#
# What this does (idempotent — safe to run again):
#   1. Pulls a Let's Encrypt cert for this host's Tailscale MagicDNS name
#      via `tailscale cert`, places the PEM files in /etc/docusort/certs.
#   2. Points DocuSort's config at those files (web.ssl_cert / web.ssl_key).
#   3. Installs a weekly systemd timer that renews the cert and restarts
#      the service — tailscale cert is itself a no-op when the existing
#      cert still has > 30 days left.
#
# Requirements:
#   - Tailscale installed, this host logged into your tailnet
#   - HTTPS enabled in the tailnet admin console (Settings → DNS → HTTPS)
#   - Existing docusort.service systemd unit + config.yaml

set -euo pipefail

USER_NAME="${SUDO_USER:-$USER}"
CERT_DIR="/etc/docusort/certs"
CONFIG_FILE="/home/${USER_NAME}/docusort/config/config.yaml"

# ---------- 1. Determine the MagicDNS hostname ----------
DOMAIN="${1:-}"
if [ -z "$DOMAIN" ]; then
  # tailscale cert with no args prints the recommended domain in its usage string.
  DOMAIN="$(tailscale cert 2>&1 | grep -oE '[a-z0-9-]+\.[a-z0-9-]+\.ts\.net' | head -1 || true)"
fi
if [ -z "$DOMAIN" ]; then
  echo "error: couldn't detect your Tailscale MagicDNS hostname." >&2
  echo "       pass it explicitly:  $0 <host>.<tailnet>.ts.net" >&2
  exit 1
fi
echo "→ using domain: $DOMAIN"

CERT="$CERT_DIR/$DOMAIN.crt"
KEY="$CERT_DIR/$DOMAIN.key"

# ---------- 2. Issue / renew the cert ----------
sudo mkdir -p "$CERT_DIR"
echo "→ requesting cert from Tailscale (may take ~10s on first run)"
sudo tailscale cert --cert-file="$CERT" --key-file="$KEY" "$DOMAIN"

sudo chown -R "$USER_NAME":"$USER_NAME" "$CERT_DIR"
sudo chmod 644 "$CERT"
sudo chmod 600 "$KEY"

# ---------- 3. Systemd unit + timer for weekly renewal ----------
echo "→ installing renewal timer (weekly)"
sudo tee /etc/systemd/system/docusort-cert-renew.service >/dev/null <<EOF
[Unit]
Description=Renew Tailscale cert for DocuSort

[Service]
Type=oneshot
ExecStart=/usr/bin/tailscale cert --cert-file=$CERT --key-file=$KEY $DOMAIN
ExecStartPost=/bin/chown $USER_NAME:$USER_NAME $CERT $KEY
ExecStartPost=/bin/chmod 644 $CERT
ExecStartPost=/bin/chmod 600 $KEY
ExecStartPost=/bin/systemctl restart docusort
EOF

sudo tee /etc/systemd/system/docusort-cert-renew.timer >/dev/null <<EOF
[Unit]
Description=Renew DocuSort Tailscale cert weekly

[Timer]
OnCalendar=weekly
Persistent=true
RandomizedDelaySec=2h

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now docusort-cert-renew.timer

# ---------- 4. Wire cert paths into config.yaml ----------
if [ -f "$CONFIG_FILE" ]; then
  echo "→ updating $CONFIG_FILE"
  python3 - "$CONFIG_FILE" "$CERT" "$KEY" <<'PY'
import pathlib, re, sys
path = pathlib.Path(sys.argv[1])
cert, key = sys.argv[2], sys.argv[3]
text = path.read_text()

if "ssl_cert:" in text:
    text = re.sub(r'^(\s*)ssl_cert:.*$', rf'\1ssl_cert: "{cert}"', text, flags=re.M)
    text = re.sub(r'^(\s*)ssl_key:.*$',  rf'\1ssl_key:  "{key}"',  text, flags=re.M)
else:
    # Insert after the `port:` line inside the web: block.
    text, n = re.subn(
        r'(^web:\s*\n(?:.*\n)*?  port:[^\n]*\n)',
        lambda m: m.group(0) + f'  ssl_cert: "{cert}"\n  ssl_key:  "{key}"\n',
        text, count=1, flags=re.M,
    )
    if n == 0:
        print("  note: couldn't find web.port in config.yaml — add these lines by hand:", file=sys.stderr)
        print(f'    ssl_cert: "{cert}"', file=sys.stderr)
        print(f'    ssl_key:  "{key}"',  file=sys.stderr)
path.write_text(text)
print("  config.yaml updated")
PY
else
  echo "note: $CONFIG_FILE not found — add these lines under your web: block manually:"
  echo "    ssl_cert: \"$CERT\""
  echo "    ssl_key:  \"$KEY\""
fi

echo
echo "✓ HTTPS ready."
echo
echo "Next:  sudo systemctl restart docusort"
echo "Then:  https://$DOMAIN:9876"
