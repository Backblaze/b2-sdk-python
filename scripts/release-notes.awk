# Extract one exact version. Treat changelog text as data.
/^## \[/ {
    if (active) exit
    heading = $0
    sub(/^## \[/, "", heading)
    sub(/\].*$/, "", heading)
    if (heading == version) { active = 1; found = 1; next }
}
active { print }
END { if (!found) exit 1 }
