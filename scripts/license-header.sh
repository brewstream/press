#!/usr/bin/env bash
#
# Adds the Apache 2.0 header to every Java source file that lacks one, or checks
# that none are missing it.
#
#   scripts/license-header.sh          add the header where it is missing
#   scripts/license-header.sh --check  exit non-zero and list files missing it
#
# Idempotent: a file that already carries the header is left untouched, so this
# is safe to run repeatedly and safe to wire into a pre-commit hook.

set -euo pipefail

cd "$(dirname "$0")/.."

YEAR="${LICENSE_YEAR:-2026}"
CHECK_ONLY=false
if [[ "${1:-}" == "--check" ]]; then
    CHECK_ONLY=true
elif [[ $# -gt 0 ]]; then
    echo "usage: $0 [--check]" >&2
    exit 2
fi

read -r -d '' HEADER <<EOF || true
/*
 * Copyright ${YEAR} the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
EOF

missing=()
added=0

# -print0/read -d '' so a path containing a space cannot split into two names.
while IFS= read -r -d '' file; do
    # The marker is the licence URL rather than the word "License", which appears
    # in ordinary prose.
    if grep -q "www.apache.org/licenses/LICENSE-2.0" "$file"; then
        continue
    fi

    if $CHECK_ONLY; then
        missing+=("$file")
        continue
    fi

    printf '%s\n\n%s' "$HEADER" "$(cat "$file")" > "$file.licensed"
    mv "$file.licensed" "$file"
    added=$((added + 1))
done < <(find core/src -name '*.java' -print0)

if $CHECK_ONLY; then
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "Missing licence header in ${#missing[@]} file(s):" >&2
        printf '  %s\n' "${missing[@]}" >&2
        echo "Run scripts/license-header.sh to add it." >&2
        exit 1
    fi
    echo "All Java sources carry the licence header."
    exit 0
fi

echo "Added the licence header to ${added} file(s)."
