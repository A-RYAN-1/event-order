#!/usr/bin/env bash
# Applies the repository settings of ROADMAP 3.4 with the gh CLI.
#   scripts/apply-settings.sh OWNER/REPO            dry run: prints what would be sent
#   scripts/apply-settings.sh OWNER/REPO --apply    sends it, then verifies each setting
# Run it once at the public flip. Rulesets, secret scanning and push protection need a public
# repository or a paid plan; a step GitHub refuses is reported as FAIL and the script goes on.
set -u
repo=${1:?usage: apply-settings.sh OWNER/REPO [--apply]}
apply=${2:-}
dir=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
fail=0

call() { # method path [json-file]
  local m=$1 p=$2 f=${3:-}
  if [ "$apply" != --apply ]; then echo "DRY  $m $p ${f:+< $f}"; return 0; fi
  if [ -n "$f" ]; then
    gh api -X "$m" "repos/$repo${p:+/$p}" --input "$f" >/dev/null
  else
    gh api -X "$m" "repos/$repo${p:+/$p}" >/dev/null
  fi && echo "OK   $m $p" || { echo "FAIL $m $p"; fail=1; }
}
json() { printf '%s' "$2" >"$tmp/$1.json"; echo "$tmp/$1.json"; }

call PATCH "" "$(json repo '{"allow_squash_merge":true,"allow_merge_commit":false,"allow_rebase_merge":false,"delete_branch_on_merge":true,"squash_merge_commit_title":"PR_TITLE","squash_merge_commit_message":"PR_BODY","has_discussions":true}')"
call PUT actions/permissions/workflow "$(json wf '{"default_workflow_permissions":"read","can_approve_pull_request_reviews":false}')"
call PUT actions/permissions/fork-pr-contributor-approval "$(json fork '{"approval_policy":"all_external_contributors"}')"
call PATCH "" "$(json sec '{"security_and_analysis":{"secret_scanning":{"status":"enabled"},"secret_scanning_push_protection":{"status":"enabled"}}}')"
call PUT private-vulnerability-reporting
for r in main tags; do
  name=$(sed -n 's/^  "name": "\(.*\)",$/\1/p' "$dir/.github/rulesets/$r.json")
  id=$(gh api "repos/$repo/rulesets" -q ".[]|select(.name==\"$name\")|.id" 2>/dev/null || true)
  if [ -n "$id" ]; then call PUT "rulesets/$id" "$dir/.github/rulesets/$r.json"; else call POST rulesets "$dir/.github/rulesets/$r.json"; fi
done

if [ "$apply" != --apply ]; then echo; echo "Dry run only. Add --apply to send."; exit 0; fi
echo; echo "Verify:"
gh api "repos/$repo" -q '"squash_only=\(.allow_squash_merge and (.allow_merge_commit|not) and (.allow_rebase_merge|not)) delete_branch=\(.delete_branch_on_merge) secret_scanning=\(.security_and_analysis.secret_scanning.status) push_protection=\(.security_and_analysis.secret_scanning_push_protection.status)"'
gh api "repos/$repo/actions/permissions/workflow" -q '"token=\(.default_workflow_permissions) can_approve=\(.can_approve_pull_request_reviews)"'
gh api "repos/$repo/private-vulnerability-reporting" -q '"private_vuln_reporting=\(.enabled)"'
gh api "repos/$repo/rulesets" -q '.[]|"ruleset \(.name) \(.enforcement)"'
exit $fail
