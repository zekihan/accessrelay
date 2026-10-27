# OPS-43 owner acceptance prerequisites

- [ ] Verify Docker Hub publication tokens are scoped to this repository and only
  required operations. Rotate credentials through the approved owner process if
  needed. Token scope and credential rotation cannot be proved from source.
- [ ] Inspect repository/environment protections and require approval for write
  tokens where supported. This change does not create or alter live environments,
  repository policy, access controls, or credential configuration.
- [ ] After review/merge and specific publication authorization, run the normal
  release workflow with approved narrowly scoped credentials and confirm registry,
  chart (where applicable), and GitHub publication. No release, image publication,
  live credential operation, or Actions retry was performed for this proposal.
- [ ] On the next Renovate action update, verify the PR preserves a full commit SHA
  and release-version comment, and independently verify the upstream tag mapping.
