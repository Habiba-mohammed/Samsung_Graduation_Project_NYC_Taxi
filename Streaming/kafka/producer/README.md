# Kafka Producer - S3 to Kafka

This producer downloads a Parquet file from S3 and streams rows to a Kafka topic.

IMPORTANT: Do NOT store AWS keys in the repository. Use environment variables or IAM profiles/roles.

Setup
1. Copy `.env.sample` to `.env` and set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, or use a configured AWS profile / role.
2. Ensure `AWS_REGION`, `S3_BUCKET` and `S3_KEY` are set if you want to override the defaults.
3. The script will fallback to the default AWS credential chain if `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are not present.

Security
- If a secret was committed by mistake, rotate/revoke those credentials immediately.
- Do not push secrets to a public repo or remote; use tools like `git-secrets`, pre-commit hooks, or GitHub secret scanning rules to prevent leaks.
