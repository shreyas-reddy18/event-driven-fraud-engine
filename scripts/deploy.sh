#!/usr/bin/env bash
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="fraud-pipeline-lambda-${ACCOUNT_ID}-${REGION}"
PACKAGE_KEY="fraud-lambda.zip"
BUILD_DIR="$(mktemp -d)"

echo "==> Creating deployment bucket if needed"
if ! aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    $([ "$REGION" != "us-east-1" ] && echo "--create-bucket-configuration LocationConstraint=$REGION")
  aws s3api put-bucket-versioning --bucket "$BUCKET" \
    --versioning-configuration Status=Enabled
fi

echo "==> Building deployment package"
pip install \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  -r requirements.txt \
  -t "$BUILD_DIR" \
  --quiet
cp -r src "$BUILD_DIR/src"
python3 - "$BUILD_DIR" /tmp/fraud-lambda.zip <<'EOF'
import sys, os, zipfile, pathlib
build_dir = pathlib.Path(sys.argv[1])
out_zip   = sys.argv[2]
with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
    for path in build_dir.rglob("*"):
        if path.suffix == ".pyc" or "__pycache__" in path.parts:
            continue
        if path.is_file():
            zf.write(path, path.relative_to(build_dir))
EOF

echo "==> Uploading package to s3://$BUCKET/$PACKAGE_KEY"
aws s3 cp /tmp/fraud-lambda.zip "s3://$BUCKET/$PACKAGE_KEY" --region "$REGION"

echo "==> Fetching stack outputs"
SNS_ARN=$(aws cloudformation describe-stacks \
  --stack-name fraud-pipeline-sns \
  --query "Stacks[0].Outputs[?OutputKey=='TopicArn'].OutputValue" \
  --output text --region "$REGION")

echo "==> Deploying lambda stack"
aws cloudformation deploy \
  --template-file infrastructure/cloudformation/lambda.yaml \
  --stack-name fraud-pipeline-lambda \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    DeploymentBucket="$BUCKET" \
    DeploymentPackageKey="$PACKAGE_KEY" \
    KinesisStreamArn=arn:aws:kinesis:${REGION}:${ACCOUNT_ID}:stream/fraud-transactions \
    KinesisStreamName=fraud-transactions \
    DynamoDBTableName=fraud-transactions \
    SNSAlertTopicArn="$SNS_ARN"

echo "==> Updating Lambda function code"
aws lambda update-function-code \
  --function-name fraud-ingestion \
  --s3-bucket "$BUCKET" \
  --s3-key "$PACKAGE_KEY" \
  --region "$REGION" \
  --output json | grep -E '"FunctionName"|"CodeSize"|"LastModified"'

aws lambda update-function-code \
  --function-name fraud-processor \
  --s3-bucket "$BUCKET" \
  --s3-key "$PACKAGE_KEY" \
  --region "$REGION" \
  --output json | grep -E '"FunctionName"|"CodeSize"|"LastModified"'

rm -rf "$BUILD_DIR" /tmp/fraud-lambda.zip
echo "==> Done"
