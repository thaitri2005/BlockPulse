# Exercise 17: infrastructure as code you can test

The goal is to read the AWS collector as a program: resources declared in files, inputs with validation, and tests that run with no AWS account. You will run those tests, follow one security property from the file that declares it to the assertion that guards it, and then break it on purpose. Nothing is created in AWS in this exercise. Never run `terraform apply` or `terraform destroy` as part of it (decision D17: only the owner deploys, deliberately, following the [AWS guide](../guides/08-aws.md)).

**Infrastructure as code** means describing servers, networks and permissions in text files that a tool turns into real resources. **Terraform** is that tool here. A **resource** is one thing Terraform manages (a bucket, an instance); a **variable** is an input you can set; a **provider** is the plugin that talks to a cloud's API.

## Run it

From PowerShell:

```powershell
cd infra\aws\terraform
terraform init                      # downloads the AWS provider plugin; creates nothing
terraform fmt -check -recursive
terraform validate
terraform test
cd ..\..\..
.\.venv\Scripts\python.exe -m pytest -q tests/test_aws_packaging.py tests/test_lake.py
```

`terraform validate` checks syntax and types. `terraform test` runs [`tests/collector.tftest.hcl`](../../infra/aws/terraform/tests/collector.tftest.hcl), which starts with a **mocked provider**: a stand-in that returns made-up IDs instead of calling AWS, so the configuration can be planned and "applied" entirely in memory.

## Open the output

`terraform test` prints one line per `run` block and a total. The AWS guide records 9 passing runs for this file; the output on your machine is the thing to read, so it is not reproduced here. The run names are the table of contents of what is protected:

```text
secure_and_cost_capped_defaults
x86_instance_type_boots_x86_image_without_credit_block
no_budget_without_email
missing_bundle_is_reported_clearly
rejects_out_of_range_runtime
rejects_bad_email_and_url
rejects_bad_name
athena_reads_the_lake_with_partition_projection_and_a_scan_cap
rejects_tiny_athena_cutoff
```

Some runs assert on resources. Others use `expect_failures`: they pass only if a bad input (500 hours, an `http://` feed URL, an upper-case name) is rejected by a variable's `validation` block.

The two pytest files cover the seams between Python and Terraform: the start-up script template renders every placeholder, and `lake_schema.json` matches the Python lake contract.

## Read one file

Open [`infra/aws/terraform/network.tf`](../../infra/aws/terraform/network.tf) and find the instance's **security group**, which is its firewall:

```hcl
resource "aws_security_group" "collector" {
  name        = "${var.name}-collector"
  description = "BlockPulse collector: no inbound access; outbound HTTPS only"
  vpc_id      = aws_vpc.this.id

  egress {
    description = "HTTPS and WSS: mempool feed, S3, Systems Manager, package repositories"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
```

The decision to trace is "no inbound ports". It is made by an absence: there is an `egress` block (outbound rules) and no `ingress` block (inbound rules). A security group allows only what it lists, so nothing on the internet can open a connection to the instance. The collector never needs one. It dials out to the feed, to S3 and to package repositories, all on TCP 443. A shell, when needed, goes through Session Manager, which is also an outbound connection from the instance.

An absence is easy to undo by accident, so the test file pins it down:

```hcl
assert {
  condition     = length(aws_security_group.collector.ingress) == 0
  error_message = "The collector security group must not allow any inbound traffic."
}
```

Follow the same pattern once more in [`iam.tf`](../../infra/aws/terraform/iam.tf). **IAM** is AWS's permission system, and **least privilege** means granting only what the job needs. `local.instance_policy` has three statements: list the bucket, read `artifacts/*`, write `data/*` and `status/*`. There is no delete action. The matching assertion compares the full set of actions with exactly `s3:ListBucket`, `s3:GetObject` and `s3:PutObject`, so adding a fourth fails the test even if it looks harmless.

Then notice one variable with a guard, in [`variables.tf`](../../infra/aws/terraform/variables.tf): `max_runtime_hours` must be a whole number from 1 to 336. It flows into `local.deadline` in `main.tf`, then into the instance's `ExpiresAt` tag and the start-up script's `BLOCKPULSE_DEADLINE`. Exercise 18 picks up that value on the instance.

## Change one small thing

Add an inbound rule to the security group in `network.tf`, directly above the `egress` block:

```hcl
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
```

Run `terraform test` again. Predict first which run fails and which message you will see, then check. Remove the block, run `terraform fmt -check -recursive` and `terraform test` again, and confirm with `git diff` that the folder is unchanged.

The point of the experiment: a reviewer could miss six added lines, and the test does not.

## What the tests cannot tell you

A mocked provider checks what the configuration says, not what AWS does with it. It cannot show that the feed is reachable from the subnet, that the account's plan allows Glue, or what the bill is. Those were answered once by the real run of 2026-10-15 to 2026-10-19, recorded in the AWS guide's "What has and hasn't been verified".

## Question to carry forward

`compute.tf` sets `ignore_changes = [ami, user_data, tags["ExpiresAt"]]` on the instance. What would a routine `terraform apply` do to a running collector, and its unsynced data, without that line? And why does the test for the deadline compare against `time_static.created` instead of the current time?

## What was verified

Checked on 2026-11-24 on Linux: the two pytest files passed (`tests/test_aws_packaging.py`, and `tests/test_lake.py` with the schema-file test), and `blockpulse lake-schema` printed exactly the contents of `infra/aws/terraform/lake_schema.json`. **Not run:** `terraform init`, `fmt`, `validate` and `test`, and the experiment with the ingress rule, because Terraform was not installed in the session that wrote this page. The run names, quoted code and assertions were read from the files. Run them yourself before relying on the description.
