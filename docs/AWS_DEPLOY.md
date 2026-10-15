# AWS collector: deploy, run, collect, tear down

A small, temporary, locked-down AWS setup that collects mempool data around the clock for a few days and then switches itself off. You run every command here; nothing is created in AWS until you run `terraform apply`. This is the plan's optional E3 step, pulled forward to collect data for M3 ([decision D16](PROJECT_PLAN.md#19-decision-register)).

## What gets created

```text
                 AWS account (your region, default us-west-2)
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  VPC 10.20.0.0/16 ── public subnet ── internet gateway ──► internet       │
  │        │                                                   (outbound     │
  │        └─ EC2 t4g.small, Amazon Linux 2023                  HTTPS only)  │
  │             blockpulse-collect.service   capture, hourly sessions        │
  │             blockpulse-process.timer     every 10 min: process + upload  │
  │             blockpulse-deadline.timer    power off after the deadline    │
  │                    │ writes data/ and status/                            │
  │                    ▼                                                     │
  │           S3 bucket (private, encrypted, TLS-only, objects expire)       │
  │  Budget alert email (optional, recommended)                              │
  └──────────────────────────────────────────────────────────────────────────┘
  You: Terraform to create/destroy, AWS CLI to read status and download data, Session Manager for a shell.
```

| Resource | Why | Notable settings |
| --- | --- | --- |
| VPC, public subnet, internet gateway | Outbound internet without a NAT gateway (which would cost more than everything else) | VPC default security group stripped of all rules |
| Security group | The instance's firewall | **No inbound rules**; outbound TCP 443 only |
| EC2 instance | Runs the collector | t4g.small (Graviton), IMDSv2 required, encrypted gp3 disk, standard CPU credits, stops itself at the deadline |
| IAM role | The instance's only permissions | Session Manager, read the app bundle, write `data/` and `status/` in its own bucket; cannot delete |
| S3 bucket | Collected data, status, app bundle | Public access blocked, AES-256, TLS required, objects expire after 30 days, not force-destroyable by default |
| Budget | Cost early warning | Monthly, alerts at 50%/100% actual and 100% forecast. Only if `alert_email` is set |
| Glue database and tables | Catalog for the Parquet lake the collector uploads | Partition projection on `dt`/`hour` (no crawler); columns generated from the Python lake contract |
| Athena workgroup and saved queries | SQL over the lake | Settings enforced, 1 GiB scan cap per query, encrypted results that expire after 7 days |

Files: Terraform in [`infra/aws/terraform/`](../infra/aws/terraform/), instance scripts and systemd units in [`infra/aws/instance/`](../infra/aws/instance/), bundle builder [`infra/aws/build_bundle.py`](../infra/aws/build_bundle.py), tests in [`infra/aws/tests/`](../infra/aws/tests/).

## Cost estimate

For the default 96 hours in us-west-2. Prices were checked on 2026-10-15 from secondary sources and memory; confirm them in the [AWS Pricing Calculator](https://calculator.aws/) before you apply.

| Item | Rate | 96 hours |
| --- | --- | ---: |
| t4g.small on-demand | $0.0168/hour | $1.61 |
| Public IPv4 address | $0.005/hour | $0.48 |
| gp3 disk, 40 GB | about $0.08/GB-month | $0.42 |
| S3 storage (about 20 GB by the end) and requests | $0.023/GB-month, a few thousand PUTs | about $0.10 |
| Data out when you download about 20 GB | first 100 GB/month free, then $0.09/GB | $0 to $1.80 |
| Session Manager, data in, budget, Glue catalog | no charge at this scale | $0 |
| Athena queries | about $5 per TB scanned, 10 MB minimum per query | under $0.01 for a few dozen demo queries |
| **Total** | | **about $2.60, up to about $4.40** |

After the deadline the instance is stopped, so compute and the public address stop billing, but the disk ($3.20/month for 40 GB) and the bucket keep billing until `terraform destroy`. New AWS accounts (since July 2025) get $100 to $200 of credits, which would cover this.

## Before you start (one time)

1. **AWS account.** Sign in as an administrator for setup. A personal account is fine.
2. **AWS CLI v2** ([install for Windows](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)). Then sign in, preferably with IAM Identity Center: `aws configure sso` (or `aws configure` with an access key). Check with `aws sts get-caller-identity`.
3. **Session Manager plugin** (optional, for a shell on the instance): [install](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html).
4. **Terraform 1.9 or newer.** This machine has 1.15.8.

## Deploy

From the repository root in PowerShell:

```powershell
.\.venv\Scripts\python.exe infra\aws\build_bundle.py            # builds infra/aws/build/blockpulse-app.zip
cd infra\aws\terraform
Copy-Item terraform.tfvars.example terraform.tfvars              # then set alert_email (and region if you like)
terraform init
terraform plan -out tfplan                                      # read it: about 20 resources, all tagged Project=blockpulse
terraform apply tfplan
terraform output                                                # bucket, instance id, deadline, handy commands
```

The deadline is fixed when the stack is created (`collection_deadline` output) and is also the instance's `ExpiresAt` tag. Changing `max_runtime_hours` later does not move it; see "Change things mid-run".

## Check that it is working

Give it about 10 minutes, then:

```powershell
Invoke-Expression (terraform output -raw status_command)     # status.json: updated every 10 minutes
aws s3 cp "s3://$(terraform output -raw bucket)/status/capture_log.csv" - --region (terraform output -raw region)
```

`status.json` appears about 10 minutes after boot. The first session finishes after an hour, so `capture_log.csv` and data appear after the first hour; each row shows messages, features, flagged rows, connection errors and stop reason per session.

For a shell on the instance (no SSH, no open ports):

```powershell
Invoke-Expression (terraform output -raw shell_command)
```

Then on the instance:

```bash
sudo cat /var/log/blockpulse-bootstrap.log                 # first-boot install log
systemctl status blockpulse-collect.service                 # capture loop
systemctl list-timers 'blockpulse-*'                        # processing and deadline timers
journalctl -u blockpulse-collect -u blockpulse-process -n 50
ls /srv/blockpulse/data/raw/
```

## Query the lake in Athena

From the first finished session (about an hour after boot), the collector uploads partitioned Parquet to `s3://<bucket>/data/lake/`. In the Athena console choose the `blockpulse-lake` workgroup and the `blockpulse_lake` database, then run one of the saved queries (`hourly_flag_rate`, `signals_by_rule`, `lifecycle_outcomes`, `capture_coverage`). Partition projection makes new hours visible immediately. Add `WHERE dt = '2026-10-17'` (and `hour`) to scan less. The same queries run locally: `blockpulse lake-query <name>`. Details: [lake guide](LAKE.md).

## Download the data and use it

Any time, as often as you like (it only copies what is new):

```powershell
cd D:\BlockPulse
Invoke-Expression (terraform -chdir=infra/aws/terraform output -raw download_command)   # into data/raw, processed, screened
.\.venv\Scripts\python.exe -m blockpulse process-pending --load-db                    # load into local Postgres (optional)
.\.venv\Scripts\python.exe -m blockpulse build-dataset                                 # then the M3 chain, see M3_REPORT.md
```

Sessions from AWS are named `session-aws-<UTC time>`, so they sit alongside your laptop sessions and the dataset manifest splits them by time like any other session.

## Tear down

When the deadline passes, the instance uploads what is left and powers off by itself. To finish:

1. Download the data (above) and check it arrived (`data/screened/session-aws-*`).
2. Allow the non-empty bucket to be deleted, then destroy everything:

   ```powershell
   cd infra\aws\terraform
   terraform apply -var bucket_force_destroy=true     # only flips the bucket setting
   terraform destroy
   ```

3. Check nothing is left: `aws ec2 describe-instances --filters Name=tag:Project,Values=blockpulse --query "Reservations[].Instances[].State.Name" --region us-west-2` should print `[]` (or only `terminated`), and `aws s3 ls | Select-String blockpulse` should print nothing.
4. Look at the bill a day or two later (Billing console, or the budget emails) and note the actual cost in the plan's cost worksheet.

Keep `terraform.tfstate` until the destroy has finished: it is how Terraform knows what to delete. To stop early, run the same steps before the deadline; the capture stops cleanly when the instance shuts down, and anything not yet uploaded is lost with the disk.

## Change things mid-run

The running instance is never replaced automatically: newer Amazon Linux images and edits to the start-up script are ignored, so a stray `terraform apply` can't throw away unsynced data.

- **New code:** rebuild the bundle and `terraform apply` (uploads it). Then either replace the instance after the next upload (`terraform apply -replace=aws_instance.collector`; a new instance gets a new deadline), or on the instance run the same steps as first boot:

  ```bash
  sudo aws s3 cp s3://<bucket>/artifacts/blockpulse-app.zip /opt/blockpulse/app.zip
  sudo rm -rf /opt/blockpulse/app && sudo python3 -m zipfile -e /opt/blockpulse/app.zip /opt/blockpulse/app
  sudo bash -c 'set -a; source /etc/blockpulse/collector.env; set +a; APP_DIR=/opt/blockpulse/app AWS_REGION=$AWS_DEFAULT_REGION bash /opt/blockpulse/app/infra/aws/instance/install.sh'
  sudo systemctl restart blockpulse-collect.service   # ends the current session cleanly, starts a new one
  ```

- **Longer run:** edit `BLOCKPULSE_DEADLINE` in `/etc/blockpulse/collector.env` on the instance and `sudo systemctl restart blockpulse-collect.service`.

## Security notes

- Nothing listens on the internet: no inbound rules, no SSH keys, no public services. Shell access is Session Manager, governed by your AWS sign-in.
- The instance metadata service requires IMDSv2 tokens, with a hop limit of 1.
- The services run as an unprivileged `blockpulse` user under a strict systemd sandbox (no capabilities, read-only system, network-only address families, system-call filter). `systemd-analyze security` rates the capture service 1.4 ("OK").
- The instance role can write data but cannot delete or read other buckets. The bucket rejects non-TLS requests and is never public.
- Collected data is public blockchain data. The bucket still stays private, and published results should follow the project's rules about not making claims about address owners.

## What has and hasn't been verified

Verified on 2026-10-15 without an AWS account:

| Check | How | Result |
| --- | --- | --- |
| Terraform syntax and types | `terraform validate` with AWS provider 6.69.0 | valid |
| Security and cost properties | `terraform test` with a mocked AWS provider (9 runs: no inbound rules, HTTPS-only egress, IMDSv2, encryption, TLS-only bucket, least-privilege IAM, deadline tag, budget, x86 fallback, Glue partition projection, Athena scan cap and result expiry, input validation) | 9 passed |
| Bootstrap on Amazon Linux 2023 | `instance_smoke.sh` in the official AL2023 container: Python 3.11 from dnf, package and `pyarrow` from PyPI, install twice, bad deadline refused, processing and lake build as the service user, upload via a fake AWS CLI, deadline logic, SIGINT | 7 passed |
| Graviton (ARM64), the real instance's architecture | the same smoke test in an emulated ARM64 Amazon Linux 2023 container (`docker run --platform linux/arm64`), including the ARM64 `pyarrow` wheel | 7 passed |
| The real systemd units | `systemd_e2e.sh` in AL2023 booted with systemd: services start, processing and upload run inside the sandbox, `systemctl stop` is clean, the deadline powers the machine off | all passed |
| Unit files | `systemd-analyze verify`, `systemd-analyze security` | clean; exposure 1.4 |
| Shell scripts, rendered user data | shellcheck 0.10 | clean |
| Collector logic | pytest (back-to-back sessions, deadline clipping and no sliver sessions, low disk, cooldown, idempotent processing, set-aside of interrupted stages, lock, upload once, load after compaction, interrupt ends the loop) | passed |
| Whole suite | pytest with Postgres | 119 passed, 1 skipped (optional loopback test) |

Not verified, because it needs a real AWS account:

- That `terraform apply` succeeds against AWS (service quotas, account restrictions, a region without t4g capacity).
- That the instance role, bucket policy and Session Manager work together as intended in a real account.
- That the Glue tables and partition projection read the uploaded Parquet correctly in real Athena (the same files and SQL are tested locally with DuckDB).
- That outbound HTTPS alone is enough for everything the instance needs (dnf, PyPI, S3, Systems Manager, the feed). If bootstrap fails, check `/var/log/blockpulse-bootstrap.log` first.
- That the regional mempool.space node is reachable from AWS, and how often it disconnects there.
- Actual cost. Record it after teardown.

Run all the local checks yourself with `bash infra/aws/tests/run_container_tests.sh` (Docker) and `terraform test` in `infra/aws/terraform`. CI runs them on every push (`.github/workflows/ci.yml`; not yet run on GitHub).

## Troubleshooting

| Symptom | Likely cause and fix |
| --- | --- |
| `terraform plan` says the bundle is missing | Run `python infra/aws/build_bundle.py` from the repository root |
| `status.json` never appears | Bootstrap failed: open a Session Manager shell and read `/var/log/blockpulse-bootstrap.log` |
| Session Manager can't connect | The SSM agent needs a few minutes after boot; check the instance is running and the role has `AmazonSSMManagedInstanceCore` |
| Capture log shows `retry_limit` or 0 messages | The feed was unreachable; the loop waits a minute and starts a new session. Try another `source_url` if it persists |
| `terraform destroy` fails on the bucket | It still has data: download it, then `terraform apply -var bucket_force_destroy=true` and destroy again |
| Instance is `stopped` | Normal after the deadline. Download the data, then destroy |
