"""Publish structured per-source results without exposing credential values."""
import json
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit('No result report was produced; inspect initialization/readiness failure logs')
report = json.loads(path.read_text(encoding='utf-8'))
rows = report.get('sites') or [row for group in report.get('sources', {}).values() for row in group]
lines = ['## AIDNA bounded ingestion', f"Run status: **{report['status']}**", '',
         '| Source | Discovered/fetched | Processed | Confirmed delivery | Skipped | Page warnings | Failed | Deferred | Status |',
         '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |']
for row in rows:
    name = row.get('site') or row.get('collection')
    lines.append(f"| {name} | {row.get('discovered', row.get('fetched', 0))} | {row.get('processed', 0)} | {row.get('uploaded', row.get('inserted', 0))} | {row.get('skipped', 0)} | {row.get('warning_count', 0)} | {row.get('failed', 0)} | {row.get('deferred', 0)} | {row['status']} |")
for row in rows:
    if row.get('warnings'):
        lines += ['', f"### {row.get('site') or row.get('collection')}: skipped page warnings"]
        for warning in row['warnings']:
            # Keep source-controlled text on one line and outside Markdown link syntax.
            url = warning['url'].replace('\n', ' ').replace('\r', ' ')
            reason = warning['reason'].replace('\n', ' ').replace('\r', ' ')
            lines.append(f'- {url}: {reason}')
lines += ['', 'Deferred records remain eligible for the next run. Existing content is not deleted.']
lines += ['Page warnings are included in Skipped, remain retryable on later runs, and do not mean delivery succeeded.']
summary = '\n'.join(lines) + '\n'
print(summary)
if os.getenv('GITHUB_STEP_SUMMARY'):
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as output:
        output.write(summary)
