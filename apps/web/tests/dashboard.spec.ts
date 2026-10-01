import { test, expect } from '@playwright/test';

test('analyst can sign in, investigate evidence, generate a cited summary, and export', async ({ page }) => {
  await page.goto('/');
  await page.getByLabel('Password', { exact: true }).fill('tracefleet-demo');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Live incidents', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Live incidents', exact: true }).click();
  await page.locator('.incident-card').first().click();
  await expect(page.getByRole('heading', { name: 'Linked evidence' })).toBeVisible();
  await expect(page.locator('tbody tr').first()).toBeVisible();
  await page.getByRole('button', { name: 'Generate AI summary' }).click();
  await expect(page.getByText(/validation passed/)).toBeVisible({ timeout: 20000 });
  const download = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Export JSON' }).click();
  expect((await download).suggestedFilename()).toMatch(/\.json$/);
  await page.screenshot({ path: 'artifacts/investigation.png', fullPage: true });
  await page.getByRole('heading', { name: 'Evidence-cited narrative' }).scrollIntoViewIfNeeded();
  await page.screenshot({ path: 'artifacts/ai-summary.png', fullPage: true });
  await page.getByRole('button', { name: 'Analytics', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Fleet analytics' })).toBeVisible();
  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible();
});

test('viewer sees only permitted navigation and rejected credentials are explained', async ({ page }) => {
  await page.goto('/');
  await page.getByLabel('Username', { exact: true }).fill('viewer');
  await page.getByLabel('Password', { exact: true }).fill('wrong');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Invalid username or password');
  await page.getByLabel('Password', { exact: true }).fill('tracefleet-demo');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Live incidents', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Analytics', exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Administration', exact: true })).toHaveCount(0);
  await page.screenshot({ path: 'artifacts/fleet-overview.png', fullPage: true });
});

test('critical telemetry reaches a connected browser within five seconds', async ({ page }) => {
  const { execFileSync } = await import('node:child_process');
  const vehicle = `browser_${Date.now()}`;
  execFileSync('docker', ['compose', 'exec', '-T', 'api', 'python', '-c', `import psycopg2
from packages.common.dsn import psycopg2_kwargs
with psycopg2.connect(**psycopg2_kwargs(owner=True)) as db,db.cursor() as c:
 c.execute("INSERT INTO vehicle(vehicle_id,tenant_id,fleet_id,vin,oem,model,model_year) VALUES(%s,'tenant_default','fleet_default','1HGCM82633A004352','TEST','Browser',2026)",('${vehicle}',))`]);
  await page.goto('/');
  await page.getByLabel('Password', { exact: true }).fill('tracefleet-demo');
  const loginStart = Date.now();
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Live incidents', exact: true })).toBeVisible();
  const dashboardMs = Date.now() - loginStart;
  expect(dashboardMs).toBeLessThan(2000);
  await page.getByRole('button', { name: 'Live incidents', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Live incidents', exact: true })).toBeVisible();
  const started = Date.now();
  execFileSync('docker', ['compose', 'exec', '-T', 'api', 'python', '-c', `import json
from packages.common import kafka
from sim.scenarios.registry import generate
p=kafka.build_producer()
for e in generate('collision_like','${vehicle}').events:
 p.produce(kafka.TELEMETRY_RAW,key='${vehicle}',value=json.dumps(e).encode())
assert p.flush(10)==0`]);
  await expect(page.locator('.incident-card').filter({ hasText: vehicle })).toBeVisible({ timeout: 5000 });
  const criticalAlertMs = Date.now() - started;
  expect(criticalAlertMs).toBeLessThan(5000);
  const { writeFileSync } = await import('node:fs');
  writeFileSync('artifacts/browser-latency.json', JSON.stringify({ dashboard_ms: dashboardMs, critical_alert_ms: criticalAlertMs, scope: 'local connected browser, seeded collision chain' }, null, 2));
});

test('administrator can inspect operations, users, roles, erasure and audit', async ({ page }) => {
  await page.goto('/');
  await page.getByLabel('Username', { exact: true }).fill('admin');
  await page.getByLabel('Password', { exact: true }).fill('tracefleet-demo');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await page.getByRole('button', { name: 'Operations', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Consumer lag' })).toBeVisible();
  await page.getByRole('button', { name: 'Administration', exact: true }).click();
  await expect(page.getByRole('cell', { name: 'analyst', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Roles', exact: true }).click();
  await expect(page.getByRole('cell', { name: 'VIEWER', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Erasure', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Request erasure' })).toBeVisible();
  await page.getByRole('button', { name: 'Audit', exact: true }).click();
  await expect(page.getByRole('columnheader', { name: 'outcome', exact: true })).toBeVisible();
});
