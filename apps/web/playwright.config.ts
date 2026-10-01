import { defineConfig } from '@playwright/test';
export default defineConfig({ testDir: './tests', use: { baseURL: process.env.PLAYWRIGHT_BASE_URL || 'http://127.0.0.1:5173', screenshot: 'only-on-failure', trace: 'retain-on-failure' }, reporter: [['list'], ['html', { outputFolder: '../../artifacts/browser-report', open: 'never' }]], outputDir: '../../artifacts/browser-results' });
