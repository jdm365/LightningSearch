const { app, BrowserWindow } = require('electron');
const { spawn } = require('child_process');
const path = require('node:path');
const fs = require('node:fs');

let backendProcess = null;

function createWindow () {
  const mainWindow = new BrowserWindow({
    width: 1600,
    height: 1200,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      nodeIntegration: false,
      contextIsolation: true,
    }
  })

  mainWindow.loadFile('index.html')

  // mainWindow.webContents.openDevTools()
}

app.whenReady().then(() => {
  const binaryPath = path.join(__dirname, 'lightning_search');
  // const dataFilePath = path.join(__dirname, '../../data/mb_small.csv');
  // const dataFilePath = path.join(__dirname, '../../data/mb.csv');
  const dataFilePath = path.join(__dirname, '../../data/mb.parquet');
  const args = [dataFilePath];

  console.log(`Starting backend command: ${binaryPath} ${args.join(' ')}`);

  if (!fs.existsSync(binaryPath)) {
      console.error(`Backend binary not found: ${binaryPath}`);
      app.quit();
      return;
  }

  backendProcess = spawn(binaryPath, args, {
    detached: true,
    stdio: 'ignore'
  });

  backendProcess.on('error', (err) => {
    console.error('Failed to start backend process:', err);
    app.quit();
  });

  backendProcess.on('exit', (code, signal) => {
    console.log(`Backend process exited with code ${code} and signal ${signal}`);
  });

  console.log("Backend process spawned. Continuing with Electron startup...");

  createWindow();

  app.on('activate', function () {
    if (BrowserWindow.getAllWindows().length === 0) createWindow()
  })
})

app.on('window-all-closed', function () {
  if (process.platform !== 'darwin') {
    if (backendProcess && !backendProcess.killed) {
      console.log("Terminating backend process...");
      backendProcess.kill();
    }
    app.quit();
  }
})

app.on('quit', () => {
  if (backendProcess && !backendProcess.killed) {
    backendProcess.kill();
  }
});
