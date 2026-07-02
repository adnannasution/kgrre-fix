import { spawn } from 'node:child_process'
import process from 'node:process'

const port = process.env.PORT || '8765'
const python = process.platform === 'win32' ? '.venv/Scripts/python.exe' : '.venv/bin/python'

const service = spawn(
  python,
  ['-m', 'uvicorn', 'backend.app:app', '--host', '0.0.0.0', '--port', port],
  { stdio: 'inherit' },
)

service.on('exit', (code) => process.exit(code ?? 0))
