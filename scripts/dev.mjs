import { spawn } from 'node:child_process'
import process from 'node:process'

const isWin = process.platform === 'win32'
const python = isWin ? '.venv/Scripts/python.exe' : '.venv/bin/python'
const vite = isWin ? 'node_modules/.bin/vite.cmd' : 'node_modules/.bin/vite'

const children = [
  spawn(python, ['-m', 'uvicorn', 'backend.app:app', '--host', '127.0.0.1', '--port', '8765', '--reload'], { stdio: 'inherit' }),
  spawn(vite, ['--host', '127.0.0.1', '--port', '5173'], { stdio: 'inherit' }),
]

const stop = () => {
  children.forEach((child) => child.kill('SIGTERM'))
  setTimeout(() => process.exit(0), 250)
}

process.on('SIGINT', stop)
process.on('SIGTERM', stop)
children.forEach((child) => child.on('exit', (code) => code && stop()))
