/**
 * The preload script runs before `index.html` is loaded
 * in the renderer. It has access to web APIs as well as
 * Electron's renderer process modules and some polyfilled
 * Node.js functions.
 *
 * https://www.electronjs.org/docs/latest/tutorial/sandbox
 */
window.addEventListener('DOMContentLoaded', () => {
	const replaceText = (selector, text) => {
	const element = document.getElementById(selector)
		if (element) element.innerText = text
	}

	for (const type of ['chrome', 'node', 'electron']) {
		replaceText(`${type}-version`, process.versions[type])
	}

	// Change the title of the window
	const title = document.getElementById('title')
	if (title) {
		title.innerText = 'Lightning Search'
	}

	// Run binary in this directory
	const { exec } = require('child_process')
	const path = require('path')
	const fs = require('fs')

	const binaryPath = path.join(__dirname, 'lightning_search')
	const args = ['../data/mb_small.csv']

	if (fs.existsSync(binaryPath)) {
		exec(binaryPath, args, (error, stdout, stderr) => {
			if (error) {
				console.error(`Error executing binary: ${error.message}`)
				return
			}
			if (stderr) {
				console.error(`stderr: ${stderr}`)
				return
			}
			console.log(`stdout: ${stdout}`)
		})
	}
})
