const path = require('path');

let PythonShellLibrary = require('python-shell');
let {PythonShell} = PythonShellLibrary;

const pythonScriptPath = path.join(__dirname, 'python_helpers', 'helpers.py');

function callPythonHelper(wfs_url) {
  let shell = new PythonShell(pythonScriptPath, {
    // The '-u' tells Python to flush every time
    pythonOptions: ['-u'],
    args: [wfs_url]
  });
  
  
  shell.on('message', function (message) {
    // received a message sent from the Python script (a simple "print" statement)
    console.log(message);
  });
}

module.exports = {
  callPythonHelper: callPythonHelper
}