const leftEditor = CodeMirror.fromTextArea(document.getElementById('leftEditor'), {
    theme: 'monokai',
    lineNumbers: true,
    autoCloseBrackets: true,
    matchBrackets: true,
    styleActiveLine: true,
    lineWrapping: true,
    indentUnit: 4,
    indentWithTabs: false
});

const rightEditor = CodeMirror.fromTextArea(document.getElementById('rightEditor'), {
    theme: 'monokai',
    lineNumbers: true,
    readOnly: false,
    styleActiveLine: true,
    lineWrapping: true
});

leftEditor.setValue(`SELECT * FROM table1 WHERE element > 30;`);

async function processCode() {
    const inputCode = leftEditor.getValue();
    try {
        const response = await fetch('/to-dsql', {
            method: 'POST',
            headers: {'Content-Type': 'text/plain'},
            body: JSON.stringify(inputCode)
        });
        const res = await response.text();
        rightEditor.setValue(res);
        updateStatus('Code submitted successfully!');
    }
    catch (error) {
        console.error('Error processing code:', error);
        rightEditor.setValue(`Error: ${error.message}`);
        updateStatus('Error processing code.');
    }
}

function updateStatus(message) {
    const statusEl = document.getElementById('status');
    statusEl.textContent = message;
    setTimeout(() => {
        statusEl.textContent = 'Ready';
    }, 3000);
}

window.addEventListener('resize', function() {
    leftEditor.refresh();
    rightEditor.refresh();
});

leftEditor.addKeyMap({
    'Ctrl-Enter': async function () {
        await processCode();
    },
    'Cmd-Enter': async function () {
        await processCode();
    }
});