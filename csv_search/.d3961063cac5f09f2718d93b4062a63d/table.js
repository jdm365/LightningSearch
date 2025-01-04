var grid;

var options = {
	enableCellNavigation: true,
	enableColumnReorder: true,
	editable: false,
	showHeaderRow: true,
	headerRowHeight: 30,
};

var data = [];
var search_columns = [];
var columns = [];

PORT = 5000;

async function get_columns() {
	try {
		const response = await fetch(`http://localhost:${PORT}/get_columns`);
		if (!response.ok) {
			throw new Error('Network response was not ok');
		}
		const data = await response.json();

		// Calculate column width
		const gridContainerWidth = document.getElementById('alt-view').offsetWidth * 0.4;
		const numColumns = data.columns.length;
		const widthPerColumn = gridContainerWidth / Math.min(5, numColumns);
		const minWidthPerColumn = gridContainerWidth / Math.min(5, numColumns);
		const columnWidth = Math.max(widthPerColumn, minWidthPerColumn);

		var columns = data.columns.map(column => ({
			id: column,
			name: column,
			field: column,
			width: columnWidth,
			formatter: highlightMatchingText,
		}));

		return columns;
	} catch (error) {
		console.error('Error fetching columns:', error);
		return [];
	}
}

async function get_search_columns() {
	try {
		const response = await fetch(`http://localhost:${PORT}/get_search_columns`);
		if (!response.ok) {
			throw new Error('Network response was not ok');
		}
		const data = await response.json();

		return data.columns;

	} catch (error) {
		console.error('Error fetching columns:', error);
		return [];
	}
}

function setupHeaderRow() {
    var headerRow = grid.getHeaderRow();
    var headerCells = headerRow.querySelectorAll(".slick-headerrow-column");
    
    headerCells.forEach((cell, i) => {
		// Only continue if column is in search_columns
		if (!search_columns.includes(columns[i].id)) {
			return;
		}

        var column = columns[i];
        var input = document.createElement('input');
        input.type = 'text';
        input.dataset.columnId = column.id;
        input.style.width = "100%";
        // input.value = column.headerFilter && column.headerFilter.value || "";
        // input.classList.add("slick-headerrow-column-filter");
        cell.appendChild(input);

        input.addEventListener("input", function() {
            search();
        });
    });

    grid.resizeCanvas();
}

async function waitForPort(port, retryInterval = 10000, maxRetries = 60) {
	return;
    let retries = 0;
    while (retries < maxRetries) {
        try {
            // Attempt to fetch a resource from the server on the specified port
            const response = await fetch(
				`http://localhost:${PORT}/healthcheck`, 
				// Make no-cors request to avoid CORS policy blocking the request
				{ method: 'HEAD' }
			);
            if (response.ok) {
                // Port is available
                console.log(`Server is up on port ${port}`);
                return;
            }
        } catch (error) {
            console.error(`Error fetching healthcheck: ${error.message}`);
            // Wait for the retry interval before checking again
            await new Promise(resolve => setTimeout(resolve, retryInterval));
        }
        retries++;
    }
    console.error(`Server did not start on port ${port} after ${maxRetries} attempts`);
    throw new Error(`Server did not start on port ${port}`);
}

document.addEventListener("DOMContentLoaded", function() {
	(async function() {
		await waitForPort(PORT);
		search_columns = await get_search_columns();
		columns = await get_columns();

		grid = new Slick.Grid("#myGrid", data, columns, options);

		setupHeaderRow();
		search();


		// Create search boxes
		const inputData = [];
		for (let i = 0; i < search_columns.length; i++) {
			const column = search_columns[i];
			inputData.push(
				{ id: `search_box_${column}`, placeholder: `Enter ${column} here...` }
			);
		}
	})();
});

const escapeMap = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;'
};

function escapeHtml(text) {
    return text.replace(/[&<>"']/g, m => escapeMap[m]);
}

function highlightMatchingText(row, cell, value, columnDef, dataContext) {
	let searchBox = document.querySelector(`input[data-column-id="${columnDef.name}"]`);

    if (!searchBox || !value || typeof value !== 'string') {
        return escapeHtml(value);
    }

    let query = searchBox.value.trim();
    if (!query) {
        return escapeHtml(value);
    }

    let tokens = query.split(/\s+/).filter(token => /[a-z0-9]/i.test(token)).slice(0, 6);
    if (tokens.length === 0) {
        return escapeHtml(value);
    }

    let escapedValue = escapeHtml(value);
    let regex = new RegExp('(' + tokens.map(escapeRegExp).join('|') + ')', 'gi');
    return escapedValue.replace(regex, '<span class="highlight">$1</span>');
}

function escapeRegExp(string) {
    return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}


function updateGrid(results) {
	// Update metadata
	var numResultsDiv = document.getElementById('metadata');
	numResultsDiv.innerHTML = `<b>Number of results:</b> ${results.results.length} 
							   &nbsp;&nbsp;&nbsp;&nbsp;
							   <b>Time taken:</b> ${Math.round(results.time_taken_ms)}ms`;

	// Update grid data
	grid.setData(results.results);
	grid.invalidate();
	grid.render();
}

// Define function `search` as GET request to the API
function search() {
	let params = {};
	search_columns.forEach(column => {
		let input_element = document.querySelector(`input[data-column-id="${column}"]`);

		if (input_element && input_element.value) {
			params[column] = input_element.value;
		}
	});

	let text_string = new URLSearchParams(params).toString();
	let query = `${text_string}`;

	fetch(`http://localhost:${PORT}/search?${query}`)
		.then(response => response.json())
		.then(data => {
			updateGrid(data);
		});
}
