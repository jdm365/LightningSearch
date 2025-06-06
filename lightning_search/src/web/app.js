// import 'slickgrid/slick.grid.css';
// import { Grid } from "./node_modules/slickgrid";


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

const PORT = 5000;


function moveSearchColumnsToFront(columns, search_columns) {
    const searchCols = search_columns
        .map(searchColId => columns.find(col => col.id === searchColId))
        .filter(Boolean);

    const otherCols = columns.filter(col => !search_columns.includes(col.id));

    return [...searchCols, ...otherCols];
}

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
			console.log(`Skipping column ${columns[i].id} as it is not in search_columns`);
			return;
		}
		console.log(`Creating input for column ${columns[i].id}`);

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

	// Create search boxes
	const inputData = [];
	for (let i = 0; i < search_columns.length; i++) {
		const column = search_columns[i];
		inputData.push(
			{ id: `search_box_${column}`, placeholder: `Enter ${column} here...` }
		);
	}
}

async function waitForPort(port, retryInterval = 50, maxRetries = 20 * 60) {
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
		columns 	   = await get_columns();
		search_columns = await get_search_columns();
		columns = moveSearchColumnsToFront(columns, search_columns);

		grid = new Slick.Grid("#myGrid", data, columns, options);

		grid.onColumnsReordered.subscribe(function() {
			console.log("Columns reordered");
			setupHeaderRow();
		});

		console.log("Columns: ", columns);
		console.log("Search columns: ", search_columns);

		setupHeaderRow();
		search();
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
    if (typeof text !== 'string') return '';
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

    let tokens = query.split(/\s+/).slice(0, 6).filter(Boolean);
    if (tokens.length === 0) {
        return escapeHtml(value);
    }

    // Normalize value and build a mapping from normalized index to original index
    let original = value;
    let normalized = '';
    let normToOrig = [];
    for (let i = 0, j = 0; i < original.length; i++) {
        if (/[a-z0-9]/i.test(original[i])) {
            normalized += original[i].toLowerCase();
            normToOrig.push(i);
        }
    }

    // Find all match ranges in normalized string
    let matchRanges = [];
    for (let token of tokens) {
        let normToken = token.replace(/[^a-z0-9]/gi, '').toLowerCase();
        if (!normToken) continue;
        let startIdx = 0;
        while (true) {
            let idx = normalized.indexOf(normToken, startIdx);
            if (idx === -1) break;

            let origStart = normToOrig[idx];
            let origEnd = normToOrig[idx + normToken.length - 1] + 1;
            matchRanges.push([origStart, origEnd]);
            startIdx = idx + 1;
        }
    }

    // Merge overlapping ranges
    matchRanges.sort((a, b) => a[0] - b[0]);
    let merged = [];
    for (let range of matchRanges) {
        if (!merged.length || merged[merged.length - 1][1] < range[0]) {
            merged.push(range);
        } else {
            merged[merged.length - 1][1] = Math.max(merged[merged.length - 1][1], range[1]);
        }
    }

    // Build highlighted HTML
    let result = '';
    let lastIdx = 0;
    for (let [start, end] of merged) {
        result += escapeHtml(original.slice(lastIdx, start));
        result += '<span class="highlight">' + escapeHtml(original.slice(start, end)) + '</span>';
        lastIdx = end;
    }
    result += escapeHtml(original.slice(lastIdx));

    return result;
}

function escapeRegExp(string) {
    return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function updateGrid(results) {
	// Update metadata
	var numResultsDiv = document.getElementById('metadata');
	let time_taken_str = new Intl.NumberFormat().format(Math.round(results.time_taken_us)) + "us";
	numResultsDiv.innerHTML = `<b>Number of results:</b> ${results.results.length} 
							   &nbsp;&nbsp;&nbsp;&nbsp;
							   <b>Time taken:</b> ${time_taken_str}`;

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
			console.log("Search results:", data);
			updateGrid(data);
		});
}
