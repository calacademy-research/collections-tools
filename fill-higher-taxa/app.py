import os
import json
import uuid
import tempfile

import xlrd
import xlwt
from xlutils.copy import copy
import mysql.connector
from fuzzywuzzy import fuzz
from flask import Flask, request, render_template, send_file, Response

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB

WORK_DIR = tempfile.mkdtemp(prefix='fill-taxa-')

DB_CONFIG = {
    'host': os.environ.get('DATABASE_HOST', 'mariadb'),
    'port': int(os.environ.get('DATABASE_PORT', '3306')),
    'user': os.environ.get('DATABASE_USER', 'root'),
    'password': os.environ.get('DATABASE_PASSWORD', 'password'),
    'database': os.environ.get('DATABASE_NAME', 'casiz'),
}

# Color names for the JSON sidecar: maps style purpose to CSS color
COLOR_NEW = '#ff4444'       # red — not found in Specify
COLOR_REPLACE = '#44cc44'   # green — value replaced from Specify
COLOR_WARNING = '#cc44cc'   # magenta — multiple matches / new column
COLOR_FILLED = '#cce5ff'    # light blue — filled in from Specify (was blank)


def search_for_name(cursor, name, parent, parent_rank):
    name = name.strip()
    if parent == "":
        sql = "SELECT * FROM vtaxonallranks WHERE fullname=%s"
        params = (name,)
    else:
        sql = f"SELECT * FROM vtaxonallranks WHERE fullname=%s AND `{parent_rank}`=%s"
        params = (name, parent)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    rowcount = len(rows)
    row = rows[0] if rows else None
    return row, rowcount


def styled_write(writesheet, row, col, value, style, cell_colors):
    """Write to xlwt sheet AND record the color for the preview."""
    writesheet.write(row, col, value, style)
    cell_colors[f"{row},{col}"] = style._color


def process_workbook_streaming(book, job_id):
    wb = copy(book)

    column_number_of_taxon_ranks = {}
    taxon_rank_and_rank_id = {}
    taxon_rank_ordered = []
    # Track cell colors: key = "row,col", value = css color string
    cell_colors = {}
    # Track all cell values (overrides from original): key = "row,col", value = string
    cell_values = {}

    # Styles — attach a _color attribute for our tracking
    warning_style = xlwt.easyxf('pattern: pattern solid;')
    warning_style.pattern.pattern_fore_colour = xlwt.Style.colour_map['magenta_ega']
    warning_style._color = COLOR_WARNING

    new_name_style = xlwt.easyxf('pattern: pattern solid;')
    new_name_style.pattern.pattern_fore_colour = xlwt.Style.colour_map['red']
    new_name_style._color = COLOR_NEW

    replace_style = xlwt.easyxf('pattern: pattern solid;')
    replace_style.pattern.pattern_fore_colour = xlwt.Style.colour_map['lime']
    replace_style._color = COLOR_REPLACE

    filled_style = xlwt.easyxf('pattern: pattern solid;')
    filled_style.pattern.pattern_fore_colour = xlwt.Style.colour_map['light_blue']
    filled_style._color = COLOR_FILLED

    def tw(r, c, val, style=None):
        """Track-write: write to xlwt + record for preview."""
        if style:
            writesheet.write(r, c, val, style)
            cell_colors[f"{r},{c}"] = style._color
        else:
            writesheet.write(r, c, val, filled_style)
            cell_colors[f"{r},{c}"] = COLOR_FILLED
        cell_values[f"{r},{c}"] = val if val is not None else ""

    yield sse_event('status', {'message': 'Connecting to database...'})

    mydb = mysql.connector.connect(**DB_CONFIG)
    cursor = mydb.cursor(buffered=True, dictionary=True)

    yield sse_event('status', {'message': 'Loading taxon ranks...'})

    cursor.execute(
        "SELECT rankid, LOWER(name) AS name FROM taxontreedefitem "
        "WHERE NAME <> 'life' ORDER BY rankid;"
    )
    for x in cursor:
        taxon_rank_and_rank_id[x["name"]] = x["rankid"]
        taxon_rank_ordered.append(x["name"])
    taxon_rank_ordered = tuple(taxon_rank_ordered)

    species_author_column = -1
    higher_taxon_column = -1

    worksheet = book.sheet_by_index(0)
    first_row = []
    for col in range(worksheet.ncols):
        first_row.append(worksheet.cell_value(0, col))
        cleaned = first_row[col].lower().replace('1', '')
        if cleaned in taxon_rank_and_rank_id:
            column_number_of_taxon_ranks[cleaned] = col
        else:
            if first_row[col].lower() == 'species author1':
                species_author_column = col
            if first_row[col].lower() == 'higher taxon':
                higher_taxon_column = col

    if higher_taxon_column == -1:
        mydb.close()
        yield sse_event('error', {'message': "File does not have a 'Higher Taxon' column"})
        return

    writesheet = wb.get_sheet(0)
    maxcolumn = worksheet.ncols
    headers = list(first_row)

    for key in taxon_rank_and_rank_id:
        if key not in column_number_of_taxon_ranks:
            writesheet.write(0, maxcolumn, key, warning_style)
            cell_colors[f"0,{maxcolumn}"] = COLOR_WARNING
            headers.append(key)
            column_number_of_taxon_ranks[key.lower()] = maxcolumn
            maxcolumn += 1

    total_rows = worksheet.nrows - 1
    total_cols = maxcolumn
    yield sse_event('status', {'message': f'Processing {total_rows} rows...', 'total': total_rows})

    for row in range(1, worksheet.nrows):
        fullname = ""
        full_name_rank_level = 0

        if (column_number_of_taxon_ranks.get("genus", worksheet.ncols) < worksheet.ncols
                and worksheet.cell_value(row, column_number_of_taxon_ranks["genus"])):
            full_name_rank_level = taxon_rank_ordered.index("genus")
            fullname = worksheet.cell_value(row, column_number_of_taxon_ranks["genus"])
            if worksheet.cell_value(row, column_number_of_taxon_ranks["species"]):
                full_name_rank_level = taxon_rank_ordered.index("species")
                fullname += " " + worksheet.cell_value(row, column_number_of_taxon_ranks["species"])
            if ("subspecies" in column_number_of_taxon_ranks
                    and worksheet.cell_value(row, column_number_of_taxon_ranks["subspecies"])):
                full_name_rank_level = taxon_rank_ordered.index("subspecies")
                fullname += " " + worksheet.cell_value(row, column_number_of_taxon_ranks["subspecies"])
        else:
            if (column_number_of_taxon_ranks.get("subfamily", worksheet.ncols) < worksheet.ncols
                    and worksheet.cell_value(row, column_number_of_taxon_ranks["subfamily"])):
                fullname = worksheet.cell_value(row, column_number_of_taxon_ranks["subfamily"])
                full_name_rank_level = taxon_rank_ordered.index("subfamily")
            elif (column_number_of_taxon_ranks.get("family", worksheet.ncols) < worksheet.ncols
                    and worksheet.cell_value(row, column_number_of_taxon_ranks["family"])):
                fullname = worksheet.cell_value(row, column_number_of_taxon_ranks["family"])
                full_name_rank_level = taxon_rank_ordered.index("family")
            else:
                fullname = worksheet.cell_value(row, higher_taxon_column)
                full_name_rank_level = taxon_rank_ordered.index("family") - 1

        multiple_matches = 0
        mysqlrow = None
        rowcount = 0
        result_status = 'skipped'

        if fullname != "":
            mysqlrow, rowcount = search_for_name(cursor, fullname, "", "")

            if rowcount == 0 and full_name_rank_level >= taxon_rank_ordered.index("family"):
                tw(row,
                   column_number_of_taxon_ranks[taxon_rank_ordered[full_name_rank_level]],
                   worksheet.cell_value(row, column_number_of_taxon_ranks[taxon_rank_ordered[full_name_rank_level]]),
                   new_name_style)
                full_name_rank_level -= 1
                while full_name_rank_level >= taxon_rank_ordered.index("family") and rowcount == 0:
                    val = worksheet.cell_value(row, column_number_of_taxon_ranks[taxon_rank_ordered[full_name_rank_level]])
                    mysqlrow, rowcount = search_for_name(cursor, val, "", "")
                    if rowcount == 0 and val != "":
                        tw(row,
                           column_number_of_taxon_ranks[taxon_rank_ordered[full_name_rank_level]],
                           val, new_name_style)
                    if rowcount == 0:
                        full_name_rank_level -= 1

            if rowcount == 0 and worksheet.cell_value(row, higher_taxon_column) != "":
                mysqlrow, rowcount = search_for_name(cursor, worksheet.cell_value(row, higher_taxon_column), "", "")
                if rowcount == 0:
                    tw(row, higher_taxon_column, worksheet.cell_value(row, higher_taxon_column), new_name_style)

            if rowcount > 1:
                multiple_matches = 1
                if taxon_rank_ordered[full_name_rank_level] == "species":
                    parent_level = full_name_rank_level - 2
                else:
                    parent_level = full_name_rank_level - 1
                while (worksheet.cell_value(row, column_number_of_taxon_ranks[taxon_rank_ordered[parent_level]]) == ""
                       and parent_level >= taxon_rank_ordered.index("family")):
                    parent_level -= 1
                mysqlrow, rowcount = search_for_name(
                    cursor,
                    worksheet.cell_value(row, column_number_of_taxon_ranks[taxon_rank_ordered[full_name_rank_level]]),
                    worksheet.cell_value(row, column_number_of_taxon_ranks[taxon_rank_ordered[parent_level]]),
                    taxon_rank_ordered[parent_level],
                )
                if rowcount == 1:
                    multiple_matches = 0

            if (taxon_rank_ordered.index("genus") - 1) < full_name_rank_level:
                rank_level = taxon_rank_ordered.index("genus") - 1
            else:
                rank_level = full_name_rank_level

            while rank_level >= 0:
                if taxon_rank_ordered[rank_level] in column_number_of_taxon_ranks:
                    col_idx = column_number_of_taxon_ranks[taxon_rank_ordered[rank_level]]
                    if mysqlrow and rowcount == 1:
                        if col_idx < worksheet.ncols:
                            cell_val = worksheet.cell_value(row, col_idx)
                            db_val = mysqlrow[taxon_rank_ordered[rank_level]]
                            if cell_val == "":
                                tw(row, col_idx, db_val)  # filled_style (default)
                            elif cell_val != db_val:
                                tw(row, col_idx, db_val, replace_style)
                        else:
                            tw(row, col_idx, mysqlrow[taxon_rank_ordered[rank_level]])
                    else:
                        if multiple_matches == 1:
                            tw(row, col_idx, "Multiple matches found", warning_style)
                        else:
                            tw(row, col_idx, "*****", warning_style)
                rank_level -= 1

            if (species_author_column > 0
                    and taxon_rank_ordered[full_name_rank_level] == 'species'
                    and mysqlrow):
                if worksheet.cell_value(row, species_author_column):
                    if mysqlrow['author'] != worksheet.cell_value(row, species_author_column):
                        if mysqlrow['author'] is None:
                            tw(row, species_author_column,
                               worksheet.cell_value(row, species_author_column) + " Specify: blank",
                               warning_style)
                        else:
                            ratio = fuzz.ratio(
                                worksheet.cell_value(row, species_author_column).lower(),
                                mysqlrow['author'].lower(),
                            )
                            partial_ratio = fuzz.partial_ratio(
                                worksheet.cell_value(row, species_author_column).lower(),
                                mysqlrow['author'].lower(),
                            )
                            if (ratio >= 58 or partial_ratio >= 69) and rowcount == 1:
                                tw(row, species_author_column, mysqlrow['author'], replace_style)
                            else:
                                if rowcount > 1:
                                    tw(row, species_author_column,
                                       worksheet.cell_value(row, species_author_column) + " Multiple matches found: " + mysqlrow['author'],
                                       warning_style)
                                else:
                                    tw(row, species_author_column,
                                       worksheet.cell_value(row, species_author_column) + " Specify:" + mysqlrow['author'],
                                       warning_style)
                else:
                    tw(row, species_author_column, mysqlrow['author'])

            if rowcount == 1:
                result_status = 'found'
            elif rowcount > 1:
                result_status = 'multiple'
            else:
                result_status = 'not_found'

        yield sse_event('row', {
            'current': row,
            'total': total_rows,
            'name': fullname or '(empty)',
            'status': result_status,
        })

    mydb.close()

    # Save result xls
    out_path = os.path.join(WORK_DIR, f'{job_id}.xls')
    wb.save(out_path)

    # Build preview data: headers + rows with original values overlaid by changes
    preview_headers = headers
    preview_rows = []
    for row in range(1, worksheet.nrows):
        row_data = []
        for col in range(total_cols):
            key = f"{row},{col}"
            if key in cell_values:
                val = cell_values[key]
            elif col < worksheet.ncols:
                val = worksheet.cell_value(row, col)
            else:
                val = ""
            color = cell_colors.get(key, None)
            row_data.append({'v': str(val) if val else '', 'c': color})
        preview_rows.append(row_data)

    # Save preview JSON
    preview_path = os.path.join(WORK_DIR, f'{job_id}_preview.json')
    with open(preview_path, 'w') as f:
        json.dump({'headers': preview_headers, 'rows': preview_rows}, f)

    yield sse_event('done', {'job_id': job_id})


def sse_event(event_type, data):
    return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return json.dumps({'error': 'No file selected.'}), 400, {'Content-Type': 'application/json'}

    f = request.files['file']
    if f.filename == '' or not f.filename.endswith('.xls'):
        return json.dumps({'error': 'Please upload an .xls file.'}), 400, {'Content-Type': 'application/json'}

    job_id = str(uuid.uuid4())[:8]
    file_bytes = f.read()

    upload_path = os.path.join(WORK_DIR, f'{job_id}_input.xls')
    with open(upload_path, 'wb') as out:
        out.write(file_bytes)

    meta_path = os.path.join(WORK_DIR, f'{job_id}_meta.json')
    with open(meta_path, 'w') as out:
        json.dump({'filename': f.filename}, out)

    return json.dumps({'job_id': job_id}), 200, {'Content-Type': 'application/json'}


@app.route('/process/<job_id>')
def process(job_id):
    upload_path = os.path.join(WORK_DIR, f'{job_id}_input.xls')
    if not os.path.exists(upload_path):
        return 'Job not found', 404

    def generate():
        try:
            book = xlrd.open_workbook(upload_path, formatting_info=True)
            yield from process_workbook_streaming(book, job_id)
        except Exception as e:
            yield sse_event('error', {'message': str(e)})

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/preview/<job_id>')
def preview(job_id):
    preview_path = os.path.join(WORK_DIR, f'{job_id}_preview.json')
    if not os.path.exists(preview_path):
        return 'Preview not found', 404
    with open(preview_path) as f:
        data = f.read()
    return data, 200, {'Content-Type': 'application/json'}


@app.route('/download/<job_id>')
def download(job_id):
    out_path = os.path.join(WORK_DIR, f'{job_id}.xls')
    meta_path = os.path.join(WORK_DIR, f'{job_id}_meta.json')
    if not os.path.exists(out_path):
        return 'Result not found', 404

    orig_name = 'output.xls'
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            orig_name = json.load(f).get('filename', 'output.xls')

    download_name = orig_name.replace('.xls', '') + '_taxa_added.xls'
    return send_file(out_path, as_attachment=True, download_name=download_name,
                     mimetype='application/vnd.ms-excel')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
