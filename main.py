"""
Main application file for the regular inspection (정기 점검) management webapp.

This application uses a very small WSGI‑based framework built from Python's
standard library. It does not rely on external web frameworks such as
Flask because those packages are not available in this environment. Instead
we implement our own URL dispatching and form parsing using the built‑in
``wsgiref`` and ``cgi`` modules. HTML templates are rendered with Jinja2.

Key features of the webapp:

* Manage customer companies (고객사) with optional sub‑company names.
* Create recurring inspection tasks for a company. Tasks specify the type
  (사내점검 for in‑house, 방문점검 for on‑site), the method of signature
  (메일 or 방문), and the schedule type. Schedules can be monthly,
  quarterly, or custom. For quarterly or custom schedules you provide a
  comma‑separated list of months (e.g. ``1,4,7,10`` or ``3,6,9,12``).
* Each task contains an ordered checklist. You can specify descriptions
  (instructions, email templates, etc.) and optionally upload a file
  attachment for each checklist item. Uploaded files are stored under the
  ``attachments`` directory and can be downloaded later.
* A dashboard lists tasks that occur in a selected month. Overdue tasks
  (i.e. tasks with incomplete checklist items) are highlighted. You can
  filter by company.
* You can mark individual checklist items as complete from the task detail
  view.

The application persists data in a SQLite database stored next to this
script (``database.db``). On first run the necessary tables are created
automatically.

To run this app locally on your machine you can start it via the
``wsgiref.simple_server`` module. For example:

    python main.py

Then open ``http://localhost:8000`` in your browser. When packaging
into Docker the provided Dockerfile will install the dependencies
(``jinja2``) and run the server on port 8000.
"""

import cgi
import os
import sqlite3
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from jinja2 import Environment, FileSystemLoader, select_autoescape
from wsgiref.simple_server import make_server
from wsgiref.util import FileWrapper


# Base directory for the application files
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database.db"
ATTACHMENTS_DIR = BASE_DIR / "attachments"

# Ensure attachments directory exists
ATTACHMENTS_DIR.mkdir(exist_ok=True)

# Jinja2 environment for rendering templates
env = Environment(
    loader=FileSystemLoader(str(BASE_DIR / "templates")),
    autoescape=select_autoescape(["html", "xml"]),
)


def render_template(template_name: str, context: Dict[str, Any]) -> bytes:
    """Render a Jinja2 template and return it as UTF‑8 encoded bytes."""
    template = env.get_template(template_name)
    html = template.render(context)
    return html.encode("utf-8")


def get_db_connection() -> sqlite3.Connection:
    """Return a new SQLite connection with row factory set to Row."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """
    Initialize the SQLite database with required tables if they don't exist.

    Tables:
    - companies: stores customer (회사) and sub‑customer (상세고객사) information.
    - tasks: stores task definitions including scheduling and metadata.
    - checklist_items: stores individual checklist steps for each task.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    # Create companies table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sub_name TEXT
        )
        """
    )
    # Create tasks table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            task_type TEXT NOT NULL,
            signature_method TEXT NOT NULL,
            schedule_type TEXT NOT NULL,
            schedule_detail TEXT,
            contact_name TEXT,
            contact_phone TEXT,
            contact_email TEXT,
            -- 추가된 상세 고객사 필드 (각 정기점검마다 홈페이지/다이렉트/mydata 등을 저장)
            detail_name TEXT,
            -- 활성화 여부; 1이면 대시보드 및 월별 통계에 표시, 0이면 숨김 처리됨
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_id) REFERENCES companies(id)
        )
        """
    )
    # Create checklist_items table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checklist_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            description TEXT NOT NULL,
            attachment TEXT,
            order_num INTEGER NOT NULL DEFAULT 0,
            completed INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (task_id) REFERENCES tasks(id)
        )
        """
    )

    # Create checklist_completions table (per year-month completion status for each checklist item)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checklist_completions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            completed INTEGER NOT NULL DEFAULT 0,
            UNIQUE(item_id, year, month),
            FOREIGN KEY (item_id) REFERENCES checklist_items(id)
        )
        """
    )
    conn.commit()

    # If the tasks table exists but lacks the detail_name column (for existing DBs), add it dynamically.
    # This allows seamless upgrades without requiring manual migration.
    cur.execute("PRAGMA table_info(tasks)")
    columns = [row[1] for row in cur.fetchall()]
    if "detail_name" not in columns:
        cur.execute("ALTER TABLE tasks ADD COLUMN detail_name TEXT")
        conn.commit()
    # Add active column if missing (for upgrades)
    if "active" not in columns:
        cur.execute("ALTER TABLE tasks ADD COLUMN active INTEGER NOT NULL DEFAULT 1")
        conn.commit()
    conn.close()


def parse_month_list(month_list_str: Optional[str]) -> List[int]:
    """
    Convert a comma‑separated string of month numbers into a list of ints.

    Returns an empty list if month_list_str is None or empty.
    """
    if not month_list_str:
        return []
    try:
        months = [int(m) for m in month_list_str.split(',') if m.strip()]
        return [m for m in months if 1 <= m <= 12]
    except ValueError:
        return []


def get_tasks_for_month(month: int) -> List[sqlite3.Row]:
    """
    Retrieve tasks that are scheduled to occur in the given month.

    A task is considered due in a month if:
    - schedule_type == 'monthly' (every month)
    - schedule_type == 'quarterly' or 'custom', and the month appears in
      schedule_detail as a comma‑separated list of months (e.g., '1,4,7,10')

    Returns a list of rows from the tasks table joined with companies.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT t.*, c.name AS company_name, c.sub_name AS company_sub_name
        FROM tasks t
        JOIN companies c ON t.company_id = c.id
        WHERE t.active = 1
        """
    )
    tasks: List[sqlite3.Row] = []
    for row in cur.fetchall():
        if row['schedule_type'] == 'monthly':
            due_months = list(range(1, 13))
        else:
            due_months = parse_month_list(row['schedule_detail'])
        if month in due_months:
            tasks.append(row)
    conn.close()
    return tasks


def count_incomplete_items(task_id: int) -> int:
    """Count how many checklist items for the given task are incomplete."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM checklist_items WHERE task_id = ? AND completed = 0",
        (task_id,),
    )
    count: int = cur.fetchone()[0]
    conn.close()
    return count


def has_any_completion(task_id: int) -> bool:
    """
    Return True if the given task has any completed checklist item in any year/month.
    A task is considered executed if at least one checklist_completions row exists
    for any of its items with completed=1.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM checklist_completions cc
        JOIN checklist_items ci ON cc.item_id = ci.id
        WHERE ci.task_id = ? AND cc.completed = 1
        """,
        (task_id,),
    )
    count = cur.fetchone()[0]
    conn.close()
    return count > 0


def ensure_completion(item_id: int, year: int, month: int) -> int:
    """
    Ensure a completion record exists for the given item/year/month. If no
    record exists, insert one with completed=0. Returns the current
    completion status (0 or 1).
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT completed FROM checklist_completions WHERE item_id = ? AND year = ? AND month = ?",
        (item_id, year, month),
    )
    row = cur.fetchone()
    if row is not None:
        conn.close()
        return int(row['completed'])
    # Insert default 0
    cur.execute(
        "INSERT INTO checklist_completions (item_id, year, month, completed) VALUES (?, ?, ?, 0)",
        (item_id, year, month),
    )
    conn.commit()
    conn.close()
    return 0


def get_incomplete_count_year_month(task_id: int, year: int, month: int) -> int:
    """
    Return the number of incomplete checklist items for a given task in a
    specific year and month. Uses the checklist_completions table to
    determine per-year completion status.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM checklist_items WHERE task_id = ?",
        (task_id,),
    )
    items = [row['id'] for row in cur.fetchall()]
    conn.close()
    incomplete = 0
    for item_id in items:
        status = ensure_completion(item_id, year, month)
        if status == 0:
            incomplete += 1
    return incomplete


def get_items_with_completion(task_id: int, year: int, month: int) -> List[Dict[str, Any]]:
    """
    Retrieve all checklist items for the task along with their completion
    status (per year/month). Each item in the returned list is a dict
    containing the item's row fields plus a 'completed' field (0/1).
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM checklist_items WHERE task_id = ? ORDER BY order_num",
        (task_id,),
    )
    items = cur.fetchall()
    conn.close()
    result: List[Dict[str, Any]] = []
    for item in items:
        status = ensure_completion(item['id'], year, month)
        result.append({**dict(item), 'completed': status})
    return result


def parse_form_data(environ) -> Tuple[Dict[str, List[str]], Dict[str, List[Any]]]:
    """
    Parse POST form data (both urlencoded and multipart) and return a tuple of
    (form_fields, file_fields).

    This parser extends the basic cgi.FieldStorage handling to better align
    uploaded files with their associated checklist descriptions. When multiple
    checklist items are provided via dynamic form fields (e.g. multiple
    ``item_description`` and ``item_file`` fields), browsers only send file
    inputs for rows where a file was actually selected. Without special
    handling, the file list indices will become misaligned with the
    descriptions. To avoid this, we track the index of each ``item_description``
    as it appears in the POST body and associate subsequent ``item_file``
    fields with the most recently seen description index.

    Returns:
        form_fields: mapping from field name to list of string values.
        file_fields: mapping from field name to list of tuples. For checklist
            files (``item_file``) each tuple is of the form (row_index, FieldStorage).
            Other file inputs are stored with just the FieldStorage.
    """
    form_fields: Dict[str, List[str]] = {}
    file_fields: Dict[str, List[Any]] = {}
    try:
        fs = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ, keep_blank_values=True)
    except Exception:
        # If parsing fails, return empty structures
        return form_fields, file_fields
    if not fs:
        return form_fields, file_fields

    # Track index for checklist descriptions to pair with file inputs
    current_item_index = -1

    if hasattr(fs, 'list') and fs.list:
        for field in fs.list:
            # Checklist description increments the current index
            if field.name == 'item_description':
                current_item_index += 1
                value = field.value if isinstance(field.value, str) else field.value.decode('utf-8')
                form_fields.setdefault('item_description', []).append(value)
                continue

            # File inputs: treat ``item_file`` specially to preserve row index
            if field.filename:
                if field.name == 'item_file':
                    file_fields.setdefault('item_file', []).append((current_item_index, field))
                else:
                    file_fields.setdefault(field.name, []).append(field)
                continue

            # All other text inputs (including hidden inputs and selects)
            value = field.value if isinstance(field.value, str) else field.value.decode('utf-8')
            form_fields.setdefault(field.name, []).append(value)

    return form_fields, file_fields


def get_companies() -> List[sqlite3.Row]:
    """Retrieve all companies from the database sorted by name."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, sub_name FROM companies ORDER BY name")
    companies = cur.fetchall()
    conn.close()
    return companies


def get_all_tasks() -> List[sqlite3.Row]:
    """
    Retrieve all tasks joined with company names. Additionally include a
    "completed_count" column indicating how many checklist completions
    (with completed=1) exist for the task across all years/months.

    The completed_count can be used by the caller to determine whether
    the task has ever been executed.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT t.*, c.name AS company_name, c.sub_name AS company_sub_name,
               (
                   SELECT COUNT(*)
                   FROM checklist_completions cc
                   JOIN checklist_items ci ON cc.item_id = ci.id
                   WHERE ci.task_id = t.id AND cc.completed = 1
               ) AS completed_count
        FROM tasks t
        JOIN companies c ON t.company_id = c.id
        ORDER BY c.name, t.id
        """
    )
    tasks = cur.fetchall()
    conn.close()
    return tasks


def dashboard_handler(environ) -> Tuple[bytes, str]:
    query = urllib.parse.parse_qs(environ.get('QUERY_STRING', ''))

    # month
    try:
        selected_month = int(query.get('month', [datetime.now().month])[0])
    except ValueError:
        selected_month = datetime.now().month

    # year
    try:
        selected_year = int(query.get('year', [datetime.now().year])[0])
    except ValueError:
        selected_year = datetime.now().year

    # company filter
    company_filter = query.get('company', [None])[0]
    company_id: Optional[int] = None
    if company_filter:
        try:
            company_id = int(company_filter)
        except ValueError:
            company_id = None

    # ✅ 방금 추가한 open 파라미터 처리
    open_task_id: Optional[int] = None
    open_param = query.get('open', [None])[0]
    if open_param is not None:
        try:
            open_task_id = int(open_param)
        except ValueError:
            open_task_id = None

    # 이 달의 task 목록
    tasks = get_tasks_for_month(selected_month)
    if company_id is not None:
        tasks = [t for t in tasks if t['company_id'] == company_id]

    annotated_tasks: List[Dict[str, Any]] = []
    for t in tasks:
        incomplete = get_incomplete_count_year_month(t['id'], selected_year, selected_month)
        items_with_status = get_items_with_completion(t['id'], selected_year, selected_month)
        annotated_tasks.append({
            **dict(t),
            'incomplete_count': incomplete,
            'is_overdue': incomplete > 0,
            'items': items_with_status,
        })

    companies = get_companies()
    current_year = datetime.now().year
    years = list(range(current_year - 2, current_year + 3))

    # ✅ 여기에서 월별 통계 생성
    month_stats = build_month_stats(selected_year, company_id)

    body = render_template(
        "index.html",
        {
            'tasks': annotated_tasks,
            'selected_month': selected_month,
            'selected_year': selected_year,
            'current_month': datetime.now().month,
            'current_year': current_year,
            'companies': companies,
            'selected_company': company_id,
            'years': years,
            'open_task_id': open_task_id,
            'month_stats': month_stats,
        },
    )
    return body, 'text/html'



def build_month_stats(year: int, company_id: Optional[int] = None) -> Dict[int, Tuple[int, int]]:
    """
    각 월별로 (완료된 점검 개수, 전체 점검 개수)를 계산해서
    {1: (done, total), 2: (done, total), ... 12: (...)} 형태로 반환.
    """
    stats: Dict[int, Tuple[int, int]] = {}

    for month in range(1, 13):
        # 이 달에 예정된 모든 task 가져오기
        tasks = get_tasks_for_month(month)

        # 회사 필터가 있으면 필터 적용
        if company_id is not None:
            tasks = [t for t in tasks if t['company_id'] == company_id]

        total_tasks = len(tasks)
        done_tasks = 0

        for t in tasks:
            # 이 task의 해당 연도/월 미완료 항목 개수
            incomplete = get_incomplete_count_year_month(t['id'], year, month)
            # 체크리스트 항목이 하나도 없는 경우는 완전 완료로 보지 않도록
            items = get_items_with_completion(t['id'], year, month)
            if total_tasks > 0 and len(items) > 0 and incomplete == 0:
                done_tasks += 1

        stats[month] = (done_tasks, total_tasks)

    return stats


def new_company_get_handler(environ) -> Tuple[bytes, str]:
    """Render the new company form."""
    body = render_template("new_company.html", {})
    return body, 'text/html'


def new_company_post_handler(environ) -> Tuple[bytes, str, str]:
    """
    Process the new company form submission and redirect to the dashboard.

    Returns a tuple (body, content_type, redirect_url). The application will
    perform the redirect.
    """
    form_fields, _ = parse_form_data(environ)
    name_list = form_fields.get('name', [])
    sub_name_list = form_fields.get('sub_name', [])
    name = name_list[0].strip() if name_list else ''
    sub_name = sub_name_list[0].strip() if sub_name_list else None
    if name:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO companies (name, sub_name) VALUES (?, ?)",
            (name, sub_name if sub_name else None),
        )
        conn.commit()
        conn.close()
    # Redirect to dashboard
    return b'', 'text/plain', '/'


def new_task_get_handler(environ) -> Tuple[bytes, str]:
    """Render the new task creation form."""
    companies = get_companies()
    body = render_template("new_task.html", {'companies': companies})
    return body, 'text/html'


def new_task_post_handler(environ) -> Tuple[bytes, str, str]:
    """
    Handle creation of a new task and its checklist items.
    Returns (body, content_type, redirect_url).
    """
    form_fields, file_fields = parse_form_data(environ)
    # Retrieve scalar fields
    company_id = int(form_fields.get('company_id', ['0'])[0])
    task_type = form_fields.get('task_type', ['INHOUSE'])[0]
    signature_method = form_fields.get('signature_method', ['EMAIL'])[0]
    schedule_type = form_fields.get('schedule_type', ['monthly'])[0]
    schedule_detail = form_fields.get('schedule_detail', [''])[0] or None
    contact_name = form_fields.get('contact_name', [''])[0].strip() or None
    contact_phone = form_fields.get('contact_phone', [''])[0].strip() or None
    contact_email = form_fields.get('contact_email', [''])[0].strip() or None
    # Retrieve detail name for this task (e.g. 홈페이지/다이렉트/mydata)
    detail_name = form_fields.get('detail_name', [''])[0].strip() or None
    # Insert task with detail_name
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tasks (
            company_id, task_type, signature_method,
            schedule_type, schedule_detail,
            contact_name, contact_phone, contact_email,
            detail_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            company_id,
            task_type,
            signature_method,
            schedule_type,
            schedule_detail,
            contact_name,
            contact_phone,
            contact_email,
            detail_name,
        ),
    )
    task_id = cur.lastrowid
    # Retrieve list fields; we expect multiple descriptions
    descriptions = form_fields.get('item_description', [])
    # Build a mapping from row index to uploaded file for checklist items
    file_entries = file_fields.get('item_file', [])  # list of (row_index, FieldStorage)
    file_map: Dict[int, Any] = {idx: fs for idx, fs in file_entries}
    for idx, desc in enumerate(descriptions):
        desc_str = desc.strip()
        if not desc_str:
            continue
        attachment_path = None
        # Determine if a file was uploaded for this row index
        file_field = file_map.get(idx)
        # cgi.FieldStorage cannot be evaluated directly in boolean context; check for None explicitly
        if file_field is not None and getattr(file_field, 'filename', None):
            # Build unique filename based on timestamp, index and original name
            unique_name = f"{int(datetime.now().timestamp())}_{idx}_{file_field.filename}"
            save_path = ATTACHMENTS_DIR / unique_name
            # Save file
            with open(save_path, 'wb') as f:
                data = file_field.file.read()
                f.write(data)
            attachment_path = unique_name
        cur.execute(
            """
            INSERT INTO checklist_items (task_id, description, attachment, order_num)
            VALUES (?, ?, ?, ?)
            """,
            (task_id, desc_str, attachment_path, idx),
        )
    conn.commit()
    conn.close()
    # Redirect to dashboard
    return b'', 'text/plain', '/'


def edit_task_get_handler(environ, task_id: int) -> Tuple[bytes, str]:
    """
    Render the edit form for an existing task. The user can modify schedule
    settings, signature and contact information, as well as the detail_name
    (상세 고객사). Checklist items are not editable from this form.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT t.*, c.name AS company_name, c.sub_name AS company_sub_name
        FROM tasks t JOIN companies c ON t.company_id = c.id
        WHERE t.id = ?
        """,
        (task_id,),
    )
    task = cur.fetchone()
    conn.close()
    if not task:
        body = render_template("404.html", {'message': 'Task not found.'})
        return body, 'text/html'
    # Fetch existing checklist items for this task
    conn2 = get_db_connection()
    cur2 = conn2.cursor()
    cur2.execute(
        "SELECT * FROM checklist_items WHERE task_id = ? ORDER BY order_num",
        (task_id,),
    )
    items = [dict(row) for row in cur2.fetchall()]
    conn2.close()
    # Provide context for the form; include items for editing
    body = render_template(
        "edit_task.html",
        {
            'task': task,
            'companies': get_companies(),  # for potential future company change
            'items': items,
        },
    )
    return body, 'text/html'


def edit_task_post_handler(environ, task_id: int) -> Tuple[bytes, str, str]:
    """
    Process updates to an existing task. Only task metadata (schedule,
    signature, contact info, detail_name, task_type, signature_method and
    schedule_detail) are updated. Checklist items are not modified from
    this handler.
    """
    form_fields, file_fields = parse_form_data(environ)
    # Retrieve scalar fields; fallback to current values if not provided
    # Company change is optional; if provided, convert to int otherwise leave unchanged
    company_list = form_fields.get('company_id', [])
    company_id: Optional[int] = None
    if company_list:
        try:
            company_id = int(company_list[0])
        except ValueError:
            company_id = None
    task_type = form_fields.get('task_type', [None])[0]
    signature_method = form_fields.get('signature_method', [None])[0]
    schedule_type = form_fields.get('schedule_type', [None])[0]
    schedule_detail = form_fields.get('schedule_detail', [None])[0] or None
    contact_name = form_fields.get('contact_name', [None])[0]
    contact_phone = form_fields.get('contact_phone', [None])[0]
    contact_email = form_fields.get('contact_email', [None])[0]
    detail_name = form_fields.get('detail_name', [None])[0]

    conn = get_db_connection()
    cur = conn.cursor()
    # Fetch existing task to know current values and company
    cur.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
    existing = cur.fetchone()
    if not existing:
        conn.close()
        return b'', 'text/plain', '/'

    # Update the task record; only set fields that are not None to allow partial updates
    update_values = {
        'company_id': company_id if company_id is not None else existing['company_id'],
        'task_type': task_type or existing['task_type'],
        'signature_method': signature_method or existing['signature_method'],
        'schedule_type': schedule_type or existing['schedule_type'],
        'schedule_detail': schedule_detail if schedule_detail is not None else existing['schedule_detail'],
        'contact_name': (contact_name.strip() if contact_name is not None else existing['contact_name']) or None,
        'contact_phone': (contact_phone.strip() if contact_phone is not None else existing['contact_phone']) or None,
        'contact_email': (contact_email.strip() if contact_email is not None else existing['contact_email']) or None,
        'detail_name': (detail_name.strip() if detail_name is not None else existing['detail_name']) or None,
    }
    cur.execute(
        """
        UPDATE tasks
        SET company_id = ?, task_type = ?, signature_method = ?, schedule_type = ?,
            schedule_detail = ?, contact_name = ?, contact_phone = ?, contact_email = ?,
            detail_name = ?
        WHERE id = ?
        """,
        (
            update_values['company_id'],
            update_values['task_type'],
            update_values['signature_method'],
            update_values['schedule_type'],
            update_values['schedule_detail'],
            update_values['contact_name'],
            update_values['contact_phone'],
            update_values['contact_email'],
            update_values['detail_name'],
            task_id,
        ),
    )

    # === Handle checklist items modifications ===
    # Fetch existing checklist items for this task
    cur.execute("SELECT id, description, attachment, order_num FROM checklist_items WHERE task_id = ? ORDER BY order_num", (task_id,))
    existing_items = cur.fetchall()
    existing_ids = [row['id'] for row in existing_items]

    # Determine which items to delete and which to update
    ids_to_delete: List[int] = []
    items_to_update: List[Tuple[int, str, Optional[str]]] = []  # (id, new_description, new_attachment_path)

    # Build mapping for new attachments for existing items
    existing_file_map: Dict[int, Any] = {}
    for k, file_list in file_fields.items():
        # keys like 'existing_item_file_123'
        if k.startswith('existing_item_file_'):
            try:
                item_id = int(k.split('_')[-1])
            except ValueError:
                continue
            if file_list:
                # use first file only
                existing_file_map[item_id] = file_list[0]

    for row in existing_items:
        item_id = row['id']
        # Check if marked for deletion
        if form_fields.get(f'delete_item_{item_id}', []):
            ids_to_delete.append(item_id)
            continue
        # New description (fallback to existing if not provided)
        new_desc_list = form_fields.get(f'existing_item_description_{item_id}', [])
        new_desc = new_desc_list[0].strip() if new_desc_list else row['description']
        # New file (if provided)
        new_attachment_path = None
        if item_id in existing_file_map:
            fs = existing_file_map[item_id]
            if getattr(fs, 'filename', None):
                unique_name = f"{int(datetime.now().timestamp())}_{item_id}_{fs.filename}"
                save_path = ATTACHMENTS_DIR / unique_name
                with open(save_path, 'wb') as f:
                    f.write(fs.file.read())
                # Remove old attachment file if exists
                old_path = row['attachment']
                if old_path:
                    try:
                        (ATTACHMENTS_DIR / old_path).unlink()
                    except Exception:
                        pass
                new_attachment_path = unique_name
        # Append to update list
        items_to_update.append((item_id, new_desc, new_attachment_path))

    # Delete items marked for deletion
    for del_id in ids_to_delete:
        # delete completions
        cur.execute("DELETE FROM checklist_completions WHERE item_id = ?", (del_id,))
        # delete item
        cur.execute("DELETE FROM checklist_items WHERE id = ?", (del_id,))

    # Update items
    for (item_id, new_desc, new_attachment_path) in items_to_update:
        if new_attachment_path is not None:
            cur.execute(
                "UPDATE checklist_items SET description = ?, attachment = ? WHERE id = ?",
                (new_desc, new_attachment_path, item_id),
            )
        else:
            cur.execute(
                "UPDATE checklist_items SET description = ? WHERE id = ?",
                (new_desc, item_id),
            )

    # Handle new items
    new_descriptions: List[str] = form_fields.get('new_item_description', [])
    new_files = file_fields.get('new_item_file', []) if 'new_item_file' in file_fields else []
    # Determine next order number (append to end)
    cur.execute("SELECT COALESCE(MAX(order_num), -1) FROM checklist_items WHERE task_id = ?", (task_id,))
    max_order = cur.fetchone()[0]
    next_order = max_order + 1
    for idx, desc in enumerate(new_descriptions):
        desc_str = desc.strip()
        if not desc_str:
            continue
        attach_path = None
        if idx < len(new_files):
            fs = new_files[idx]
            if getattr(fs, 'filename', None):
                unique_name = f"{int(datetime.now().timestamp())}_new_{idx}_{fs.filename}"
                save_path = ATTACHMENTS_DIR / unique_name
                with open(save_path, 'wb') as f:
                    f.write(fs.file.read())
                attach_path = unique_name
        cur.execute(
            "INSERT INTO checklist_items (task_id, description, attachment, order_num) VALUES (?, ?, ?, ?)",
            (task_id, desc_str, attach_path, next_order),
        )
        next_order += 1

    # Reorder the remaining items to have sequential order_num starting from 0
    cur.execute(
        "SELECT id FROM checklist_items WHERE task_id = ? ORDER BY order_num",
        (task_id,),
    )
    all_item_ids = [row['id'] for row in cur.fetchall()]
    for new_order, iid in enumerate(all_item_ids):
        cur.execute(
            "UPDATE checklist_items SET order_num = ? WHERE id = ?",
            (new_order, iid),
        )

    conn.commit()
    conn.close()
    # After editing, redirect back to dashboard (current year and month not specified); you may choose to redirect to all tasks
    return b'', 'text/plain', '/'


def delete_task_handler(environ, task_id: int) -> Tuple[bytes, str, str]:
    """
    Delete a task and all of its associated checklist items and completion records.
    Returns a redirect back to the all tasks list.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    # Get item ids for this task
    cur.execute("SELECT id FROM checklist_items WHERE task_id = ?", (task_id,))
    item_ids = [row['id'] for row in cur.fetchall()]
    # Delete completions for these items
    for item_id in item_ids:
        cur.execute("DELETE FROM checklist_completions WHERE item_id = ?", (item_id,))
    # Delete checklist items
    cur.execute("DELETE FROM checklist_items WHERE task_id = ?", (task_id,))
    # Delete task itself
    cur.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    conn.commit()
    conn.close()
    # Redirect to the all tasks list
    return b'', 'text/plain', '/all'


def compose_email_handler(environ) -> Tuple[bytes, str]:
    """
    Compose an aggregated email for a company's tasks in a given month and year.
    The query parameters should include:
      - company: the company ID
      - year: the year
      - month: the month
    The handler gathers all tasks scheduled for the specified month/year for the
    company, computes completion status for each checklist item, and builds a
    summary body. A mailto link is provided with the subject and body pre-filled,
    and recipients extracted from the tasks' contact_email fields.
    """
    query = urllib.parse.parse_qs(environ.get('QUERY_STRING', ''))
    company_param = query.get('company', [None])[0]
    year_param = query.get('year', [str(datetime.now().year)])[0]
    month_param = query.get('month', [str(datetime.now().month)])[0]
    try:
        company_id = int(company_param) if company_param is not None else None
    except ValueError:
        company_id = None
    try:
        year = int(year_param)
    except ValueError:
        year = datetime.now().year
    try:
        month = int(month_param)
    except ValueError:
        month = datetime.now().month
    if company_id is None:
        body = render_template("404.html", {'message': '잘못된 회사 ID입니다.'})
        return body, 'text/html'
    # Get company info
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT name, sub_name FROM companies WHERE id = ?", (company_id,))
    comp = cur.fetchone()
    conn.close()
    if not comp:
        body = render_template("404.html", {'message': '해당 회사를 찾을 수 없습니다.'})
        return body, 'text/html'
    company_name = comp['name']
    # Gather tasks for this company in the month
    tasks_in_month = [t for t in get_tasks_for_month(month) if t['company_id'] == company_id]
    # Build email subject
    subject = f"{company_name} {year}년 {month}월 정기점검 결과"
    body_lines: List[str] = []
    # Collect recipients from tasks
    recipients = []
    for t in tasks_in_month:
        if t['contact_email']:
            recipients.append(t['contact_email'])
        # Header per sub-company/detail
        detail = t['detail_name'] or t['company_sub_name'] or ''
        header = f"- {detail} ({'사내점검' if t['task_type']=='INHOUSE' else '방문점검'})"
        body_lines.append(header)
        # Fetch items with completion status for this year/month
        items = get_items_with_completion(t['id'], year, month)
        for item in items:
            status_mark = '✔' if item['completed'] else '✘'
            line = f"    {status_mark} {item['description']}"
            body_lines.append(line)
        body_lines.append("")
    # Compose body
    body_text = f"{company_name} {year}년 {month}월 정기점검 체크리스트 결과입니다.\n\n" + "\n".join(body_lines)
    # Encode subject and body for mailto
    import urllib.parse as up
    to_param = up.quote(",".join(sorted(set([r for r in recipients if r]))))
    subject_param = up.quote(subject)
    body_param = up.quote(body_text)
    mailto_link = f"mailto:{to_param}?subject={subject_param}&body={body_param}" if to_param else f"mailto:?subject={subject_param}&body={body_param}"
    html_body = render_template(
        "email_compose.html",
        {
            'company_name': company_name,
            'year': year,
            'month': month,
            'subject': subject,
            'body_text': body_text,
            'mailto_link': mailto_link,
            'recipients': sorted(set([r for r in recipients if r])),
        },
    )
    return html_body, 'text/html'


def task_detail_handler(environ, task_id: int) -> Tuple[bytes, str]:
    """
    Render the detail view of a single task. Supports year/month query
    parameters to display per-year completion status. Defaults to current
    year and month.
    """
    # Parse query params for year and month
    query = urllib.parse.parse_qs(environ.get('QUERY_STRING', ''))
    try:
        selected_year = int(query.get('year', [datetime.now().year])[0])
    except ValueError:
        selected_year = datetime.now().year
    try:
        selected_month = int(query.get('month', [datetime.now().month])[0])
    except ValueError:
        selected_month = datetime.now().month
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT t.*, c.name AS company_name, c.sub_name AS company_sub_name
        FROM tasks t JOIN companies c ON t.company_id = c.id
        WHERE t.id = ?
        """,
        (task_id,),
    )
    task = cur.fetchone()
    if not task:
        body = render_template("404.html", {'message': 'Task not found.'})
        return body, 'text/html'
    # Retrieve items with completion status for this year/month
    items = get_items_with_completion(task_id, selected_year, selected_month)
    conn.close()
    body = render_template(
        "view_task.html",
        {
            'task': task,
            'items': items,
            'selected_year': selected_year,
            'selected_month': selected_month,
        },
    )
    return body, 'text/html'


def complete_item_handler(environ, task_id: int, item_id: int) -> Tuple[bytes, str, str]:
    """
    Mark the given checklist item as completed and redirect back to the task.
    Returns (body, content_type, redirect_url).
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE checklist_items SET completed = 1 WHERE id = ? AND task_id = ?",
        (item_id, task_id),
    )
    conn.commit()
    conn.close()
    return b'', 'text/plain', f'/task/{task_id}'


def toggle_item_handler(environ, task_id: int, item_id: int) -> Tuple[bytes, str, str]:
    """
    Toggle the completion status of a checklist item for a specific year and month.
    Expects POST form data containing 'year' and 'month' (and optional 'company', 'open_task').
    Redirects back to the dashboard with the appropriate query parameters.
    """
    form_fields, _ = parse_form_data(environ)

    # year / month
    try:
        year = int(form_fields.get('year', [str(datetime.now().year)])[0])
    except ValueError:
        year = datetime.now().year
    try:
        month = int(form_fields.get('month', [str(datetime.now().month)])[0])
    except ValueError:
        month = datetime.now().month

    # 현재 상태 → 토글
    current_status = ensure_completion(item_id, year, month)
    new_status = 0 if current_status == 1 else 1

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE checklist_completions SET completed = ? WHERE item_id = ? AND year = ? AND month = ?",
        (new_status, item_id, year, month),
    )
    conn.commit()
    conn.close()

    # 리다이렉트 URL 만들기
    query_params = [f"month={month}", f"year={year}"]

    company = form_fields.get('company', [None])[0]
    if company:
        query_params.append(f"company={company}")

    # 어떤 task를 펼쳐 둘지
    open_task = form_fields.get('open_task', [None])[0]
    if open_task:
        query_params.append(f"open={open_task}")

    redirect_url = '/' + ('?' + '&'.join(query_params) if query_params else '')
    return b'', 'text/plain', redirect_url



def update_tasks_visibility_handler(environ) -> Tuple[bytes, str, str]:
    """
    Handle bulk visibility update for tasks from the all tasks page. The form submits
    a list of task_ids and an action (hide or show). If action is 'hide', the
    selected tasks' active field is set to 0 (hidden). If action is 'show', the
    selected tasks' active field is set to 1 (visible). After updating, the
    user is redirected back to the all tasks list.
    """
    form_fields, _ = parse_form_data(environ)
    id_strings = form_fields.get('task_ids', [])
    task_ids: List[int] = []
    for s in id_strings:
        try:
            task_ids.append(int(s))
        except ValueError:
            continue
    action = form_fields.get('action', ['hide'])[0]
    if task_ids:
        conn = get_db_connection()
        cur = conn.cursor()
        placeholders = ','.join(['?'] * len(task_ids))
        if action == 'show':
            cur.execute(f"UPDATE tasks SET active = 1 WHERE id IN ({placeholders})", task_ids)
        else:
            cur.execute(f"UPDATE tasks SET active = 0 WHERE id IN ({placeholders})", task_ids)
        conn.commit()
        conn.close()
    # Redirect to all tasks page
    return b'', 'text/plain', '/all'


def all_tasks_handler(environ) -> Tuple[bytes, str]:
    """
    Render a page with a list of all tasks (no month/year filtering).
    Shows company, contact, task type, signature method, and schedule info.
    """
    tasks = get_all_tasks()
    body = render_template('all_tasks.html', {'tasks': tasks})
    return body, 'text/html'


def attachments_handler(environ, filename: str) -> Tuple[Iterable[bytes], str, int]:
    """
    Serve an uploaded attachment. Returns (body_iterable, content_type, status_code).
    If the file doesn't exist, returns a 404 page.
    """
    file_path = ATTACHMENTS_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        body = render_template("404.html", {'message': 'File not found.'})
        return [body], 'text/html', 404
    # Determine original filename
    parts = filename.split('_', 2)
    original_name = parts[2] if len(parts) > 2 else filename
    # Wrap file in iterator for streaming
    def file_iter():
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                yield chunk
    return file_iter(), original_name, 200


def application(environ, start_response) -> Iterable[bytes]:
    """
    WSGI application entry point. Dispatches requests based on the path and
    method and returns an iterable of bytes.
    """
    # Initialise DB if needed (only once per process)
    if not DB_PATH.exists():
        init_db()
    method = environ['REQUEST_METHOD']
    path = environ.get('PATH_INFO', '/')
    try:
        # Routing for static attachments
        if path.startswith('/attachments/') and method == 'GET':
            filename = path[len('/attachments/'):]
            body_iter, orig_name, status_code = attachments_handler(environ, filename)
            if status_code == 404:
                status = '404 Not Found'
                headers = [
                    ('Content-Type', 'text/html; charset=utf-8'),
                    ('Cache-Control', 'no-cache'),
                ]
                body = next(body_iter)
                start_response(status, headers + [('Content-Length', str(len(body)))])
                return [body]
            # For file download
            status = '200 OK'
            headers = [
                ('Content-Type', 'application/octet-stream'),
                ('Content-Disposition', f'attachment; filename="{orig_name}"'),
            ]
            start_response(status, headers)
            return body_iter
        # Dashboard
        if path == '/' and method == 'GET':
            body, content_type = dashboard_handler(environ)
            status = '200 OK'
            headers = [('Content-Type', content_type + '; charset=utf-8'), ('Content-Length', str(len(body)))]
            start_response(status, headers)
            return [body]
        # New company
        if path == '/company/new':
            if method == 'GET':
                body, content_type = new_company_get_handler(environ)
                status = '200 OK'
                headers = [('Content-Type', content_type + '; charset=utf-8'), ('Content-Length', str(len(body)))]
                start_response(status, headers)
                return [body]
            elif method == 'POST':
                body, content_type, redirect_url = new_company_post_handler(environ)
                status = '303 See Other'
                headers = [('Location', redirect_url)]
                start_response(status, headers)
                return [b'']
        # New task
        if path == '/task/new':
            if method == 'GET':
                body, content_type = new_task_get_handler(environ)
                status = '200 OK'
                headers = [('Content-Type', content_type + '; charset=utf-8'), ('Content-Length', str(len(body)))]
                start_response(status, headers)
                return [body]
            elif method == 'POST':
                body, content_type, redirect_url = new_task_post_handler(environ)
                status = '303 See Other'
                headers = [('Location', redirect_url)]
                start_response(status, headers)
                return [b'']
        # All tasks page
        if path == '/all' and method == 'GET':
            body, content_type = all_tasks_handler(environ)
            status = '200 OK'
            headers = [('Content-Type', content_type + '; charset=utf-8'), ('Content-Length', str(len(body)))]
            start_response(status, headers)
            return [body]
        # Bulk hide tasks (from all tasks list)
        if path == '/tasks/update' and method == 'POST':
            body, content_type, redirect_url = update_tasks_visibility_handler(environ)
            status = '303 See Other'
            headers = [('Location', redirect_url)]
            start_response(status, headers)
            return [b'']
        # Compose email page
        if path == '/email/compose' and method == 'GET':
            body, content_type = compose_email_handler(environ)
            status = '200 OK'
            headers = [('Content-Type', content_type + '; charset=utf-8'), ('Content-Length', str(len(body)))]
            start_response(status, headers)
            return [body]
        # Task detail
        # Match /task/<id>
        if path.startswith('/task/'):
            segments = path.strip('/').split('/')
            if len(segments) == 2 and method == 'GET':
                # /task/{task_id}
                try:
                    task_id = int(segments[1])
                except ValueError:
                    pass
                else:
                    body, content_type = task_detail_handler(environ, task_id)
                    status = '200 OK' if content_type != 'text/html' or not body.startswith(b'<!DOCTYPE') else '200 OK'
                    headers = [('Content-Type', content_type + '; charset=utf-8'), ('Content-Length', str(len(body)))]
                    start_response(status, headers)
                    return [body]
            # /task/{task_id}/complete/{item_id} (POST)
            if len(segments) == 4 and segments[2] == 'complete' and method == 'POST':
                try:
                    task_id = int(segments[1])
                    item_id = int(segments[3])
                except ValueError:
                    pass
                else:
                    body, content_type, redirect_url = complete_item_handler(environ, task_id, item_id)
                    status = '303 See Other'
                    headers = [('Location', redirect_url)]
                    start_response(status, headers)
                    return [b'']
            # /task/{task_id}/toggle/{item_id} (POST) - toggle per year/month
            if len(segments) == 4 and segments[2] == 'toggle' and method == 'POST':
                try:
                    task_id = int(segments[1])
                    item_id = int(segments[3])
                except ValueError:
                    pass
                else:
                    body, content_type, redirect_url = toggle_item_handler(environ, task_id, item_id)
                    status = '303 See Other'
                    headers = [('Location', redirect_url)]
                    start_response(status, headers)
                    return [b'']
            # /task/{task_id}/edit (GET or POST)
            if len(segments) == 3 and segments[2] == 'edit':
                try:
                    task_id = int(segments[1])
                except ValueError:
                    task_id = None
                if task_id is not None:
                    if method == 'GET':
                        body, content_type = edit_task_get_handler(environ, task_id)
                        status = '200 OK'
                        headers = [('Content-Type', content_type + '; charset=utf-8'), ('Content-Length', str(len(body)))]
                        start_response(status, headers)
                        return [body]
                    elif method == 'POST':
                        body, content_type, redirect_url = edit_task_post_handler(environ, task_id)
                        status = '303 See Other'
                        headers = [('Location', redirect_url)]
                        start_response(status, headers)
                        return [b'']
            # /task/{task_id}/delete (POST)
            if len(segments) == 3 and segments[2] == 'delete' and method == 'POST':
                try:
                    task_id = int(segments[1])
                except ValueError:
                    task_id = None
                if task_id is not None:
                    body, content_type, redirect_url = delete_task_handler(environ, task_id)
                    status = '303 See Other'
                    headers = [('Location', redirect_url)]
                    start_response(status, headers)
                    return [b'']
        # Not Found
        body = render_template("404.html", {'message': '페이지를 찾을 수 없습니다.'})
        status = '404 Not Found'
        headers = [('Content-Type', 'text/html; charset=utf-8'), ('Content-Length', str(len(body)))]
        start_response(status, headers)
        return [body]
    except Exception as exc:
        # Internal server error
        error_body = render_template(
            "404.html",
            {
                'message': '서버 오류가 발생했습니다. ' + str(exc),
            },
        )
        status = '500 Internal Server Error'
        headers = [('Content-Type', 'text/html; charset=utf-8'), ('Content-Length', str(len(error_body)))]
        start_response(status, headers)
        return [error_body]


if __name__ == '__main__':
    # When run directly, start a simple development server.
    init_db()
    port = 8000
    print(f"Serving on http://localhost:{port}")
    with make_server('0.0.0.0', port, application) as httpd:
        httpd.serve_forever()