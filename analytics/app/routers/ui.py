from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from pathlib import Path

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def demo_ui():
    return """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Analytics Demo</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; background: #f7f7f7; }
      .card { background: white; padding: 16px; margin-bottom: 16px; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
      h1 { margin-top: 0; }
      label { display: inline-block; width: 160px; }
      input { padding: 6px 8px; margin: 4px 0; }
      button { padding: 8px 12px; margin-right: 8px; cursor: pointer; }
      pre { background: #111; color: #0f0; padding: 12px; border-radius: 6px; overflow: auto; }
    </style>
  </head>
  <body>
    <h1>Analytics Demo</h1>
    <div class="card">
      <div>
        <label>Moodle User ID</label>
        <input id="studentId" type="number" value="2" />
      </div>
      <div>
        <label>Course ID</label>
        <input id="courseId" type="number" value="101" />
      </div>
      <div style="margin-top:8px;">
        <button onclick="loadStudentOverall()">Student Overall</button>
        <button onclick="loadStudentPerCourse()">Student Per-course</button>
      </div>
    </div>

    <div class="card">
      <div>
        <label>Teacher ID</label>
        <input id="teacherId" type="number" value="10" />
      </div>
      <div>
        <label>Course ID</label>
        <input id="teacherCourseId" type="number" value="101" />
      </div>
      <div style="margin-top:8px;">
        <button onclick="loadTeacherOverall()">Teacher Overall</button>
        <button onclick="loadTeacherPerCourse()">Teacher Per-course</button>
      </div>
    </div>

    <div class="card">
      <div>
        <label>Mentor ID</label>
        <input id="mentorId" type="number" value="10" />
      </div>
      <div style="margin-top:8px;">
        <button onclick="loadMentorOverall()">Mentor Overall</button>
        <button onclick="loadMentorPerMentee()">Mentor Per-mentee</button>
      </div>
    </div>

    <div class="card">
      <h3>Response</h3>
      <pre id="output">{}</pre>
    </div>

    <script>
      async function callApi(path) {
        const res = await fetch(path);
        const text = await res.text();
        const out = document.getElementById('output');
        try {
          out.textContent = JSON.stringify(JSON.parse(text), null, 2);
        } catch (e) {
          out.textContent = text;
        }
      }
      function loadStudentOverall() {
        const sid = document.getElementById('studentId').value;
        callApi(`/analytics/student-overall?moodle_user_id=${sid}`);
      }
      function loadStudentPerCourse() {
        const sid = document.getElementById('studentId').value;
        const cid = document.getElementById('courseId').value;
        callApi(`/analytics/student-per-course?moodle_user_id=${sid}&course_id=${cid}`);
      }
      function loadTeacherOverall() {
        const tid = document.getElementById('teacherId').value;
        callApi(`/analytics/teacher-overall?teacher_id=${tid}`);
      }
      function loadTeacherPerCourse() {
        const tid = document.getElementById('teacherId').value;
        const cid = document.getElementById('teacherCourseId').value;
        callApi(`/analytics/teacher-per-course?teacher_id=${tid}&course_id=${cid}`);
      }
      function loadMentorOverall() {
        const mid = document.getElementById('mentorId').value;
        callApi(`/analytics/mentor-overall?mentor_id=${mid}`);
      }
      function loadMentorPerMentee() {
        const mid = document.getElementById('mentorId').value;
        callApi(`/analytics/mentor-per-mentee?mentor_id=${mid}`);
      }
    </script>
  </body>
</html>
"""


@router.get("/ui/teacher-overall", response_class=HTMLResponse)
def teacher_overall_ui():
    base = Path(__file__).resolve().parents[2]
    html_path = base / "mobile_teacher_overall.html"
    if not html_path.exists():
        return "<h3>teacher UI not found</h3>"
    return html_path.read_text(encoding="utf-8")


@router.get("/ui/student-per-course", response_class=HTMLResponse)
def student_per_course_ui():
    base = Path(__file__).resolve().parents[2]
    html_path = base / "mobile_student_per_course.html"
    if not html_path.exists():
        return "<h3>student per-course UI not found</h3>"
    return html_path.read_text(encoding="utf-8")


@router.get("/ui/student-overall", response_class=HTMLResponse)
def student_overall_ui():
    base = Path(__file__).resolve().parents[2]
    html_path = base / "mobile_student_overall.html"
    if not html_path.exists():
        return "<h3>student overall UI not found</h3>"
    return html_path.read_text(encoding="utf-8")


@router.get("/ui/teacher-per-course", response_class=HTMLResponse)
def teacher_per_course_ui():
    base = Path(__file__).resolve().parents[2]
    html_path = base / "mobile_teacher_per_course.html"
    if not html_path.exists():
        return "<h3>teacher per-course UI not found</h3>"
    return html_path.read_text(encoding="utf-8")


@router.get("/ui/admin-overall", response_class=HTMLResponse)
def admin_overall_ui():
    base = Path(__file__).resolve().parents[2]
    html_path = base / "mobile_admin_overall.html"
    if not html_path.exists():
        return "<h3>admin UI not found</h3>"
    return html_path.read_text(encoding="utf-8")
