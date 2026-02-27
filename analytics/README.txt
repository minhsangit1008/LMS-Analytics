Analytics Service (FastAPI)

1) Scope
- Service path: analytics/
- Runtime: FastAPI + Uvicorn
- Data source: real MySQL databases (LMS + Moodle)
- No fake/seed data instructions in this document

2) Requirements
- Python 3.10+
- MySQL access to 2 databases:
  - LMS database (default name in config: lms)
  - Moodle database (default name in config: moodle)

3) Setup
Run in analytics/:

Windows PowerShell:
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

macOS/Linux:
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

4) Configuration
- Copy .env.example to .env
- Set DB connection values for your real environment

Environment variables:
LMS_DB_HOST
LMS_DB_PORT
LMS_DB_NAME
LMS_DB_USER
LMS_DB_PASS
MOODLE_DB_HOST
MOODLE_DB_PORT
MOODLE_DB_NAME
MOODLE_DB_USER
MOODLE_DB_PASS
MOODLE_DB_PREFIX

5) Run
Run in analytics/:
uvicorn app.main:app --reload --host 127.0.0.1 --port 8001

6) API Endpoints
Base prefix: /analytics

Student:
- GET /analytics/student-overall?moodle_user_id={int}
- GET /analytics/student-per-course?moodle_user_id={int}&course_id={int}

Teacher:
- GET /analytics/teacher-overall?teacher_id={int}
- GET /analytics/teacher-per-course?teacher_id={int}&course_id={int}

Mentor:
- GET /analytics/mentor-overall?mentor_id={int}
- GET /analytics/mentor-per-idea?mentor_id={int}[&idea_id={str}]

Admin:
- GET /analytics/admin-overall
- GET /analytics/admin-learning
- GET /analytics/admin-engagement
- GET /analytics/admin-ideas

Investor:
- GET /analytics/investor-overall?investor_id={str}
- GET /analytics/investor-invested-ideas?investor_id={str}
- GET /analytics/investor-per-idea?investor_id={str}[&idea_id={str}][&mentor_id={str}][&student_id={str}]

7) Quick check
Sample requests:
curl "http://127.0.0.1:8001/analytics/student-overall?moodle_user_id=20"
curl "http://127.0.0.1:8001/analytics/teacher-overall?teacher_id=10"
curl "http://127.0.0.1:8001/analytics/admin-overall"
