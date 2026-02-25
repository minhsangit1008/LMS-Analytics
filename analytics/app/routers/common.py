from datetime import datetime, timedelta, date
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ..db import LMS_ENGINE, MOODLE_ENGINE
from ..config import MOODLE_DB_PREFIX


def _date_keys(days: int) -> list[str]:
    today = datetime.utcnow().date()
    return [
        (today - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days - 1, -1, -1)
    ]


def _bucketize(rows, date_key: str, value_key: str, days: int) -> list[dict]:
    keys = _date_keys(days)
    mapping = {k: 0 for k in keys}
    for row in rows:
        k = row[date_key]
        if k in mapping:
            mapping[k] += int(row[value_key] or 0)
    return [{"date": _fmt_dt(k), "count": mapping[k]} for k in keys]


def _fmt_dt(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OSError):
            return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("T", " ")).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            return value
    return str(value)


def _safe_fetch(conn, sql: str, params: dict):
    try:
        return conn.execute(text(sql), params).mappings().all()
    except SQLAlchemyError:
        return []


def _in_params(values, prefix: str):
    params = {}
    placeholders = []
    for i, v in enumerate(values):
        key = f"{prefix}{i}"
        placeholders.append(f":{key}")
        params[key] = v
    return ", ".join(placeholders), params


def _get_lms_user_id(moodle_user_id: int) -> str:
    with LMS_ENGINE.connect() as conn:
        row = conn.execute(
            text("SELECT userId FROM account WHERE moodleUserId = :mid LIMIT 1"),
            {"mid": moodle_user_id},
        ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="LMS user not found")
    return row["userId"]


def _get_course_progress(moodle_user_id: int, course_id: int | None = None):
    prefix = MOODLE_DB_PREFIX
    params = {"uid": moodle_user_id}
    course_filter = ""
    if course_id is not None:
        course_filter = " AND c.id = :courseid"
        params["courseid"] = course_id

    with MOODLE_ENGINE.connect() as conn:
        course_rows = _safe_fetch(
            conn,
            f"""
            SELECT
              c.id AS course_id,
              c.fullname AS course_name,
              MAX(CASE WHEN cc.timecompleted IS NOT NULL THEN 1 ELSE 0 END) AS completed,
              SUM(CASE WHEN cm.completion > 0 THEN 1 ELSE 0 END) AS total_activities,
              SUM(CASE WHEN cmc.completionstate IN (1,2) THEN 1 ELSE 0 END) AS completed_activities
            FROM {prefix}course c
            JOIN {prefix}enrol e ON e.courseid = c.id
            JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id
            LEFT JOIN {prefix}course_completions cc ON cc.course = c.id AND cc.userid = :uid
            LEFT JOIN {prefix}course_modules cm ON cm.course = c.id
            LEFT JOIN {prefix}course_modules_completion cmc
              ON cmc.coursemoduleid = cm.id AND cmc.userid = :uid
            WHERE ue.userid = :uid AND c.id != 1{course_filter}
            GROUP BY c.id, c.fullname
            """,
            params,
        )

    result = []
    for item in course_rows:
        total_act = int(item["total_activities"] or 0)
        done_act = int(item["completed_activities"] or 0)
        progress = round((done_act / total_act) * 100) if total_act else 0
        result.append(
            {
                "courseId": int(item["course_id"]),
                "courseName": item["course_name"],
                "completed": bool(item["completed"]),
                "progressPercent": progress,
                "totalActivities": total_act,
                "completedActivities": done_act,
            }
        )
    return result


def _get_continue_learning(moodle_user_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT
              c.id AS course_id,
              c.fullname AS course_name,
              SUM(CASE WHEN cm.completion > 0 THEN 1 ELSE 0 END) AS total_activities,
              SUM(CASE WHEN cmc.completionstate IN (1,2) THEN 1 ELSE 0 END) AS completed_activities,
              MAX(log.timecreated) AS last_ts
            FROM {prefix}course c
            JOIN {prefix}enrol e ON e.courseid = c.id
            JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id AND ue.userid = :uid
            LEFT JOIN {prefix}course_completions cc ON cc.course = c.id AND cc.userid = :uid
            LEFT JOIN {prefix}course_modules cm ON cm.course = c.id
            LEFT JOIN {prefix}course_modules_completion cmc
              ON cmc.coursemoduleid = cm.id AND cmc.userid = :uid
            LEFT JOIN {prefix}logstore_standard_log log
              ON log.courseid = c.id AND log.userid = :uid
            WHERE c.id != 1
              AND (cc.timecompleted IS NULL)
            GROUP BY c.id, c.fullname
            ORDER BY last_ts DESC
            """,
            {"uid": moodle_user_id},
        )

    today = datetime.utcnow().date()
    result = []
    for item in rows:
        total_act = int(item["total_activities"] or 0)
        done_act = int(item["completed_activities"] or 0)
        progress = round((done_act / total_act) * 100) if total_act else 0
        last_ts = item.get("last_ts")
        days_inactive = None
        if last_ts:
            last_date = datetime.utcfromtimestamp(int(last_ts)).date()
            days_inactive = (today - last_date).days
        result.append(
            {
                "courseId": int(item["course_id"]),
                "courseName": item["course_name"],
                "completed": False,
                "progressPercent": progress,
                "totalActivities": total_act,
                "completedActivities": done_act,
                "lastActive": _fmt_dt(last_ts) if last_ts else None,
                "daysInactive": days_inactive,
            }
        )
    return result


def _get_overall_courses(moodle_user_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        total_courses_row = conn.execute(
            text(
                f"""
                SELECT COUNT(DISTINCT c.id) AS total
                FROM {prefix}course c
                JOIN {prefix}enrol e ON e.courseid = c.id
                JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id
                WHERE ue.userid = :uid AND c.id != 1
                """
            ),
            {"uid": moodle_user_id},
        ).mappings().first()

        completed_courses_row = conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS completed
                FROM {prefix}course_completions cc
                WHERE cc.userid = :uid AND cc.timecompleted IS NOT NULL
                """
            ),
            {"uid": moodle_user_id},
        ).mappings().first()

    total_courses = int(total_courses_row["total"] or 0)
    completed_courses = int(completed_courses_row["completed"] or 0)
    completion_rate = (
        round((completed_courses / total_courses) * 100) if total_courses else 0
    )

    return {
        "total": total_courses,
        "completed": completed_courses,
        "completionRate": completion_rate,
    }


def _get_learning_trend(moodle_user_id: int, days: int = 7):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        learning_rows = _safe_fetch(
            conn,
            f"""
            SELECT DATE(FROM_UNIXTIME(cmc.timemodified)) AS d, COUNT(*) AS c
            FROM {prefix}course_modules_completion cmc
            WHERE cmc.userid = :uid
              AND cmc.timemodified >= UNIX_TIMESTAMP(DATE_SUB(UTC_TIMESTAMP(), INTERVAL {days - 1} DAY))
            GROUP BY d
            """,
            {"uid": moodle_user_id},
        )
    return _bucketize(learning_rows, "d", "c", days)


def _get_engagement(lms_user_id: str, days: int = 7):
    with LMS_ENGINE.connect() as conn:
        posts = conn.execute(
            text("SELECT COUNT(*) AS c FROM post WHERE authorId = :uid"),
            {"uid": lms_user_id},
        ).scalar_one()
        comments = conn.execute(
            text("SELECT COUNT(*) AS c FROM comment WHERE authorId = :uid"),
            {"uid": lms_user_id},
        ).scalar_one()
        reactions = conn.execute(
            text("SELECT COUNT(*) AS c FROM reaction WHERE authorId = :uid"),
            {"uid": lms_user_id},
        ).scalar_one()

        engagement_rows = _safe_fetch(
            conn,
            f"""
            SELECT DATE(createdAt) AS d, COUNT(*) AS c
            FROM post
            WHERE authorId = :uid AND createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {days - 1} DAY)
            GROUP BY d
            """,
            {"uid": lms_user_id},
        )

    return {
        "counts": {
            "posts": int(posts),
            "comments": int(comments),
            "reactions": int(reactions),
        },
        "daily": _bucketize(engagement_rows, "d", "c", days),
    }


def _get_missing_tasks(moodle_user_id: int, limit: int = 20):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT
              c.id AS course_id,
              c.fullname AS course_name,
              a.id AS assignment_id,
              a.name AS assignment_name,
              FROM_UNIXTIME(a.duedate) AS due_date
            FROM {prefix}assign a
            JOIN {prefix}course c ON c.id = a.course
            JOIN {prefix}enrol e ON e.courseid = c.id
            JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id AND ue.userid = :uid
            LEFT JOIN {prefix}assign_submission s
              ON s.assignment = a.id AND s.userid = :uid AND s.latest = 1
            WHERE a.duedate > 0
              AND a.duedate < UNIX_TIMESTAMP(UTC_TIMESTAMP())
              AND (s.id IS NULL OR s.status != 'submitted')
            ORDER BY a.duedate ASC
            LIMIT :limit
            """,
            {"uid": moodle_user_id, "limit": limit},
        )

    return [
        {
            "courseId": int(r["course_id"]),
            "courseName": r["course_name"],
            "assignmentId": int(r["assignment_id"]),
            "assignmentName": r["assignment_name"],
            "dueDate": _fmt_dt(r["due_date"]) if r["due_date"] else None,
        }
        for r in rows
    ]


def _get_due_soon_tasks(moodle_user_id: int, days: int = 7, limit: int = 20):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT
              c.id AS course_id,
              c.fullname AS course_name,
              a.id AS assignment_id,
              a.name AS assignment_name,
              FROM_UNIXTIME(a.duedate) AS due_date
            FROM {prefix}assign a
            JOIN {prefix}course c ON c.id = a.course
            JOIN {prefix}enrol e ON e.courseid = c.id
            JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id AND ue.userid = :uid
            LEFT JOIN {prefix}assign_submission s
              ON s.assignment = a.id AND s.userid = :uid AND s.latest = 1
            WHERE a.duedate > 0
              AND a.duedate >= UNIX_TIMESTAMP(UTC_TIMESTAMP())
              AND a.duedate <= UNIX_TIMESTAMP(DATE_ADD(UTC_TIMESTAMP(), INTERVAL :days DAY))
              AND (s.id IS NULL OR s.status != 'submitted')
            ORDER BY a.duedate ASC
            LIMIT :limit
            """,
            {"uid": moodle_user_id, "days": days, "limit": limit},
        )

    return [
        {
            "courseId": int(r["course_id"]),
            "courseName": r["course_name"],
            "assignmentId": int(r["assignment_id"]),
            "assignmentName": r["assignment_name"],
            "dueDate": _fmt_dt(r["due_date"]) if r["due_date"] else None,
        }
        for r in rows
    ]


def _get_course_avg_grade(moodle_user_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT
              gi.courseid AS course_id,
              AVG(gg.finalgrade / NULLIF(gi.grademax, 0)) * 100 AS avg_grade_pct
            FROM {prefix}grade_items gi
            JOIN {prefix}grade_grades gg ON gg.itemid = gi.id
            WHERE gg.userid = :uid
              AND gi.courseid IS NOT NULL
              AND gi.grademax > 0
              AND gg.finalgrade IS NOT NULL
            GROUP BY gi.courseid
            """,
            {"uid": moodle_user_id},
        )
    return {int(r["course_id"]): float(r["avg_grade_pct"] or 0) for r in rows}


def _get_last_activity_by_course(moodle_user_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT
              courseid AS course_id,
              MAX(timecreated) AS last_ts
            FROM {prefix}logstore_standard_log
            WHERE userid = :uid AND courseid IS NOT NULL AND courseid != 0
            GROUP BY courseid
            """,
            {"uid": moodle_user_id},
        )
    result = {}
    for r in rows:
        cid = int(r["course_id"])
        ts = r["last_ts"]
        result[cid] = int(ts) if ts else None
    return result


def _get_last_activity_overall(moodle_user_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT MAX(timecreated) AS last_ts
                FROM {prefix}logstore_standard_log
                WHERE userid = :uid
                """
            ),
            {"uid": moodle_user_id},
        ).mappings().first()
    return int(row["last_ts"]) if row and row["last_ts"] else None


def _get_teacher_courses(teacher_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT c.id AS course_id, c.fullname AS course_name
            FROM {prefix}role_assignments ra
            JOIN {prefix}context ctx ON ctx.id = ra.contextid AND ctx.contextlevel = 50
            JOIN {prefix}course c ON c.id = ctx.instanceid
            WHERE ra.userid = :tid AND ra.roleid IN (3,4)
            GROUP BY c.id, c.fullname
            """,
            {"tid": teacher_id},
        )
    return [{"courseId": int(r["course_id"]), "courseName": r["course_name"]} for r in rows]


def _get_students_in_courses(course_ids: list[int]):
    if not course_ids:
        return []
    prefix = MOODLE_DB_PREFIX
    in_courses, params = _in_params(course_ids, "c")
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT DISTINCT ra.userid AS user_id
            FROM {prefix}role_assignments ra
            JOIN {prefix}context ctx ON ctx.id = ra.contextid AND ctx.contextlevel = 50
            WHERE ra.roleid = 5 AND ctx.instanceid IN ({in_courses})
            """,
            params,
        )
    return [int(r["user_id"]) for r in rows]


def _get_last_activity_by_user(course_ids: list[int], user_ids: list[int]):
    if not course_ids or not user_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u}
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT userid, MAX(timecreated) AS last_ts
            FROM {prefix}logstore_standard_log
            WHERE courseid IN ({in_courses}) AND userid IN ({in_users})
            GROUP BY userid
            """,
            params,
        )
    return {int(r["userid"]): int(r["last_ts"]) for r in rows if r["last_ts"]}


def _get_avg_grade_by_user(course_ids: list[int], user_ids: list[int]):
    if not course_ids or not user_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u}
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT gg.userid AS user_id,
                   AVG(gg.finalgrade / NULLIF(gi.grademax,0)) * 100 AS avg_pct
            FROM {prefix}grade_items gi
            JOIN {prefix}grade_grades gg ON gg.itemid = gi.id
            WHERE gi.courseid IN ({in_courses})
              AND gg.userid IN ({in_users})
              AND gi.grademax > 0
              AND gg.finalgrade IS NOT NULL
            GROUP BY gg.userid
            """,
            params,
        )
    return {int(r["user_id"]): float(r["avg_pct"] or 0) for r in rows}


def _get_missing_by_user(course_ids: list[int], user_ids: list[int]):
    if not course_ids or not user_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u}
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT ue.userid AS user_id, COUNT(*) AS miss_cnt
            FROM {prefix}assign a
            JOIN {prefix}enrol e ON e.courseid = a.course
            JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id
            LEFT JOIN {prefix}assign_submission s
              ON s.assignment = a.id AND s.userid = ue.userid AND s.latest = 1
            WHERE a.course IN ({in_courses})
              AND ue.userid IN ({in_users})
              AND a.duedate > 0
              AND a.duedate < UNIX_TIMESTAMP(UTC_TIMESTAMP())
              AND (s.id IS NULL OR s.status != 'submitted')
            GROUP BY ue.userid
            """,
            params,
        )
    return {int(r["user_id"]): int(r["miss_cnt"] or 0) for r in rows}


def _get_ungraded_submissions_count(course_ids: list[int], user_ids: list[int]):
    if not course_ids or not user_ids:
        return 0
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u}
    with MOODLE_ENGINE.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS c
                FROM {prefix}assign_submission s
                JOIN {prefix}assign a ON a.id = s.assignment
                LEFT JOIN {prefix}grade_items gi
                  ON gi.itemmodule = 'assign' AND gi.iteminstance = a.id
                LEFT JOIN {prefix}grade_grades gg
                  ON gg.itemid = gi.id AND gg.userid = s.userid
                WHERE a.course IN ({in_courses})
                  AND s.userid IN ({in_users})
                  AND s.status = 'submitted'
                  AND a.duedate > 0
                  AND a.duedate < UNIX_TIMESTAMP(UTC_TIMESTAMP())
                  AND gg.id IS NULL
                """
            ),
            params,
        ).mappings().first()
    return int(row["c"] or 0) if row else 0


def _avg_learning_hours(course_ids: list[int], user_ids: list[int]):
    if not course_ids or not user_ids:
        return 0
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u}
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT userid, timecreated
            FROM {prefix}logstore_standard_log
            WHERE courseid IN ({in_courses}) AND userid IN ({in_users})
            ORDER BY userid, timecreated
            """,
            params,
        )
    gaps = []
    last_by_user = {}
    for r in rows:
        uid = int(r["userid"])
        ts = int(r["timecreated"])
        if uid in last_by_user:
            gap_min = (ts - last_by_user[uid]) / 60
            if 1 <= gap_min <= 30:
                gaps.append(gap_min)
        last_by_user[uid] = ts
    if not gaps:
        return 0
    return round(sum(gaps) / len(gaps) / 60, 2)


def _avg_learning_hours_window(course_ids: list[int], user_ids: list[int], start_ts: int, end_ts: int):
    if not course_ids or not user_ids:
        return 0
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u, "start_ts": start_ts, "end_ts": end_ts}
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT userid, timecreated
            FROM {prefix}logstore_standard_log
            WHERE courseid IN ({in_courses})
              AND userid IN ({in_users})
              AND timecreated BETWEEN :start_ts AND :end_ts
            ORDER BY userid, timecreated
            """,
            params,
        )
    gaps = []
    last_by_user = {}
    for r in rows:
        uid = int(r["userid"])
        ts = int(r["timecreated"])
        if uid in last_by_user:
            gap_min = (ts - last_by_user[uid]) / 60
            if 1 <= gap_min <= 30:
                gaps.append(gap_min)
        last_by_user[uid] = ts
    if not gaps:
        return 0
    return round(sum(gaps) / len(gaps) / 60, 2)


def _get_active_students_in_window(course_ids: list[int], start_ts: int, end_ts: int):
    if not course_ids:
        return []
    prefix = MOODLE_DB_PREFIX
    in_courses, params = _in_params(course_ids, "c")
    params.update({"start_ts": start_ts, "end_ts": end_ts})
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT DISTINCT ra.userid AS user_id
            FROM {prefix}logstore_standard_log log
            JOIN {prefix}role_assignments ra ON ra.userid = log.userid
            JOIN {prefix}context ctx ON ctx.id = ra.contextid AND ctx.contextlevel = 50
            WHERE log.courseid IN ({in_courses})
              AND log.timecreated BETWEEN :start_ts AND :end_ts
              AND ctx.instanceid = log.courseid
              AND ra.roleid = 5
            """,
            params,
        )
    return [int(r["user_id"]) for r in rows]


def _get_last_activity_by_user_window(course_ids: list[int], user_ids: list[int], start_ts: int, end_ts: int):
    if not course_ids or not user_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u, "start_ts": start_ts, "end_ts": end_ts}
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT userid, MAX(timecreated) AS last_ts
            FROM {prefix}logstore_standard_log
            WHERE courseid IN ({in_courses})
              AND userid IN ({in_users})
              AND timecreated BETWEEN :start_ts AND :end_ts
            GROUP BY userid
            """,
            params,
        )
    return {int(r["userid"]): int(r["last_ts"]) for r in rows if r["last_ts"]}


def _get_missing_by_user_window(course_ids: list[int], user_ids: list[int], end_ts: int):
    if not course_ids or not user_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u, "end_ts": end_ts}
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT ue.userid AS user_id, COUNT(*) AS miss_cnt
            FROM {prefix}assign a
            JOIN {prefix}enrol e ON e.courseid = a.course
            JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id
            LEFT JOIN {prefix}assign_submission s
              ON s.assignment = a.id AND s.userid = ue.userid AND s.latest = 1
            WHERE a.course IN ({in_courses})
              AND ue.userid IN ({in_users})
              AND a.duedate > 0
              AND a.duedate < :end_ts
              AND (s.id IS NULL OR s.status != 'submitted')
            GROUP BY ue.userid
            """,
            params,
        )
    return {int(r["user_id"]): int(r["miss_cnt"] or 0) for r in rows}


def _get_ungraded_submissions_count_window(course_ids: list[int], user_ids: list[int], start_ts: int, end_ts: int):
    if not course_ids or not user_ids:
        return 0
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u, "start_ts": start_ts, "end_ts": end_ts}
    with MOODLE_ENGINE.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS c
                FROM {prefix}assign_submission s
                JOIN {prefix}assign a ON a.id = s.assignment
                LEFT JOIN {prefix}grade_items gi
                  ON gi.itemmodule = 'assign' AND gi.iteminstance = a.id
                LEFT JOIN {prefix}grade_grades gg
                  ON gg.itemid = gi.id AND gg.userid = s.userid
                WHERE a.course IN ({in_courses})
                  AND s.userid IN ({in_users})
                  AND s.status = 'submitted'
                  AND a.duedate BETWEEN :start_ts AND :end_ts
                  AND gg.id IS NULL
                """
            ),
            params,
        ).mappings().first()
    return int(row["c"] or 0) if row else 0


def _get_completion_rate_window(course_ids: list[int], user_ids: list[int], start_ts: int, end_ts: int):
    if not course_ids or not user_ids:
        return 0
    prefix = MOODLE_DB_PREFIX
    in_courses, params_c = _in_params(course_ids, "c")
    in_users, params_u = _in_params(user_ids, "u")
    params = {**params_c, **params_u, "start_ts": start_ts, "end_ts": end_ts}
    with MOODLE_ENGINE.connect() as conn:
        total_row = conn.execute(
            text(
                f"""
                SELECT SUM(CASE WHEN completion > 0 THEN 1 ELSE 0 END) AS total_act
                FROM {prefix}course_modules
                WHERE course IN ({in_courses})
                """
            ),
            params_c,
        ).mappings().first()
        completed_row = conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS done_act
                FROM {prefix}course_modules_completion cmc
                JOIN {prefix}course_modules cm ON cm.id = cmc.coursemoduleid
                WHERE cm.course IN ({in_courses})
                  AND cmc.userid IN ({in_users})
                  AND cmc.completionstate IN (1,2)
                  AND cmc.timemodified BETWEEN :start_ts AND :end_ts
                """
            ),
            params,
        ).mappings().first()
    total_act = int(total_row["total_act"] or 0) if total_row else 0
    done_act = int(completed_row["done_act"] or 0) if completed_row else 0
    denom = total_act * len(user_ids)
    return round((done_act / denom) * 100, 1) if denom else 0


def _get_avg_grade_by_user_all(user_ids: list[int]):
    if not user_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_users, params_u = _in_params(user_ids, "u")
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT gg.userid AS user_id,
                   AVG(gg.finalgrade / NULLIF(gi.grademax,0)) * 100 AS avg_pct
            FROM {prefix}grade_items gi
            JOIN {prefix}grade_grades gg ON gg.itemid = gi.id
            WHERE gg.userid IN ({in_users})
              AND gi.grademax > 0
              AND gg.finalgrade IS NOT NULL
            GROUP BY gg.userid
            """,
            params_u,
        )
    return {int(r["user_id"]): float(r["avg_pct"] or 0) for r in rows}


def _get_missing_by_user_all(user_ids: list[int]):
    if not user_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_users, params_u = _in_params(user_ids, "u")
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT ue.userid AS user_id, COUNT(*) AS miss_cnt
            FROM {prefix}assign a
            JOIN {prefix}enrol e ON e.courseid = a.course
            JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id
            LEFT JOIN {prefix}assign_submission s
              ON s.assignment = a.id AND s.userid = ue.userid AND s.latest = 1
            WHERE ue.userid IN ({in_users})
              AND a.duedate > 0
              AND a.duedate < UNIX_TIMESTAMP(UTC_TIMESTAMP())
              AND (s.id IS NULL OR s.status != 'submitted')
            GROUP BY ue.userid
            """,
            params_u,
        )
    return {int(r["user_id"]): int(r["miss_cnt"] or 0) for r in rows}


def _get_last_activity_by_user_all(user_ids: list[int]):
    if not user_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_users, params_u = _in_params(user_ids, "u")
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT userid, MAX(timecreated) AS last_ts
            FROM {prefix}logstore_standard_log
            WHERE userid IN ({in_users})
            GROUP BY userid
            """,
            params_u,
        )
    return {int(r["userid"]): int(r["last_ts"]) for r in rows if r["last_ts"]}


def _get_progress_by_user(user_ids: list[int]):
    if not user_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_users, params_u = _in_params(user_ids, "u")
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT ue.userid AS user_id,
                   SUM(CASE WHEN cm.completion > 0 THEN 1 ELSE 0 END) AS total_activities,
                   SUM(CASE WHEN cmc.completionstate IN (1,2) THEN 1 ELSE 0 END) AS completed_activities
            FROM {prefix}user_enrolments ue
            JOIN {prefix}enrol e ON e.id = ue.enrolid
            JOIN {prefix}course_modules cm ON cm.course = e.courseid
            LEFT JOIN {prefix}course_modules_completion cmc
              ON cmc.coursemoduleid = cm.id AND cmc.userid = ue.userid
            WHERE ue.userid IN ({in_users})
            GROUP BY ue.userid
            """,
            params_u,
        )
    result = {}
    for r in rows:
        total_act = int(r["total_activities"] or 0)
        done_act = int(r["completed_activities"] or 0)
        pct = round((done_act / total_act) * 100) if total_act else 0
        result[int(r["user_id"])] = pct
    return result


def _get_mentor_matches(mentor_lms_id: str):
    with LMS_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            """
            SELECT id, studentId, mentorId, ideaId, status, dueDate, createdAt
            FROM studentmentormatch
            WHERE mentorId = :mid
            """,
            {"mid": mentor_lms_id},
        )
    return rows


def _get_lms_users_by_ids(user_ids: list[str]):
    if not user_ids:
        return {}
    in_users, params = _in_params(user_ids, "u")
    with LMS_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"SELECT userId, moodleUserId, username FROM account WHERE userId IN ({in_users})",
            params,
        )
    return {
        r["userId"]: {"moodleUserId": int(r["moodleUserId"]), "username": r["username"]}
        for r in rows
    }


def _get_moodle_users(moodle_ids: list[int]):
    if not moodle_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_ids, params = _in_params(moodle_ids, "m")
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"SELECT id, firstname, lastname FROM {prefix}user WHERE id IN ({in_ids})",
            params,
        )
    return {int(r["id"]): f"{r['firstname']} {r['lastname']}".strip() for r in rows}


def _get_ideas(idea_ids: list[str]):
    if not idea_ids:
        return {}
    in_ids, params = _in_params(idea_ids, "i")
    with LMS_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"SELECT id, name, status FROM businessidea WHERE id IN ({in_ids})",
            params,
        )
    return {r["id"]: {"name": r["name"], "status": r["status"]} for r in rows}


def _get_pitch_scores(idea_ids: list[str]):
    if not idea_ids:
        return {}
    in_ids, params = _in_params(idea_ids, "i")
    with LMS_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT ideaId, status, funding, eventDate
            FROM pitchperfect
            WHERE ideaId IN ({in_ids})
            """,
            params,
        )
    score_map = {}
    status_map = {}
    event_map = {}
    for r in rows:
        status = r["status"]
        base = 50
        if status == "approve":
            base = 80
        elif status == "reject":
            base = 20
        funding = r.get("funding") or 0
        bonus = min(20, funding / 1000) if funding else 0
        score_map[r["ideaId"]] = round(min(100, base + bonus), 1)
        status_map[r["ideaId"]] = status
        event_map[r["ideaId"]] = r.get("eventDate")
    return score_map, status_map, event_map




def _mentor_build_rows(mentor_lms_id: str):
    matches = _get_mentor_matches(mentor_lms_id)
    if not matches:
        return []

    student_ids = [m["studentId"] for m in matches if m.get("studentId")]
    idea_ids = [m["ideaId"] for m in matches if m.get("ideaId")]

    lms_users = _get_lms_users_by_ids(student_ids)
    moodle_ids = [
        info["moodleUserId"]
        for info in lms_users.values()
        if info.get("moodleUserId") is not None
    ]
    moodle_names = _get_moodle_users(moodle_ids)
    progress_map = _get_progress_by_user(moodle_ids)
    avg_grade_map = _get_avg_grade_by_user_all(moodle_ids)
    missing_map = _get_missing_by_user_all(moodle_ids)
    last_activity_map = _get_last_activity_by_user_all(moodle_ids)
    ideas_map = _get_ideas(idea_ids)
    pitch_scores, pitch_statuses, pitch_events = _get_pitch_scores(idea_ids)

    rows = []
    for match in matches:
        student_lms_id = match.get("studentId")
        student_info = lms_users.get(student_lms_id, {})
        moodle_id = student_info.get("moodleUserId")
        student_name = moodle_names.get(moodle_id) if moodle_id else None
        if not student_name:
            student_name = student_info.get("username") or "Unknown"

        progress_pct = progress_map.get(moodle_id, 0) if moodle_id else 0
        avg_pct = avg_grade_map.get(moodle_id, 0) if moodle_id else 0
        missing_cnt = missing_map.get(moodle_id, 0) if moodle_id else 0
        last_ts = last_activity_map.get(moodle_id) if moodle_id else None

        idea = ideas_map.get(match.get("ideaId"), {})
        pitch_score = pitch_scores.get(match.get("ideaId"))
        pitch_status = pitch_statuses.get(match.get("ideaId"))
        pitch_event = pitch_events.get(match.get("ideaId"))

        due_date = match.get("dueDate")
        due_iso = None
        days_to_due = None
        if isinstance(due_date, datetime):
            due_iso = _fmt_dt(due_date)
            days_to_due = (due_date.date() - datetime.utcnow().date()).days
        elif due_date:
            try:
                parsed = datetime.fromisoformat(str(due_date))
                due_iso = _fmt_dt(parsed)
                days_to_due = (parsed.date() - datetime.utcnow().date()).days
            except ValueError:
                due_iso = _fmt_dt(due_date)

        created_at = match.get("createdAt")
        created_iso = None
        if isinstance(created_at, datetime):
            created_iso = _fmt_dt(created_at)
        elif created_at:
            created_iso = _fmt_dt(created_at)

        rows.append(
            {
                "matchId": match.get("id"),
                "student": {
                    "lmsUserId": student_lms_id,
                    "moodleUserId": moodle_id,
                    "name": student_name,
                },
                "idea": {
                    "id": match.get("ideaId"),
                    "name": idea.get("name"),
                    "status": idea.get("status"),
                },
                "status": match.get("status"),
                "dueDate": due_iso,
                "daysToDue": days_to_due,
                "matchCreatedAt": created_iso,
                "progressPct": round(progress_pct, 1),
                "avgGradePct": round(avg_pct, 1),
                "missingTasks": int(missing_cnt),
                "pitchScore": pitch_score,
                "pitchStatus": pitch_status,
                "pitchEventDate": _fmt_dt(pitch_event) if pitch_event else None,
            }
        )

    return rows


def _get_course_name(course_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT fullname FROM {prefix}course WHERE id = :cid
                """
            ),
            {"cid": course_id},
        ).mappings().first()
    return row["fullname"] if row else None


def _get_course_rating(course_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                  AVG(gg.finalgrade / NULLIF(gi.grademax, 0)) * 5 AS avg_rating,
                  COUNT(*) AS num_ratings
                FROM {prefix}grade_items gi
                JOIN {prefix}grade_grades gg ON gg.itemid = gi.id
                WHERE gi.courseid = :cid
                  AND gi.grademax > 0
                  AND gg.finalgrade IS NOT NULL
                """
            ),
            {"cid": course_id},
        ).mappings().first()
    if not row:
        return {"avg_rating": 0, "num_ratings": 0}
    return {
        "avg_rating": round(float(row["avg_rating"] or 0), 1),
        "num_ratings": int(row["num_ratings"] or 0),
    }


def _get_course_tags(course_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT tags FROM {prefix}course WHERE id = :cid
                """
            ),
            {"cid": course_id},
        ).mappings().first()
    if not row:
        return []
    tags = row.get("tags") or ""
    return [t.strip() for t in tags.split(",") if t.strip()]


def _get_course_teacher_name(course_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT u.firstname, u.lastname
                FROM {prefix}role_assignments ra
                JOIN {prefix}context ctx ON ctx.id = ra.contextid AND ctx.contextlevel = 50
                JOIN {prefix}user u ON u.id = ra.userid
                WHERE ctx.instanceid = :cid AND ra.roleid IN (3,4)
                ORDER BY ra.id ASC
                LIMIT 1
                """
            ),
            {"cid": course_id},
        ).mappings().first()
    if not row:
        return None
    return f"{row['firstname']} {row['lastname']}".strip()


def _get_course_activities(moodle_user_id: int, course_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT cm.id AS module_id,
                   cm.completion AS completion_required,
                   cmc.completionstate AS completion_state
            FROM {prefix}course_modules cm
            LEFT JOIN {prefix}course_modules_completion cmc
              ON cmc.coursemoduleid = cm.id AND cmc.userid = :uid
            WHERE cm.course = :cid
            ORDER BY cm.id ASC
            """,
            {"uid": moodle_user_id, "cid": course_id},
        )
    activities = []
    for idx, r in enumerate(rows, start=1):
        completed = False
        if r.get("completion_required"):
            completed = r.get("completion_state") in (1, 2)
        activities.append(
            {
                "activityId": int(r["module_id"]),
                "activityName": f"Activity {idx}",
                "completed": bool(completed),
            }
        )
    return activities


def _get_missing_count(moodle_user_id: int, course_id: int):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS miss_cnt
                FROM {prefix}assign a
                LEFT JOIN {prefix}assign_submission s
                  ON s.assignment = a.id AND s.userid = :uid AND s.latest = 1
                WHERE a.course = :cid
                  AND a.duedate > 0
                  AND a.duedate < UNIX_TIMESTAMP(UTC_TIMESTAMP())
                  AND (s.id IS NULL OR s.status != 'submitted')
                """
            ),
            {"uid": moodle_user_id, "cid": course_id},
        ).mappings().first()
    return int(row["miss_cnt"] or 0) if row else 0


def _get_learning_hours_per_day(moodle_user_id: int, course_id: int, days: int = 7):
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT timecreated
            FROM {prefix}logstore_standard_log
            WHERE userid = :uid AND courseid = :cid
              AND timecreated >= UNIX_TIMESTAMP(DATE_SUB(UTC_TIMESTAMP(), INTERVAL {days - 1} DAY))
            ORDER BY timecreated ASC
            """,
            {"uid": moodle_user_id, "cid": course_id},
        )
    # bucket by day using gaps (1-30 min)
    per_day = {k: 0.0 for k in _date_keys(days)}
    last_ts = None
    for r in rows:
        ts = int(r["timecreated"])
        if last_ts is not None:
            gap_min = (ts - last_ts) / 60
            if 1 <= gap_min <= 30:
                day_key = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                per_day[day_key] = per_day.get(day_key, 0.0) + gap_min / 60
        last_ts = ts
    return [{"date": _fmt_dt(k), "hours": round(per_day[k], 2)} for k in per_day.keys()]


def _get_all_courses():
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"SELECT id, fullname FROM {prefix}course WHERE id != 1",
            {},
        )
    return [{"courseId": int(r["id"]), "courseName": r["fullname"]} for r in rows]


def _get_course_enrol_counts(course_ids: list[int]):
    if not course_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_courses, params = _in_params(course_ids, "c")
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT e.courseid AS course_id, COUNT(DISTINCT ue.userid) AS cnt
            FROM {prefix}enrol e
            JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id
            WHERE e.courseid IN ({in_courses})
            GROUP BY e.courseid
            """,
            params,
        )
    return {int(r["course_id"]): int(r["cnt"] or 0) for r in rows}


def _get_course_missing_counts(course_ids: list[int]):
    if not course_ids:
        return {}
    prefix = MOODLE_DB_PREFIX
    in_courses, params = _in_params(course_ids, "c")
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT a.course AS course_id, COUNT(*) AS miss_cnt
            FROM {prefix}assign a
            JOIN {prefix}enrol e ON e.courseid = a.course
            JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id
            LEFT JOIN {prefix}assign_submission s
              ON s.assignment = a.id AND s.userid = ue.userid AND s.latest = 1
            WHERE a.course IN ({in_courses})
              AND a.duedate > 0
              AND a.duedate < UNIX_TIMESTAMP(UTC_TIMESTAMP())
              AND (s.id IS NULL OR s.status != 'submitted')
            GROUP BY a.course
            """,
            params,
        )
    return {int(r["course_id"]): int(r["miss_cnt"] or 0) for r in rows}


def _get_all_students_moodle_ids():
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = _safe_fetch(
            conn,
            f"""
            SELECT DISTINCT ra.userid AS user_id
            FROM {prefix}role_assignments ra
            JOIN {prefix}context ctx ON ctx.id = ra.contextid AND ctx.contextlevel = 50
            WHERE ra.roleid = 5
            """,
            {},
        )
    return [int(r["user_id"]) for r in rows]


def _get_overdue_assignments_count():
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS cnt
                FROM {prefix}assign a
                JOIN {prefix}enrol e ON e.courseid = a.course
                JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id
                LEFT JOIN {prefix}assign_submission s
                  ON s.assignment = a.id AND s.userid = ue.userid AND s.latest = 1
                WHERE a.duedate > 0
                  AND a.duedate < UNIX_TIMESTAMP(UTC_TIMESTAMP())
                  AND (s.id IS NULL OR s.status != 'submitted')
                """
            )
        ).mappings().first()
    return int(row["cnt"] or 0) if row else 0


def _get_completion_rate_overall():
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        total_row = conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS total
                FROM {prefix}course_completions
                """
            )
        ).mappings().first()
        completed_row = conn.execute(
            text(
                f"""
                SELECT COUNT(*) AS completed
                FROM {prefix}course_completions
                WHERE timecompleted IS NOT NULL
                """
            )
        ).mappings().first()
    total = int(total_row["total"] or 0) if total_row else 0
    completed = int(completed_row["completed"] or 0) if completed_row else 0
    rate = round((completed / total) * 100, 1) if total else 0
    return {"total": total, "completed": completed, "rate": rate}
