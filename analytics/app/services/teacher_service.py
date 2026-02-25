from datetime import datetime, timedelta
from fastapi import HTTPException
from sqlalchemy import text

from ..routers.common import (
    _get_teacher_courses,
    _get_students_in_courses,
    _get_last_activity_by_user,
    _get_avg_grade_by_user,
    _get_missing_by_user,
    _avg_learning_hours,
    _get_ungraded_submissions_count,
    _get_moodle_users,
    _get_progress_by_user,
    _get_lms_user_id,
    _get_course_enrol_counts,
    _fmt_dt,
    _get_course_name,
    _get_course_rating,
    _get_active_students_in_window,
    _get_completion_rate_window,
    _avg_learning_hours_window,
    _get_last_activity_by_user_window,
    _get_missing_by_user_window,
    _get_ungraded_submissions_count_window,
    _get_avg_grade_by_user,
    _in_params,
    _date_keys,
)
from ..config import MOODLE_DB_PREFIX
from ..db import MOODLE_ENGINE, LMS_ENGINE


def get_teacher_overall(teacher_id: int):
    courses = _get_teacher_courses(teacher_id)
    if not courses:
        raise HTTPException(status_code=404, detail="teacher_id not found")

    course_ids = [c["courseId"] for c in courses]
    students = _get_students_in_courses(course_ids)
    total_students = int(len(students))
    total_courses = int(len(course_ids))
    course_names = {c["courseId"]: c["courseName"] for c in courses}

    # My courses list (avg completion + total students)
    enrol_counts = _get_course_enrol_counts(course_ids)
    completion_map = {}
    if course_ids:
        prefix = MOODLE_DB_PREFIX
        in_courses = ",".join(str(i) for i in course_ids)
        with MOODLE_ENGINE.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT cm.course AS course_id,
                           SUM(CASE WHEN cm.completion > 0 THEN 1 ELSE 0 END) AS total_activities,
                           SUM(CASE WHEN cmc.completionstate IN (1,2) THEN 1 ELSE 0 END) AS completed_activities
                    FROM {prefix}course_modules cm
                    LEFT JOIN {prefix}course_modules_completion cmc
                      ON cmc.coursemoduleid = cm.id
                    WHERE cm.course IN ({in_courses})
                    GROUP BY cm.course
                    """
                )
            ).mappings().all()
        for r in rows:
            course_id = int(r["course_id"])
            total_act = int(r["total_activities"] or 0)
            completed_act = int(r["completed_activities"] or 0)
            student_count = int(enrol_counts.get(course_id, 0))
            denom = total_act * student_count
            avg_completion = round((completed_act / denom) * 100, 1) if denom else 0
            completion_map[course_id] = avg_completion

    last_activity = _get_last_activity_by_user(course_ids, students)
    today = datetime.utcnow().date()
    inactive_students_7d = 0
    inactive_students_30d = 0
    for uid in students:
        ts = last_activity.get(uid)
        if not ts:
            inactive_students_7d += 1
            inactive_students_30d += 1
            continue
        last_date = datetime.utcfromtimestamp(ts).date()
        if (today - last_date).days >= 7:
            inactive_students_7d += 1
        if (today - last_date).days >= 30:
            inactive_students_30d += 1

    avg_grade_map = _get_avg_grade_by_user(course_ids, students)
    missing_map = _get_missing_by_user(course_ids, students)
    progress_map = _get_progress_by_user(students)

    # risk metrics removed per requirement

    avg_learning_hours = _avg_learning_hours(course_ids, students)
    ungraded_submissions = _get_ungraded_submissions_count(course_ids, students)

    # Forums managed by teacher (LMS DB)
    forums = []
    forum_ids = []
    try:
        lms_user_id = _get_lms_user_id(teacher_id)
        with LMS_ENGINE.connect() as conn:
            forum_rows = conn.execute(
                text(
                    """
                    SELECT
                        f.id AS forum_id,
                        f.name AS forum_name,
                        COALESCE(fu.role, 'author') AS role,
                        (SELECT COUNT(*) FROM post p WHERE p.forumId = f.id) AS post_count,
                        (SELECT COUNT(*) FROM comment c
                            JOIN post p2 ON p2.id = c.postId
                            WHERE p2.forumId = f.id) AS comment_count,
                        (SELECT COUNT(*) FROM forumuser fu2 WHERE fu2.forumId = f.id) AS member_count,
                        (SELECT MAX(p3.createdAt) FROM post p3 WHERE p3.forumId = f.id) AS last_post_at,
                        (SELECT MAX(c3.createdAt) FROM comment c3
                            JOIN post p4 ON p4.id = c3.postId
                            WHERE p4.forumId = f.id) AS last_comment_at
                    FROM forum f
                    LEFT JOIN forumuser fu
                      ON fu.forumId = f.id AND fu.userId = :uid
                    WHERE f.authorId = :uid
                       OR (fu.userId = :uid AND fu.role IN ('author','admin','inspector'))
                    ORDER BY f.createdAt DESC
                    """
                ),
                {"uid": lms_user_id},
            ).mappings().all()
        for r in forum_rows:
            forum_ids.append(r["forum_id"])
            last_post = r.get("last_post_at")
            last_comment = r.get("last_comment_at")
            last_activity = None
            if last_post and last_comment:
                last_activity = max(last_post, last_comment)
            else:
                last_activity = last_post or last_comment
            forums.append(
                {
                    "forumId": r["forum_id"],
                    "forumName": r["forum_name"],
                    "role": r["role"],
                    "totalPosts": int(r["post_count"] or 0),
                    "totalComments": int(r["comment_count"] or 0),
                    "totalMembers": int(r["member_count"] or 0),
                    "lastActivity": _fmt_dt(last_activity),
                }
            )
    except HTTPException:
        forums = []

    total_forums = int(len(forums))

    forum_activity = {
        "timeline": [],
        "activityBreakdown": {"posts": 0, "comments": 0},
        "topContributors": [],
    }
    if forum_ids:
        in_forums, params = _in_params(forum_ids, "f")
        with LMS_ENGINE.connect() as conn:
            post_rows = conn.execute(
                text(
                    f"""
                    SELECT DATE(createdAt) AS d, COUNT(*) AS c
                    FROM post
                    WHERE forumId IN ({in_forums})
                      AND createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 6 DAY)
                    GROUP BY d
                    """
                ),
                params,
            ).mappings().all()
            comment_rows = conn.execute(
                text(
                    f"""
                    SELECT DATE(c.createdAt) AS d, COUNT(*) AS c
                    FROM comment c
                    JOIN post p ON p.id = c.postId
                    WHERE p.forumId IN ({in_forums})
                      AND c.createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 6 DAY)
                    GROUP BY d
                    """
                ),
                params,
            ).mappings().all()

            total_posts = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) AS c
                    FROM post
                    WHERE forumId IN ({in_forums})
                    """
                ),
                params,
            ).scalar()

            total_comments = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) AS c
                    FROM comment c
                    JOIN post p ON p.id = c.postId
                    WHERE p.forumId IN ({in_forums})
                    """
                ),
                params,
            ).scalar()

            contrib_rows = conn.execute(
                text(
                    f"""
                    SELECT authorId,
                           SUM(posts) AS posts,
                           SUM(comments) AS comments
                    FROM (
                        SELECT authorId, COUNT(*) AS posts, 0 AS comments
                        FROM post
                        WHERE forumId IN ({in_forums})
                        GROUP BY authorId
                        UNION ALL
                        SELECT c.authorId, 0 AS posts, COUNT(*) AS comments
                        FROM comment c
                        JOIN post p ON p.id = c.postId
                        WHERE p.forumId IN ({in_forums})
                        GROUP BY c.authorId
                    ) t
                    GROUP BY authorId
                    ORDER BY (SUM(posts) + SUM(comments)) DESC
                    LIMIT 5
                    """
                ),
                params,
            ).mappings().all()

        timeline_map = {k: {"date": _fmt_dt(k), "posts": 0, "comments": 0} for k in _date_keys(7)}
        for r in post_rows:
            key = r["d"].strftime("%Y-%m-%d") if hasattr(r["d"], "strftime") else str(r["d"])
            if key in timeline_map:
                timeline_map[key]["posts"] = int(r["c"] or 0)
        for r in comment_rows:
            key = r["d"].strftime("%Y-%m-%d") if hasattr(r["d"], "strftime") else str(r["d"])
            if key in timeline_map:
                timeline_map[key]["comments"] = int(r["c"] or 0)

        forum_activity["timeline"] = list(timeline_map.values())
        forum_activity["activityBreakdown"] = {
            "posts": int(total_posts or 0),
            "comments": int(total_comments or 0),
        }

        if contrib_rows:
            user_ids = [r["authorId"] for r in contrib_rows]
            in_users, params_u = _in_params(user_ids, "u")
            with LMS_ENGINE.connect() as conn:
                user_rows = conn.execute(
                    text(f"SELECT userId, username FROM account WHERE userId IN ({in_users})"),
                    params_u,
                ).mappings().all()
            name_map = {r["userId"]: r["username"] for r in user_rows}
            forum_activity["topContributors"] = [
                {
                    "userId": r["authorId"],
                    "name": name_map.get(r["authorId"], r["authorId"]),
                    "posts": int(r["posts"] or 0),
                    "comments": int(r["comments"] or 0),
                    "total": int((r["posts"] or 0) + (r["comments"] or 0)),
                }
                for r in contrib_rows
            ]

    if progress_map:
        completion_rate = round(sum(progress_map.values()) / len(progress_map), 1)
    else:
        completion_rate = 0

    dropout_rate = round(
        (inactive_students_30d / total_students) * 100, 1
    ) if total_students else 0

    # risk student lists removed per requirement

    my_courses = [
        {
            "courseId": cid,
            "courseName": course_names.get(cid),
            "avgCompletion": completion_map.get(cid, 0),
            "totalStudents": int(enrol_counts.get(cid, 0)),
            "image": None,
        }
        for cid in course_ids
    ]

    # KPI comparisons (current vs prev week/month)
    def _calc_delta(current_val, prev_val):
        if not prev_val:
            return 0
        return round(((current_val - prev_val) / prev_val) * 100, 1)

    def _window_metrics(days: int, offset_days: int = 0):
        end = datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=offset_days)
        start = end - timedelta(days=days - 1)
        start_ts = int(start.timestamp())
        end_ts = int(end.timestamp())
        active_students = _get_active_students_in_window(course_ids, start_ts, end_ts)
        total_students_all = total_students
        completion_rate_window = _get_completion_rate_window(course_ids, active_students, start_ts, end_ts)
        avg_hours = _avg_learning_hours_window(course_ids, active_students, start_ts, end_ts)
        dropout_rate_window = (
            round(((total_students_all - len(active_students)) / total_students_all) * 100, 1)
            if total_students_all else 0
        )
        last_activity_window = _get_last_activity_by_user_window(course_ids, students, start_ts, end_ts)
        missing_window = _get_missing_by_user_window(course_ids, students, end_ts)
        ungraded_window = _get_ungraded_submissions_count_window(course_ids, students, start_ts, end_ts)

        return {
            "students": int(len(active_students)),
            "completion": completion_rate_window,
            "avgHours": avg_hours,
            "dropout": dropout_rate_window,
            "ungraded": int(ungraded_window),
        }

    current_metrics = _window_metrics(7, 0)
    prev_week_metrics = _window_metrics(7, 7)
    prev_month_metrics = _window_metrics(30, 30)

    def _trend_series(period_days: int, points: int, label_prefix: str):
        series = []
        end_base = datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=0)
        for i in range(points - 1, -1, -1):
            end = end_base - timedelta(days=period_days * i)
            start = end - timedelta(days=period_days - 1)
            start_ts = int(start.timestamp())
            end_ts = int(end.timestamp())
            active_students = _get_active_students_in_window(course_ids, start_ts, end_ts)
            completion = _get_completion_rate_window(course_ids, students, start_ts, end_ts)
            avg_hours = _avg_learning_hours_window(course_ids, active_students, start_ts, end_ts)
            dropout = (
                round(((total_students - len(active_students)) / total_students) * 100, 1)
                if total_students else 0
            )
            label = f"{label_prefix}{points - i}"
            series.append({
                "label": label,
                "start": _fmt_dt(start),
                "end": _fmt_dt(end),
                "completion": completion,
                "avgHours": avg_hours,
                "dropout": dropout,
            })
        return series

    return {
        "teacher_id": teacher_id,
        "total_students": total_students,
        "total_courses": total_courses,
        "inactive_students_7d": inactive_students_7d,
        "inactive_students_30d": inactive_students_30d,
        "completion_rate": completion_rate,
        "avg_learning_hours_per_week": avg_learning_hours,
        "dropout_rate": dropout_rate,
        "ungraded_submissions": ungraded_submissions,
        "total_forums": total_forums,
        "forums": forums,
        "forumActivity": forum_activity,
        "my_courses": my_courses,
        "kpi_compare": {
            "students": {
                "current": current_metrics["students"],
                "prevWeek": prev_week_metrics["students"],
                "prevMonth": prev_month_metrics["students"],
                "deltaWeekPct": _calc_delta(current_metrics["students"], prev_week_metrics["students"]),
                "deltaMonthPct": _calc_delta(current_metrics["students"], prev_month_metrics["students"]),
            },
            "completion": {
                "current": current_metrics["completion"],
                "prevWeek": prev_week_metrics["completion"],
                "prevMonth": prev_month_metrics["completion"],
                "deltaWeekPct": _calc_delta(current_metrics["completion"], prev_week_metrics["completion"]),
                "deltaMonthPct": _calc_delta(current_metrics["completion"], prev_month_metrics["completion"]),
            },
            "avgHours": {
                "current": current_metrics["avgHours"],
                "prevWeek": prev_week_metrics["avgHours"],
                "prevMonth": prev_month_metrics["avgHours"],
                "deltaWeekPct": _calc_delta(current_metrics["avgHours"], prev_week_metrics["avgHours"]),
                "deltaMonthPct": _calc_delta(current_metrics["avgHours"], prev_month_metrics["avgHours"]),
            },
            "dropout": {
                "current": current_metrics["dropout"],
                "prevWeek": prev_week_metrics["dropout"],
                "prevMonth": prev_month_metrics["dropout"],
                "deltaWeekPct": _calc_delta(current_metrics["dropout"], prev_week_metrics["dropout"]),
                "deltaMonthPct": _calc_delta(current_metrics["dropout"], prev_month_metrics["dropout"]),
            },
            "ungraded": {
                "current": current_metrics["ungraded"],
                "prevWeek": prev_week_metrics["ungraded"],
                "prevMonth": prev_month_metrics["ungraded"],
                "deltaWeekPct": _calc_delta(current_metrics["ungraded"], prev_week_metrics["ungraded"]),
                "deltaMonthPct": _calc_delta(current_metrics["ungraded"], prev_month_metrics["ungraded"]),
            },
        },
        "trends": {
            "weekly": _trend_series(7, 8, "W"),
            "monthly": _trend_series(30, 6, "M"),
            "quarterly": _trend_series(90, 4, "Q"),
            "yearly": _trend_series(365, 3, "Y"),
        },
    }


def get_teacher_per_course(teacher_id: int, course_id: int):
    teacher_courses = _get_teacher_courses(teacher_id)
    course_ids = [c["courseId"] for c in teacher_courses]
    if course_id not in course_ids:
        raise HTTPException(status_code=404, detail="course_id not found for teacher")

    course_name = _get_course_name(course_id)
    students = _get_students_in_courses([course_id])
    total_students = int(len(students))

    avg_grade_map = _get_avg_grade_by_user([course_id], students)
    if avg_grade_map:
        avg_grade_pct = sum(avg_grade_map.values()) / len(avg_grade_map)
    else:
        avg_grade_pct = 0

    missing_map = _get_missing_by_user([course_id], students)
    missing_submissions = int(sum(missing_map.values()))

    names = _get_moodle_users(students)
    prefix = MOODLE_DB_PREFIX
    missing_details = []
    ungraded_details = []
    with MOODLE_ENGINE.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT ue.userid AS user_id,
                       u.firstname, u.lastname,
                       a.id AS assignment_id,
                       a.name AS assignment_name,
                       FROM_UNIXTIME(a.duedate) AS due_date
                FROM {prefix}assign a
                JOIN {prefix}enrol e ON e.courseid = a.course
                JOIN {prefix}user_enrolments ue ON ue.enrolid = e.id
                JOIN {prefix}user u ON u.id = ue.userid
                LEFT JOIN {prefix}assign_submission s
                  ON s.assignment = a.id AND s.userid = ue.userid AND s.latest = 1
                WHERE a.course = :cid
                  AND a.duedate > 0
                  AND a.duedate < UNIX_TIMESTAMP(UTC_TIMESTAMP())
                  AND (s.id IS NULL OR s.status != 'submitted')
                ORDER BY a.duedate ASC
                """
            ),
            {"cid": course_id},
        ).mappings().all()
        for r in rows:
            missing_details.append(
                {
                    "studentId": int(r["user_id"]),
                    "studentName": f"{r['firstname']} {r['lastname']}".strip()
                    if r.get("firstname")
                    else names.get(int(r["user_id"]), "Unknown"),
                    "assignmentId": int(r["assignment_id"]),
                    "assignmentName": r["assignment_name"],
                    "dueDate": _fmt_dt(r["due_date"]) if r["due_date"] else None,
                }
            )

        urows = conn.execute(
            text(
                f"""
                SELECT s.userid AS user_id,
                       u.firstname, u.lastname,
                       a.id AS assignment_id,
                       a.name AS assignment_name,
                       FROM_UNIXTIME(a.duedate) AS due_date
                FROM {prefix}assign_submission s
                JOIN {prefix}assign a ON a.id = s.assignment
                JOIN {prefix}user u ON u.id = s.userid
                JOIN {prefix}grade_items gi
                  ON gi.itemmodule = 'assign' AND gi.iteminstance = a.id
                LEFT JOIN {prefix}grade_grades gg
                  ON gg.itemid = gi.id AND gg.userid = s.userid
                WHERE a.course = :cid
                  AND s.status = 'submitted'
                  AND s.latest = 1
                  AND (gg.id IS NULL OR gg.finalgrade IS NULL)
                ORDER BY s.id DESC
                """
            ),
            {"cid": course_id},
        ).mappings().all()
        for r in urows:
            ungraded_details.append(
                {
                    "studentId": int(r["user_id"]),
                    "studentName": f"{r['firstname']} {r['lastname']}".strip()
                    if r.get("firstname")
                    else names.get(int(r["user_id"]), "Unknown"),
                    "assignmentId": int(r["assignment_id"]),
                    "assignmentName": r["assignment_name"],
                    "dueDate": _fmt_dt(r["due_date"]) if r["due_date"] else None,
                }
            )

    return {
        "course_id": course_id,
        "course_name": course_name,
        "total_students": total_students,
        "avg_grade_pct": round(avg_grade_pct, 1),
        "missing_submissions": missing_submissions,
        "course_rating": _get_course_rating(course_id),
        "missing_per_student": {str(k): v for k, v in missing_map.items()},
        "missing_details": missing_details,
        "ungraded_submissions": ungraded_details,
    }
