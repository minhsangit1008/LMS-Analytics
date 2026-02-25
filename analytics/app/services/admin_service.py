from datetime import datetime
from sqlalchemy import text

from ..routers.common import (
    _get_all_students_moodle_ids,
    _get_overdue_assignments_count,
    _get_all_courses,
    _get_course_enrol_counts,
    _get_course_missing_counts,
    _get_completion_rate_overall,
    _get_progress_by_user,
    _date_keys,
    _fmt_dt,
)
from ..db import LMS_ENGINE, MOODLE_ENGINE
from ..config import MOODLE_DB_PREFIX


def get_admin_overall():
    with LMS_ENGINE.connect() as conn:
        total_users = conn.execute(text("SELECT COUNT(*) AS c FROM account")).scalar()
        role_rows = conn.execute(
            text(
                """
                SELECT r.name AS role, COUNT(*) AS c
                FROM account a
                LEFT JOIN role r ON r.id = a.roleId
                GROUP BY r.name
                """
            )
        ).mappings().all()
        new_users_week = conn.execute(
            text(
                "SELECT COUNT(*) AS c FROM account WHERE createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)"
            )
        ).scalar()
        new_users_month = conn.execute(
            text(
                "SELECT COUNT(*) AS c FROM account WHERE createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)"
            )
        ).scalar()

        moodle_ids_rows = conn.execute(
            text("SELECT moodleUserId FROM account WHERE moodleUserId IS NOT NULL")
        ).mappings().all()

        mentor_load_rows = conn.execute(
            text(
                """
                SELECT mentorId, COUNT(*) AS c
                FROM studentmentormatch
                GROUP BY mentorId
                ORDER BY c DESC
                LIMIT 20
                """
            )
        ).mappings().all()
    moodle_ids = [int(r["moodleUserId"]) for r in moodle_ids_rows if r["moodleUserId"]]

    prefix = MOODLE_DB_PREFIX
    active_7d = 0
    active_30d = 0
    if moodle_ids:
        in_users = ",".join(str(i) for i in moodle_ids)
        with MOODLE_ENGINE.connect() as conn:
            last_rows = conn.execute(
                text(
                    f"""
                    SELECT userid, MAX(timecreated) AS last_ts
                    FROM {prefix}logstore_standard_log
                    WHERE userid IN ({in_users})
                    GROUP BY userid
                    """
                )
            ).mappings().all()
        today = datetime.utcnow().date()
        for r in last_rows:
            ts = r["last_ts"]
            if not ts:
                continue
            last_date = datetime.utcfromtimestamp(int(ts)).date()
            if (today - last_date).days <= 7:
                active_7d += 1
            if (today - last_date).days <= 30:
                active_30d += 1
    inactive_7d = max(0, (len(moodle_ids) - active_7d))
    inactive_30d = max(0, (len(moodle_ids) - active_30d))

    users_trend = []
    if moodle_ids:
        in_users = ",".join(str(i) for i in moodle_ids)
        with MOODLE_ENGINE.connect() as conn:
            trend_rows = conn.execute(
                text(
                    f"""
                    SELECT FROM_UNIXTIME(timecreated, '%Y-%m-%d') AS d,
                           COUNT(DISTINCT userid) AS c
                    FROM {prefix}logstore_standard_log
                    WHERE userid IN ({in_users})
                      AND timecreated >= UNIX_TIMESTAMP(DATE_SUB(UTC_TIMESTAMP(), INTERVAL 6 DAY))
                    GROUP BY d
                    """
                )
            ).mappings().all()
        trend_map = {r["d"]: int(r["c"] or 0) for r in trend_rows}
        for d in _date_keys(7):
            users_trend.append(
                {"date": f"{d} 00:00:00", "activeUsers": int(trend_map.get(d, 0))}
            )

    # Log volume + event mix (7d) based on existing tables (no new table)
    log_volume = []
    event_mix = []
    concurrent_users = []
    with MOODLE_ENGINE.connect() as conn:
        log_rows = conn.execute(
            text(
                f"""
                SELECT FROM_UNIXTIME(timecreated, '%Y-%m-%d') AS d, COUNT(*) AS c
                FROM {prefix}logstore_standard_log
                WHERE timecreated >= UNIX_TIMESTAMP(DATE_SUB(UTC_TIMESTAMP(), INTERVAL 6 DAY))
                GROUP BY d
                """
            )
        ).mappings().all()
        concurrent_rows = conn.execute(
            text(
                f"""
                SELECT FROM_UNIXTIME(FLOOR(timecreated/300)*300) AS t, COUNT(DISTINCT userid) AS c
                FROM {prefix}logstore_standard_log
                WHERE timecreated >= UNIX_TIMESTAMP(DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY))
                GROUP BY t
                ORDER BY t
                """
            )
        ).mappings().all()
        completion_rows = conn.execute(
            text(
                f"""
                SELECT FROM_UNIXTIME(cmc.timemodified, '%Y-%m-%d') AS d, COUNT(*) AS c
                FROM {prefix}course_modules_completion cmc
                WHERE cmc.timemodified >= UNIX_TIMESTAMP(DATE_SUB(UTC_TIMESTAMP(), INTERVAL 6 DAY))
                GROUP BY d
                """
            )
        ).mappings().all()

    with LMS_ENGINE.connect() as conn:
        post_rows = conn.execute(
            text(
                """
                SELECT DATE(createdAt) AS d, COUNT(*) AS c
                FROM post
                WHERE createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 6 DAY)
                GROUP BY d
                """
            )
        ).mappings().all()
        comment_rows = conn.execute(
            text(
                """
                SELECT DATE(createdAt) AS d, COUNT(*) AS c
                FROM comment
                WHERE createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 6 DAY)
                GROUP BY d
                """
            )
        ).mappings().all()

    log_map = {r["d"]: int(r["c"] or 0) for r in log_rows}
    completion_map = {r["d"]: int(r["c"] or 0) for r in completion_rows}
    post_map = {str(r["d"]): int(r["c"] or 0) for r in post_rows}
    comment_map = {str(r["d"]): int(r["c"] or 0) for r in comment_rows}
    for d in _date_keys(7):
        log_volume.append({"date": f"{d} 00:00:00", "logs": log_map.get(d, 0)})
        event_mix.append(
            {
                "date": f"{d} 00:00:00",
                "activity": log_map.get(d, 0),
                "completion": completion_map.get(d, 0),
                "posts": post_map.get(d, 0),
                "comments": comment_map.get(d, 0),
            }
        )

    concurrent_users = [
        {"date": _fmt_dt(r["t"]), "users": int(r["c"] or 0)} for r in concurrent_rows
    ]

    overdue_assignments = _get_overdue_assignments_count()

    with LMS_ENGINE.connect() as conn:
        idea_pending = conn.execute(
            text(
                "SELECT COUNT(*) AS c FROM businessidea WHERE status IN ('submitted','underreview')"
            )
        ).scalar()
        mentor_overdue = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM studentmentormatch
                WHERE dueDate IS NOT NULL
                  AND dueDate < UTC_TIMESTAMP()
                  AND status NOT IN ('approve','reject','completed')
                """
            )
        ).scalar()

    return {
        "users": {
            "total": int(total_users or 0),
            "byRole": {r["role"] or "unknown": int(r["c"] or 0) for r in role_rows},
            "newWeek": int(new_users_week or 0),
            "newMonth": int(new_users_month or 0),
            "active7d": int(active_7d),
            "inactive7d": int(inactive_7d),
            "active30d": int(active_30d),
            "inactive30d": int(inactive_30d),
            "trend7d": users_trend,
        },
        "logs": {
            "volume7d": log_volume,
            "eventMix7d": event_mix,
        },
        "concurrentUsers": concurrent_users,
        "mentorLoadTop": [
            {"mentorId": r["mentorId"], "matchCount": int(r["c"] or 0)}
            for r in mentor_load_rows
        ],
        "alerts": {
            "assignmentOverdue": int(overdue_assignments),
            "ideaPendingReview": int(idea_pending or 0),
            "mentorMatchOverdue": int(mentor_overdue or 0),
        },
    }


def get_admin_learning():
    courses = _get_all_courses()
    course_ids = [c["courseId"] for c in courses]

    total_courses = len(course_ids)
    completion = _get_completion_rate_overall()

    students = _get_all_students_moodle_ids()
    progress_map = _get_progress_by_user(students)
    avg_progress = (
        round(sum(progress_map.values()) / len(progress_map), 1)
        if progress_map
        else 0
    )

    enrol_counts = _get_course_enrol_counts(course_ids)
    top_courses = sorted(
        courses,
        key=lambda c: enrol_counts.get(c["courseId"], 0),
        reverse=True,
    )[:5]
    top_courses = [
        {
            "courseId": c["courseId"],
            "courseName": c["courseName"],
            "enrolCount": int(enrol_counts.get(c["courseId"], 0)),
        }
        for c in top_courses
    ]

    missing_counts = _get_course_missing_counts(course_ids)
    missing_rows = []
    for c in courses:
        cid = c["courseId"]
        enrol = enrol_counts.get(cid, 0)
        miss = missing_counts.get(cid, 0)
        rate = round((miss / enrol) * 100, 1) if enrol else 0
        missing_rows.append(
            {
                "courseId": cid,
                "courseName": c["courseName"],
                "missingCount": int(miss),
                "enrolCount": int(enrol),
                "missingRate": rate,
            }
        )
    top_missing = sorted(missing_rows, key=lambda r: r["missingRate"], reverse=True)[:5]

    completion_trend = []
    prefix = MOODLE_DB_PREFIX
    with MOODLE_ENGINE.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT FROM_UNIXTIME(cmc.timemodified, '%Y-%m-%d') AS d,
                       ROUND(100.0 * SUM(CASE WHEN cmc.completionstate IN (1,2) THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) AS pct
                FROM {prefix}course_modules_completion cmc
                WHERE cmc.timemodified >= UNIX_TIMESTAMP(DATE_SUB(UTC_TIMESTAMP(), INTERVAL 29 DAY))
                GROUP BY d
                """
            )
        ).mappings().all()
    trend_map = {r["d"]: float(r["pct"] or 0) for r in rows}
    for d in _date_keys(30):
        completion_trend.append(
            {"date": f"{d} 00:00:00", "completionPct": trend_map.get(d, 0)}
        )

    return {
        "coursesTotal": total_courses,
        "completionRate": completion["rate"],
        "avgProgressPct": avg_progress,
        "topCoursesByEnroll": top_courses,
        "topMissingCourses": top_missing,
        "completionTrend30d": completion_trend,
    }


def get_admin_engagement():
    with LMS_ENGINE.connect() as conn:
        total_posts = conn.execute(text("SELECT COUNT(*) AS c FROM post")).scalar()
        total_comments = conn.execute(
            text("SELECT COUNT(*) AS c FROM comment")
        ).scalar()
        total_reactions = conn.execute(
            text("SELECT COUNT(*) AS c FROM reaction")
        ).scalar()

        post_rows = conn.execute(
            text("SELECT authorId, COUNT(*) AS c FROM post GROUP BY authorId")
        ).mappings().all()
        comment_rows = conn.execute(
            text("SELECT authorId, COUNT(*) AS c FROM comment GROUP BY authorId")
        ).mappings().all()
        reaction_rows = conn.execute(
            text("SELECT authorId, COUNT(*) AS c FROM reaction GROUP BY authorId")
        ).mappings().all()

        users_rows = conn.execute(
            text("SELECT userId, username, moodleUserId FROM account")
        ).mappings().all()

    user_map = {r["userId"]: r for r in users_rows}
    score = {}
    for r in post_rows:
        score[r["authorId"]] = score.get(r["authorId"], 0) + int(r["c"] or 0)
    for r in comment_rows:
        score[r["authorId"]] = score.get(r["authorId"], 0) + int(r["c"] or 0)
    for r in reaction_rows:
        score[r["authorId"]] = score.get(r["authorId"], 0) + int(r["c"] or 0)

    top_users = sorted(score.items(), key=lambda x: x[1], reverse=True)[:5]
    top_users = [
        {
            "userId": uid,
            "username": user_map.get(uid, {}).get("username"),
            "moodleUserId": user_map.get(uid, {}).get("moodleUserId"),
            "engagementScore": int(cnt),
        }
        for uid, cnt in top_users
    ]

    with LMS_ENGINE.connect() as conn:
        post_rows = conn.execute(
            text(
                """
                SELECT DATE(createdAt) AS d, COUNT(*) AS c
                FROM post
                WHERE createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 29 DAY)
                GROUP BY d
                """
            )
        ).mappings().all()
        comment_rows = conn.execute(
            text(
                """
                SELECT DATE(createdAt) AS d, COUNT(*) AS c
                FROM comment
                WHERE createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 29 DAY)
                GROUP BY d
                """
            )
        ).mappings().all()

    post_map = {str(r["d"]): int(r["c"] or 0) for r in post_rows}
    comment_map = {str(r["d"]): int(r["c"] or 0) for r in comment_rows}
    timeline = []
    for d in _date_keys(30):
        timeline.append(
            {
                "date": f"{d} 00:00:00",
                "posts": post_map.get(d, 0),
                "comments": comment_map.get(d, 0),
            }
        )

    return {
        "totals": {
            "posts": int(total_posts or 0),
            "comments": int(total_comments or 0),
            "reactions": int(total_reactions or 0),
        },
        "topUsers": top_users,
        "timeline30d": timeline,
    }


def get_admin_ideas():
    with LMS_ENGINE.connect() as conn:
        total_ideas = conn.execute(
            text("SELECT COUNT(*) AS c FROM businessidea")
        ).scalar()
        status_rows = conn.execute(
            text(
                """
                SELECT status, COUNT(*) AS c
                FROM businessidea
                GROUP BY status
                """
            )
        ).mappings().all()

        match_total = conn.execute(
            text("SELECT COUNT(*) AS c FROM studentmentormatch")
        ).scalar()
        match_overdue = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM studentmentormatch
                WHERE dueDate IS NOT NULL
                  AND dueDate < UTC_TIMESTAMP()
                  AND status NOT IN ('approve','reject','completed')
                """
            )
        ).scalar()
        match_upcoming = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM studentmentormatch
                WHERE dueDate IS NOT NULL
                  AND dueDate >= UTC_TIMESTAMP()
                  AND dueDate <= DATE_ADD(UTC_TIMESTAMP(), INTERVAL 7 DAY)
                """
            )
        ).scalar()

        pitch_total = conn.execute(
            text("SELECT COUNT(*) AS c FROM pitchperfect")
        ).scalar()
        funding_total = conn.execute(
            text("SELECT SUM(funding) AS s FROM pitchperfect")
        ).scalar()

        idea_rows = conn.execute(
            text(
                """
                SELECT DATE(createdAt) AS d, COUNT(*) AS c
                FROM businessidea
                WHERE createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 29 DAY)
                GROUP BY d
                """
            )
        ).mappings().all()

        pitch_rows = conn.execute(
            text(
                """
                SELECT DATE(createdAt) AS d,
                       COUNT(*) AS pitch_count,
                       SUM(funding) AS funding_total
                FROM pitchperfect
                WHERE createdAt >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 29 DAY)
                GROUP BY d
                """
            )
        ).mappings().all()

    idea_map = {str(r["d"]): int(r["c"] or 0) for r in idea_rows}
    pitch_map = {
        str(r["d"]): {
            "pitchCount": int(r["pitch_count"] or 0),
            "fundingTotal": float(r["funding_total"] or 0),
        }
        for r in pitch_rows
    }
    ideas_trend = []
    pitch_trend = []
    for d in _date_keys(30):
        ideas_trend.append({"date": f"{d} 00:00:00", "ideas": idea_map.get(d, 0)})
        row = pitch_map.get(d, {"pitchCount": 0, "fundingTotal": 0})
        pitch_trend.append(
            {
                "date": f"{d} 00:00:00",
                "pitchCount": row["pitchCount"],
                "fundingTotal": row["fundingTotal"],
            }
        )

    return {
        "ideasTotal": int(total_ideas or 0),
        "ideasByStatus": {r["status"]: int(r["c"] or 0) for r in status_rows},
        "mentorMatch": {
            "total": int(match_total or 0),
            "overdue": int(match_overdue or 0),
            "upcoming7d": int(match_upcoming or 0),
        },
        "pitch": {
            "total": int(pitch_total or 0),
            "fundingTotal": float(funding_total or 0),
        },
        "ideasTrend30d": ideas_trend,
        "pitchTrend30d": pitch_trend,
    }
