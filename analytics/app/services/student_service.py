from datetime import datetime
from ..routers.common import (
    _get_lms_user_id,
    _get_overall_courses,
    _get_engagement,
    _get_learning_trend,
    _get_missing_tasks,
    _get_due_soon_tasks,
    _get_last_activity_overall,
    _get_continue_learning,
    _fmt_dt,
    _get_course_progress,
    _get_course_avg_grade,
    _get_last_activity_by_course,
    _get_course_tags,
    _get_course_teacher_name,
    _get_course_activities,
    _get_missing_count,
    _get_learning_hours_per_day,
)
from fastapi import HTTPException


def get_student_overall(moodle_user_id: int):
    lms_user_id = _get_lms_user_id(moodle_user_id)
    courses_overall = _get_overall_courses(moodle_user_id)
    avg_grade_map = _get_course_avg_grade(moodle_user_id)
    engagement = _get_engagement(lms_user_id, days=7)
    learning_daily = _get_learning_trend(moodle_user_id, days=7)
    missing_tasks = _get_missing_tasks(moodle_user_id)
    due_soon_tasks = _get_due_soon_tasks(moodle_user_id, days=7)
    last_ts = _get_last_activity_overall(moodle_user_id)
    continue_learning = _get_continue_learning(moodle_user_id)
    if last_ts:
        last_active = _fmt_dt(last_ts)
        days_inactive = (
            datetime.utcnow().date() - datetime.utcfromtimestamp(last_ts).date()
        ).days
    else:
        last_active = None
        days_inactive = None

    avg_grade_all = 0
    if avg_grade_map:
        avg_grade_all = round(sum(avg_grade_map.values()) / len(avg_grade_map), 1)

    total_hours_7d = round(sum(d.get("count", 0) * 0.25 for d in learning_daily), 2)
    active_days_7d = len([d for d in learning_daily if (d.get("count") or 0) > 0])

    return {
        "courses": courses_overall,
        "summary": {
            "totalCourses": courses_overall.get("total", 0),
            "completedCourses": courses_overall.get("completed", 0),
            "completionRate": courses_overall.get("completionRate", 0),
            "avgGradeAll": avg_grade_all,
        },
        "activity": {
            "totalHours7d": total_hours_7d,
            "activeDays7d": active_days_7d,
            "lastActive": last_active,
            "daysInactive": days_inactive,
        },
        "totals": {
            "missingTasks": len(missing_tasks),
            "dueSoonTasks": len(due_soon_tasks),
        },
        "engagement": engagement["counts"],
        "trend": {
            "learningDaily": learning_daily,
            "engagementDaily": engagement["daily"],
        },
        "missingTasks": missing_tasks,
        "dueSoonTasks": due_soon_tasks,
        "continueLearning": continue_learning,
        "lastActive": last_active,
        "daysInactive": days_inactive,
    }


def get_student_per_course(moodle_user_id: int, course_id: int):
    _ = _get_lms_user_id(moodle_user_id)
    course_progress = _get_course_progress(moodle_user_id, course_id=course_id)
    if not course_progress:
        raise HTTPException(status_code=404, detail="Course not found for user")
    avg_grade_map = _get_course_avg_grade(moodle_user_id)
    last_activity_map = _get_last_activity_by_course(moodle_user_id)
    last_ts = last_activity_map.get(course_id)
    item = course_progress[0]
    avg_grade = round(avg_grade_map.get(course_id, 0), 1)
    missing_cnt = _get_missing_count(moodle_user_id, course_id)
    if last_ts:
        last_active = _fmt_dt(last_ts)
        days_inactive = (
            datetime.utcnow().date() - datetime.utcfromtimestamp(last_ts).date()
        ).days
    else:
        last_active = None
        days_inactive = None

    activities = _get_course_activities(moodle_user_id, course_id)
    total_activities = item.get("totalActivities", 0) or 0
    completed_activities = item.get("completedActivities", 0) or 0
    progress_percent = item.get("progressPercent", 0) or 0

    hours_per_day = _get_learning_hours_per_day(moodle_user_id, course_id, days=7)
    time_spent_hours = round(sum(d.get("hours", 0) for d in hours_per_day), 2)
    avg_hours_per_week = round(time_spent_hours / 1, 2)

    return {
        "courseInfo": {
            "courseId": item.get("courseId"),
            "courseName": item.get("courseName"),
            "teacherName": _get_course_teacher_name(course_id),
            "tags": _get_course_tags(course_id),
            "totalActivities": total_activities,
            "completedActivities": completed_activities,
        },
        "progress": {
            "progressPercent": progress_percent,
            "completionRate": progress_percent,
            "completed": item.get("completed", False),
        },
        "avgGradePct": avg_grade,
        "missingTasks": missing_cnt,
        "lastActive": last_active,
        "daysInactive": days_inactive,
        "timeSpentHours": time_spent_hours,
        "learningHoursPerWeek": avg_hours_per_week,
        "hoursPerDay": hours_per_day,
        "progressDonut": {
            "progress": progress_percent,
            "done": max(0, 100 - progress_percent),
        },
        "activities": activities,
    }
