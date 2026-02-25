# Analytics Service (FastAPI)

This service runs in `analytics/` and reads DBs directly (LMS + Moodle).

## Setup
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Config
Copy `.env.example` to `.env` and fill DB settings.

## Run
```bash
uvicorn app.main:app --reload --host 127.0.0.1 --port 8001
```

## Response format
- JSON is pretty-printed by default (easy to read in browser/Postman).
- All datetime fields are formatted as `YYYY-MM-DD HH:MM:SS`.

## Data sources (e-learning context)
This analytics service reads two databases:
- **Moodle DB (learning data)**: courses, enrollments, grades, assignments, activity logs.
- **LMS DB (app data)**: users/roles, forums, ideas, mentor matches, pitch records.

Key Moodle tables used:
- `mdl_course`, `mdl_enrol`, `mdl_user_enrolments`
- `mdl_course_completions`, `mdl_course_modules`, `mdl_course_modules_completion`
- `mdl_grade_items`, `mdl_grade_grades`
- `mdl_assign`, `mdl_assign_submission`
- `mdl_logstore_standard_log`
- `mdl_role_assignments`, `mdl_context`, `mdl_user`

Key LMS tables used:
- `account`, `role`
- `forum`, `forumuser`, `post`, `comment`, `reaction`
- `businessidea`, `studentmentormatch`, `pitchperfect`
- `userworkflowinstance`

## KPI reference (why + source + formula)
All KPIs are designed for an e-learning app. Each KPI answers a specific role question:
- **Student**: “Am I progressing? What should I do next?”
- **Teacher**: “Are my learners engaged? Which courses need attention?”
- **Mentor**: “Which ideas/mentees need support and review?”
- **Admin**: “Is the platform healthy? Which areas need action?”
- **Investor**: “Which ideas are promising to invest?”

### Student Overall — `GET /analytics/student-overall`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `courses.total` | Know how many courses the student is enrolled in | `mdl_course`, `mdl_enrol`, `mdl_user_enrolments` | `COUNT(DISTINCT course)` for the user |
| `courses.completed` | Measure finished courses | `mdl_course_completions` | `COUNT(*) WHERE timecompleted IS NOT NULL` |
| `courses.completionRate` | Overall completion % | `mdl_course_completions` + enrol tables | `completed / total * 100` |
| `summary.avgGradeAll` | Academic performance snapshot | `mdl_grade_items`, `mdl_grade_grades` | Avg of per-course grade % |
| `activity.totalHours7d` | Weekly learning effort | `mdl_logstore_standard_log` | Session proxy: sum of 1–30 min gaps; scaled to hours |
| `activity.activeDays7d` | Consistency of learning | `mdl_logstore_standard_log` | Count of days with any activity (7d) |
| `activity.lastActive` | Recency of study | `mdl_logstore_standard_log` | `MAX(timecreated)` formatted |
| `activity.daysInactive` | Detect inactivity gap | `mdl_logstore_standard_log` | `Today - lastActive` in days |
| `engagement.posts/comments/reactions` | Community activity | `post`, `comment`, `reaction` | Count by authorId (all-time) |
| `trend.learningDaily` | Learning trend (7d) | `mdl_course_modules_completion` | Daily count of completion updates |
| `trend.engagementDaily` | Engagement trend (7d) | `post` | Daily posts by user |
| `missingTasks[]` | Overdue tasks to act on | `mdl_assign`, `mdl_assign_submission` | Assignments past due without submitted record |
| `dueSoonTasks[]` | Next tasks to plan | `mdl_assign`, `mdl_assign_submission` | Due in next 7 days and not submitted |
| `continueLearning[]` | Courses in progress | `mdl_course_modules_completion`, `mdl_course_completions`, `mdl_logstore_standard_log` | Incomplete courses sorted by last activity |

### Student Per Course — `GET /analytics/student-per-course`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `courseInfo.*` | Course identity and meta | `mdl_course`, `mdl_role_assignments`, `mdl_context`, `mdl_user` | Course name + teacher name |
| `courseInfo.tags[]` | Domain/topic filtering | `mdl_course.tags` | Split CSV tags |
| `courseInfo.totalActivities` | Scope of course work | `mdl_course_modules` | Count of activities with completion enabled |
| `courseInfo.completedActivities` | Completed work | `mdl_course_modules_completion` | Count of completionstate in (1,2) |
| `progress.progressPercent` | Course progress % | `mdl_course_modules_completion` | `completed / total * 100` |
| `avgGradePct` | Course grade % | `mdl_grade_items`, `mdl_grade_grades` | Average grade percent for course |
| `missingTasks` | Overdue items in this course | `mdl_assign`, `mdl_assign_submission` | Count of overdue non-submitted |
| `timeSpentHours` | Total time in 7d | `mdl_logstore_standard_log` | Session proxy, summed |
| `learningHoursPerWeek` | Weekly study effort | `mdl_logstore_standard_log` | Same as `timeSpentHours` |
| `hoursPerDay[]` | Study distribution | `mdl_logstore_standard_log` | Hours per day (last 7 days) |
| `activities[]` | Activity checklist | `mdl_course_modules`, `mdl_course_modules_completion` | Each activity marked completed or not |

### Teacher Overall — `GET /analytics/teacher-overall`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `total_students` | Class size | `mdl_role_assignments`, `mdl_context` | Count distinct students across teacher courses |
| `total_courses` | Teaching load | `mdl_role_assignments`, `mdl_context`, `mdl_course` | Courses where role is teacher |
| `inactive_students_7d/30d` | Engagement risk | `mdl_logstore_standard_log` | Students with no activity in last 7/30 days |
| `completion_rate` | Overall learning progress | `mdl_course_modules_completion` | Avg progress % across students |
| `avg_learning_hours_per_week` | Learning effort | `mdl_logstore_standard_log` | Session proxy per week |
| `dropout_rate` | Potential dropouts | `mdl_logstore_standard_log` | `inactive_30d / total_students * 100` |
| `ungraded_submissions` | Teacher backlog | `mdl_assign_submission`, `mdl_grade_items`, `mdl_grade_grades` | Count submitted but not graded |
| `total_forums` | Managed communities | `forum`, `forumuser` | Forums authored/managed by teacher |
| `forums[]` | Forum health | `forum`, `post`, `comment` | Posts/comments/member count per forum |
| `forumActivity.timeline[]` | Discussion trend | `post`, `comment` | Daily posts/comments (7d) |
| `forumActivity.activityBreakdown` | Balance of activities | `post`, `comment` | Total posts vs comments |
| `forumActivity.topContributors[]` | Top engaged users | `post`, `comment`, `account` | Rank by post+comment count |
| `my_courses[]` | Course snapshots | `mdl_course`, `mdl_course_modules_completion` | Avg completion + enrol count per course |
| `kpi_compare.*` | Trend vs previous week/month | `mdl_logstore_standard_log` + completions | Compare current vs prev windows |
| `trends.*` | Time series for charts | `mdl_logstore_standard_log`, completions | Weekly/monthly/quarterly/yearly series |

### Teacher Per Course — `GET /analytics/teacher-per-course`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `total_students` | Course size | `mdl_enrol`, `mdl_user_enrolments` | Count students enrolled |
| `avg_grade_pct` | Course performance | `mdl_grade_items`, `mdl_grade_grades` | Avg grade % for course |
| `missing_submissions` | Overdue workload | `mdl_assign`, `mdl_assign_submission` | Count overdue non-submitted |
| `course_rating` | Course quality proxy | `mdl_grade_items`, `mdl_grade_grades` | Avg grade scaled to 5 |
| `missing_details[]` | Which students missed what | `mdl_assign`, `mdl_assign_submission`, `mdl_user` | List of overdue assignments per student |
| `ungraded_submissions[]` | Which submissions need grading | `mdl_assign_submission`, `mdl_grade_grades` | Submitted but finalgrade null |

### Mentor Overall — `GET /analytics/mentor-overall`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `total_ideas` | Mentoring scope | `studentmentormatch`, `businessidea` | Distinct idea IDs |
| `total_mentees` | Students mentored | `studentmentormatch`, `account` | Distinct student IDs |
| `avg_progress_pct` | Overall mentee progress | `userworkflowinstance` + Moodle progress | Average progress % |
| `avg_grade_pct` | Learning performance | `mdl_grade_items`, `mdl_grade_grades` | Avg grade % across mentees |
| `overdue_actions` | Past-due mentoring | `studentmentormatch` | dueDate < today and not completed |
| `upcoming_deadlines_7d` | Imminent tasks | `studentmentormatch` | dueDate within 7 days |
| `deal_ready_ideas` | Pitch-ready ideas | `pitchperfect` | pitchScore >= 80 |
| `new_ideas` | Recently assigned ideas | `studentmentormatch` | matches created in last 7 days |
| `ideas_table[]` | Full mentor workload | `studentmentormatch`, `businessidea`, `account` | Idea + student + pitch status |
| `new_ideas_table[]` | Newly matched ideas | `studentmentormatch`, `businessidea` | Filter by createdAt |
| `ready_to_invest_table[]` | Pitch-ready list | `pitchperfect`, `businessidea` | pitchScore >= 80 |
| `my_mentoring_table[]` | Mentoring list | `studentmentormatch`, `businessidea` | Idea name + process + progress |

### Mentor Per Idea — `GET /analytics/mentor-per-idea`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `ideas[]` | Per-idea view | `studentmentormatch`, `businessidea`, `account`, `pitchperfect` | Filter by mentor + optional idea |

### Admin Overall — `GET /analytics/admin-overall`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `users.total` | Platform size | `account` | Count all users |
| `users.byRole` | Role distribution | `account`, `role` | Group by role |
| `users.newWeek/newMonth` | Growth | `account` | Count created in 7/30 days |
| `users.active7d/30d` | Activity level | `mdl_logstore_standard_log` | Users with logs in window |
| `users.inactive7d/30d` | Engagement gaps | `mdl_logstore_standard_log` | Total - active |
| `users.trend7d[]` | Active users trend | `mdl_logstore_standard_log` | Daily distinct active users |
| `logs.volume7d[]` | System usage | `mdl_logstore_standard_log` | Daily log count |
| `logs.eventMix7d[]` | Activity mix | `mdl_logstore_standard_log`, `mdl_course_modules_completion`, `post`, `comment` | Daily counts by type |
| `concurrentUsers[]` | Peak usage proxy | `mdl_logstore_standard_log` | Distinct users per 5‑minute bucket |
| `mentorLoadTop[]` | Mentor load | `studentmentormatch` | Count matches per mentor |
| `alerts.assignmentOverdue` | Urgent learner issues | `mdl_assign`, `mdl_assign_submission` | Overdue unsubmitted count |
| `alerts.ideaPendingReview` | Admin queue | `businessidea` | status in submitted/underreview |
| `alerts.mentorMatchOverdue` | Overdue mentoring | `studentmentormatch` | dueDate < today |

### Admin Learning — `GET /analytics/admin-learning`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `coursesTotal` | Catalog size | `mdl_course` | Count courses |
| `completionRate` | Learning success | `mdl_course_completions` | completed / total * 100 |
| `avgProgressPct` | System progress | `mdl_course_modules_completion` | Avg completion across students |
| `topCoursesByEnroll[]` | High-demand courses | `mdl_enrol`, `mdl_user_enrolments` | Top 5 by enrol count |
| `topMissingCourses[]` | Courses with most overdue | `mdl_assign`, `mdl_assign_submission` | Highest missing rate |
| `completionTrend30d[]` | Completion trend | `mdl_course_modules_completion` | Daily completion % |

### Admin Engagement — `GET /analytics/admin-engagement`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `totals.posts/comments/reactions` | Platform engagement | `post`, `comment`, `reaction` | Count all records |
| `topUsers[]` | Most engaged users | `post`, `comment`, `reaction`, `account` | Rank by total actions |
| `timeline30d[]` | Engagement trend | `post`, `comment` | Daily counts |

### Admin Ideas — `GET /analytics/admin-ideas`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `ideasTotal` | Idea pipeline size | `businessidea` | Count all ideas |
| `ideasByStatus` | Pipeline distribution | `businessidea` | Group by status |
| `mentorMatch.total/overdue/upcoming7d` | Mentoring load | `studentmentormatch` | Total + due windows |
| `pitch.total` | Pitch volume | `pitchperfect` | Count pitch records |
| `pitch.fundingTotal` | Funding signal | `pitchperfect` | Sum of funding |
| `ideasTrend30d[]` | Idea creation trend | `businessidea` | Daily ideas created |
| `pitchTrend30d[]` | Pitch + funding trend | `pitchperfect` | Daily pitch count + funding |

### Investor Overall — `GET /analytics/investor-overall`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `pitchTotal` | Deal flow | `pitchperfect` | Count investor pitches |
| `fundingTotal` | Capital allocated | `pitchperfect` | Sum funding |
| `upcomingPitches7d` | Upcoming review load | `pitchperfect` | eventDate within 7 days |
| `readyToInvest` | High-potential ideas | `pitchperfect` | pitchScore >= 80 |
| `investedIdeas[]` | Portfolio list | `pitchperfect`, `businessidea` | Approved + funded |
| `newIdeas[]` | New pipeline | `pitchperfect`, `businessidea` | Not funded yet |
| `rankingTable[]` | Top idea ranking | `pitchperfect`, `businessidea` | Sort by pitchScore |
| `ideaByDomain` | Domain distribution | `businessidea.tags` | Count by primary tag |

### Investor Invested Ideas — `GET /analytics/investor-invested-ideas`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `ideas[]` | Full invested list | `pitchperfect`, `businessidea` | Approved + funded only |

### Investor Per Idea — `GET /analytics/investor-per-idea`
| Field | Why it matters | Source tables | How it is calculated |
| --- | --- | --- | --- |
| `ideas[]` | One idea drill‑down | `businessidea`, `pitchperfect`, `studentmentormatch`, `account` | Join by ideaId + filters |

## Test
```bash
BASE_URL="http://127.0.0.1:8001"
# If you are using cloudflared, replace BASE_URL with your tunnel URL, e.g.:
# BASE_URL="https://forward-planned-opponent-developments.trycloudflare.com"

curl "$BASE_URL/analytics/student-overall?moodle_user_id=20"
curl "$BASE_URL/analytics/student-per-course?moodle_user_id=20&course_id=101"

curl "$BASE_URL/analytics/teacher-overall?teacher_id=10"
curl "$BASE_URL/analytics/teacher-per-course?teacher_id=10&course_id=101"

curl "$BASE_URL/analytics/mentor-overall?mentor_id=11"
curl "$BASE_URL/analytics/mentor-per-idea?mentor_id=11"
curl "$BASE_URL/analytics/mentor-per-idea?mentor_id=11&idea_id=StuIdea-001"

curl "$BASE_URL/analytics/admin-overall"
curl "$BASE_URL/analytics/admin-learning"
curl "$BASE_URL/analytics/admin-engagement"
curl "$BASE_URL/analytics/admin-ideas"

curl "$BASE_URL/analytics/investor-overall?investor_id=inv-001"
curl "$BASE_URL/analytics/investor-invested-ideas?investor_id=inv-001"
curl "$BASE_URL/analytics/investor-per-idea?investor_id=inv-001"
curl "$BASE_URL/analytics/investor-per-idea?investor_id=inv-001&idea_id=StuIdea-001"
curl "$BASE_URL/analytics/investor-per-idea?investor_id=inv-001&mentor_id=men-001"
curl "$BASE_URL/analytics/investor-per-idea?investor_id=inv-001&student_id=stu-001"
```

## Demo UI
Open in browser:
```
http://127.0.0.1:8001/
```

## Seed demo data (fake DB)
```bash
Get-Content .\seed\fakedata.sql | mysql -u root -p fakedatalms
Get-Content .\seed\fakedatamoodle.sql | mysql -u root -p fakedatamoodle
```
