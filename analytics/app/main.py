import json
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from .controllers.student import router as student_router
from .controllers.teacher import router as teacher_router
from .controllers.mentor import router as mentor_router
from .controllers.admin import router as admin_router
from .controllers.investor import router as investor_router

class PrettyJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(content, ensure_ascii=False, indent=2).encode("utf-8")


app = FastAPI(title="Founders Academy Analytics", default_response_class=PrettyJSONResponse)
app.include_router(student_router)
app.include_router(teacher_router)
app.include_router(mentor_router)
app.include_router(admin_router)
app.include_router(investor_router)
