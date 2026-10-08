"""ADK tools for interacting with PostgreSQL, Cloud Storage and Vertex AI Search."""
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
from db.database import get_db
from db.models import Assignment, AssignmentDocument, StudentEssay
from services.search_service import SearchService

search_service = SearchService()

# Instructions/rubric text handed to the model per category, so a long PDF does not flood its context
MAX_DOCUMENT_TEXT = 8000


def _get_assignment(db, identifier: Union[int, str]) -> Optional[Assignment]:
    """Looks up an assignment by its numeric id (PostgreSQL) or by its text slug."""
    if isinstance(identifier, int):
        return db.query(Assignment).filter_by(id=identifier).first()
    if isinstance(identifier, str):
        if identifier.isdigit():
            by_id = db.query(Assignment).filter_by(id=int(identifier)).first()
            if by_id:
                return by_id
        return db.query(Assignment).filter_by(slug=identifier).first()
    return None


def get_assignment_details(assignment_id: Union[int, str]) -> Dict[str, Any]:
    """Gets the full details of an essay assignment set by the teacher.

    Args:
        assignment_id: Integer PostgreSQL ID or slug of the assignment (e.g. 1 or 'world-war-ii').

    Returns:
        A dictionary with the ID, slug, title, description, skills of focus, due date, the list of attached
        documents with their category, and the text of the teacher's instructions and rubric documents.
    """
    with get_db() as db:
        assignment = _get_assignment(db, assignment_id)
        if not assignment:
            return {
                "status": "error",
                "message": f"No assignment found with ID or slug '{assignment_id}'.",
            }

        docs_info = []
        for doc in assignment.documents:
            docs_info.append({
                "file_name": doc.file_name,
                "category": doc.category,
                "gcs_uri": doc.gcs_uri,
                "summary": doc.summary,
            })

        def text_of(category: str) -> str:
            texts = [d.content_snippet or "" for d in assignment.documents if d.category == category]
            return "\n\n".join(t for t in texts if t)[:MAX_DOCUMENT_TEXT]

        return {
            "status": "success",
            "assignment_id": assignment.id,
            "slug": assignment.slug,
            "title": assignment.title,
            "description": assignment.description,
            "instructions_text": text_of("instructions"),
            "rubric_text": text_of("rubric"),
            "focus_skills": assignment.focus_skill_list,
            "due_date": assignment.due_date.isoformat() if assignment.due_date else None,
            "documents": docs_info,
        }


def get_student_draft(student_id: int, assignment_id: Union[int, str]) -> Dict[str, Any]:
    """Retrieves the current essay draft and the outline saved by a student.

    Args:
        student_id: The student's identifier (e.g. 12).
        assignment_id: Integer PostgreSQL ID or slug of the assignment (e.g. 1 or 'world-war-ii').

    Returns:
        A dictionary with the essay title, current content, outline, status and last saved date.
    """
    with get_db() as db:
        assignment = _get_assignment(db, assignment_id)
        if not assignment:
            return {
                "status": "error",
                "message": f"No assignment found with ID or slug '{assignment_id}'.",
            }

        draft = (
            db.query(StudentEssay)
            .filter_by(student_id=student_id, assignment_id=assignment.id)
            .first()
        )
        if not draft:
            return {
                "status": "not_found",
                "message": f"Student '{student_id}' does not have a draft for assignment '{assignment_id}' yet.",
                "title": "",
                "content": "",
                "outline": "",
                "status_essay": "not_started",
            }

        return {
            "status": "success",
            "assignment_id": assignment.id,
            "title": draft.title or "Untitled",
            "content": draft.content or "",
            "outline": draft.outline or "",
            "status_essay": draft.status,
            "last_saved_at": draft.last_saved_at.isoformat() if draft.last_saved_at else None,
            "submitted_at": draft.submitted_at.isoformat() if draft.submitted_at else None,
        }


def save_student_draft(
    student_id: int,
    assignment_id: Union[int, str],
    title: str,
    content: str,
    outline: str,
) -> Dict[str, Any]:
    """Saves the essay progress (title, content and outline) to the PostgreSQL database.

    Args:
        student_id: The student's identifier (e.g. 12).
        assignment_id: Integer PostgreSQL ID or slug of the assignment (e.g. 1 or 'world-war-ii').
        title: Title of the essay written by the student.
        content: Text or paragraphs of the essay written so far.
        outline: Outline of arguments or notes on the student's thesis.

    Returns:
        A dictionary with the save confirmation status and the recorded date/time.
    """
    with get_db() as db:
        assignment = _get_assignment(db, assignment_id)
        if not assignment:
            return {
                "status": "error",
                "message": f"No assignment found with ID or slug '{assignment_id}'.",
            }

        draft = (
            db.query(StudentEssay)
            .filter_by(student_id=student_id, assignment_id=assignment.id)
            .first()
        )
        now = datetime.utcnow()

        if draft:
            if draft.status != "draft":
                return {
                    "status": "error",
                    "message": "This essay has already been formally submitted and can no longer be modified.",
                }
            draft.title = title
            draft.content = content
            draft.outline = outline
            draft.last_saved_at = now
        else:
            draft_id = f"draft_{student_id}_{assignment.id}"
            draft = StudentEssay(
                id=draft_id,
                assignment_id=assignment.id,
                student_id=student_id,
                title=title,
                content=content,
                outline=outline,
                status="draft",
                last_saved_at=now,
            )
            db.add(draft)

        db.commit()

        return {
            "status": "success",
            "message": "Essay draft successfully saved to the database.",
            "assignment_id": assignment.id,
            "title": draft.title,
            "last_saved_at": now.isoformat(),
        }


def submit_student_essay(student_id: int, assignment_id: Union[int, str]) -> Dict[str, Any]:
    """Marks the student's essay as formally submitted to the teacher in PostgreSQL.

    Args:
        student_id: The student's identifier (e.g. 12).
        assignment_id: Integer PostgreSQL ID or slug of the assignment (e.g. 1 or 'world-war-ii').

    Returns:
        A dictionary with the submission confirmation and the date it was received.
    """
    with get_db() as db:
        assignment = _get_assignment(db, assignment_id)
        if not assignment:
            return {
                "status": "error",
                "message": f"No assignment found with ID or slug '{assignment_id}'.",
            }

        draft = (
            db.query(StudentEssay)
            .filter_by(student_id=student_id, assignment_id=assignment.id)
            .first()
        )
        if not draft:
            return {
                "status": "error",
                "message": "No draft was found to submit for this assignment.",
            }

        if draft.status != "draft":
            return {
                "status": "warning",
                "message": "The essay had already been submitted.",
                "submitted_at": draft.submitted_at.isoformat() if draft.submitted_at else None,
            }

        if not draft.content or len(draft.content.strip()) < 100:
            return {
                "status": "error",
                "message": "The essay is empty or too short for a formal submission. Keep working on the draft.",
            }

        now = datetime.utcnow()
        draft.status = "submitted"
        draft.submitted_at = now
        db.commit()

        return {
            "status": "success",
            "message": "Congratulations! Your essay has been formally submitted to your teacher.",
            "assignment_id": assignment.id,
            "submitted_at": now.isoformat(),
            "title": draft.title,
        }


def search_assignment_documents(assignment_id: Union[int, str], query: str) -> Dict[str, Any]:
    """Searches the documents and PDFs that the teacher uploaded to Cloud Storage, indexed with Vertex AI Search.

    Args:
        assignment_id: Integer PostgreSQL ID or slug of the assignment (e.g. 1 or 'world-war-ii').
        query: Query, topic question or historical concept to search for in the readings.

    Returns:
        A dictionary with the most relevant passages and quotes found in the documents.
    """
    results = search_service.search_documents(assignment_id, query)
    return {
        "status": "success",
        "assignment_id": assignment_id,
        "query": query,
        "results_count": len(results),
        "results": results,
    }
