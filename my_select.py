from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from models import Student, Group, Grade, Subject, Teacher
from db import engine

# 1. Знайти 5 студентів з найбільшим середнім балом з усіх предметів.
def select_1(session: Session):
    result = (
        session.query(Student.name, func.avg(Grade.grade).label("average_score"))
        .join(Grade, Student.id == Grade.student_id)
        .group_by(Student.id)
        .order_by(desc("average_score"))
        .limit(5)
        .all()
    )
    return result

# 2. Знайти студента з найвищим середнім балом з певного предмета.
def select_2(session: Session, subject_name: str):
    result = (
        session.query(Student.name, func.avg(Grade.grade).label("average_score"))
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subject_id == Subject.id)
        .filter(Subject.name == subject_name)
        .group_by(Student.id)
        .order_by(desc("average_score"))
        .first()
    )
    return result

# 3. Знайти середній бал у групах з певного предмета.
def select_3(session: Session, subject_name: str):
    result = (
        session.query(Group.name, func.avg(Grade.grade).label("average_score"))
        .join(Student, Student.group_id == Group.id)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subject_id == Subject.id)
        .filter(Subject.name == subject_name)
        .group_by(Group.id)
        .all()
    )
    return result

# 4. Знайти середній бал у потоці (по всій таблиці оцінок).
def select_4(session: Session):
    result = session.query(func.avg(Grade.grade).label("average_score")).scalar()
    return result

# 5. Знайти, які курси читає певний викладач.
def select_5(session: Session, teacher_id: int):
    result = (
        session.query(Subject.name)
        .join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Teacher.id == teacher_id)
        .all()
    )
    return [r[0] for r in result]

# 6. Знайти список студентів у певній групі.
def select_6(session: Session, group_name: str):
    result = (
        session.query(Student.name)
        .join(Group, Student.group_id == Group.id)
        .filter(Group.name == group_name)
        .all()
    )
    return [r[0] for r in result]

# 7. Знайти оцінки студентів у певній групі з певного предмета.
def select_7(session: Session, group_name: str, subject_name: str):
    result = (
        session.query(Student.name, Grade.grade, Grade.received_at)
        .join(Group, Student.group_id == Group.id)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subject_id == Subject.id)
        .filter(Group.name == group_name, Subject.name == subject_name)
        .all()
    )
    return result

# 8. Знайти середній бал, який ставить певний викладач зі своїх предметів.
def select_8(session: Session, teacher_id: int):
    result = (
        session.query(func.avg(Grade.grade).label("average_score"))
        .join(Subject, Grade.subject_id == Subject.id)
        .join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Teacher.id == teacher_id)
        .scalar()
    )
    return result

# 9. Знайти список курсів, які відвідує певний студент.
def select_9(session: Session, student_id: int):
    result = (
        session.query(Subject.name)
        .join(Grade, Subject.id == Grade.subject_id)
        .join(Student, Grade.student_id == Student.id)
        .filter(Student.id == student_id)
        .distinct()
        .all()
    )
    return [r[0] for r in result]

# 10. Список курсів, які певний викладач читає певному студенту.
def select_10(session: Session, teacher_id: int, student_id: int):
    result = (
        session.query(Subject.name)
        .join(Grade, Subject.id == Grade.subject_id)
        .join(Student, Grade.student_id == Student.id)
        .join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Teacher.id == teacher_id, Student.id == student_id)
        .distinct()
        .all()
    )
    return [r[0] for r in result]

if __name__ == "__main__":
    with Session(engine) as session:
        print(
            "5 студентів з найбільшим середнім балом з усіх предметів: ",
            select_1(session),
        )
        print(
            "Студент з найвищим середнім балом з математики: ",
            select_2(session, "Mathematics"),
        )
        print("Середній бал у групах з біології: ", select_3(session, "Biology"))
        print("Середній бал у потоці: ", select_4(session))
        print("Курси, які читає певний викладач: ", select_5(session, 1))
        print("Список студентів у групі 1: ", select_6(session, "Group 1"))
        print(
            "Оцінки для студентів у групі 2 з англійської: ",
            select_7(session, "Group 2", "English"),
        )
        print(
            "Середній бал, який ставить певний викладач зі своїх предметів: ",
            select_8(session, 2),
        )
        print("Список курсів, які відвідує певний студент: ", select_9(session, 10))
        print(
            "Список курсів, які певний викладач читає певному студенту: ",
            select_10(session, 3, 15),
        )
