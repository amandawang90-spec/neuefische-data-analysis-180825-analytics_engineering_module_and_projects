/* Creating 3 tables with Constraints
    - students
    - enrolments
    - exam_rades
*/

/* # TABLE students */
DROP TABLE IF EXISTS students CASCADE;
CREATE TABLE students (
  student_id SERIAL,
  student_name VARCHAR(255),
  email TEXT
);

ALTER TABLE students ADD PRIMARY KEY (student_id);
ALTER TABLE students ALTER COLUMN student_name SET NOT NULL;
ALTER TABLE students ADD UNIQUE (email);

INSERT INTO students (student_name, email)
VALUES 
('Anna', 'anna@gmail.com')
,('Joseph', 'joseph@gmail.com')
,('Scally', 'scally@gmail.com')
,('Liam', 'liam@gmail.com')
,('Elif', 'elif@gmail.com');

/* # TABLE enrolments */
DROP TABLE IF EXISTS enrolments;
CREATE TABLE enrolments (
  enrolment_id SERIAL PRIMARY KEY,
  seminar_name VARCHAR(255),
  student_id INT
);

ALTER TABLE enrolments
ADD FOREIGN KEY (student_id) REFERENCES students(student_id);

INSERT INTO enrolments (seminar_name, student_id)
VALUES 
('science', 2)
,('history', 1)
,('ethics', 2)
,('politics', 1)
,('art', 5)
,('engineering', 4);

/* # TABLE exam_grades */
DROP TABLE IF EXISTS exam_grades;
CREATE TABLE exam_grades (
  seminar_name VARCHAR(255),
  student_id INT,
  grade NUMERIC
);

ALTER TABLE exam_grades
ADD FOREIGN KEY (student_id) REFERENCES students(student_id);

INSERT INTO exam_grades (seminar_name, student_id, grade)
VALUES 
('art', 5, 1.3)
,('ethics', 2, 1.0)
,('engineering', 4, 2.1)
,('politics', 1, 3.5);