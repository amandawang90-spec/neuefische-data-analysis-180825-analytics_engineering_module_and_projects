## Note to the Teacher
- **(in backlog) code_along_NF** is just a copy of old NF curriculum. **It is not fit for the week's pipeline flow.** possibly it can go to the backlog.
- **B_lecture_SP** is goes in detail into "What is actually an API?" it is a lecture and some bits of code along. `API_lecture_teacher.ipynb` is filled the `API_lecture_student.ipynb` has some cells to finish during the lecture. 
- the **exercise** contains instructions how to sign-up and use the `meteostat` API account
- **(moved to next day) live_extract_load** is meant as the separate praxis lecture. There are *filled* and *to_fill* versions of loading moteostat data to database, for hourly and for daily data. (whether both or just "daily" is sufficient is not set in stone. Both is doable)

**Hint:** In the first run we got feedback that it was very confusing and owherwhelming to learn the API terminology and logic and on the same day to make advanced calls and push all to the DB. Students wanted to "practice more with the API". 

So the next to do is to split the current lecture into two parts. And put SQL with Python inbetween.
- First "Intro to APIs" Lecture with some simple API showcase
- on the next day "SQL with Python"
- and finish the day with "live_extract_load"


## To-Do's (obsolete?)

- adapt both options to simple API showcase and then call meteostat Weather
- update exercises (at least in option B)


by the end of the day, students should have a script

- reading env
- calling the api 
- pushing raw data to DB