query4_1_1 = '''SELECT COUNT(shows.show_id) 
                    FROM shows 
                    LEFT JOIN movie_crew 
                    ON shows.show_id = movie_crew.show_id 
                    WHERE movie_crew.show_id IS NULL;'''


query4_1_2 = '''SELECT COUNT(shows.show_id) 
                    FROM shows 
                    LEFT JOIN listings 
                    ON shows.show_id = listings.show_id 
                    WHERE listings.show_id IS NULL;'''


query5_1_1 = '''SELECT p.first_name
                    FROM personnel p
                    JOIN movie_crew mc ON p.id = mc.personnel_id
                    WHERE p.gender = 'female' AND mc.personnel_type = 'cast'
                    GROUP BY p.first_name
                    ORDER BY COUNT(*) DESC
                    LIMIT 1;'''

query5_1_2 = '''SELECT p.first_name
                    FROM personnel p
                    JOIN movie_crew mc ON p.id = mc.personnel_id
                    WHERE p.gender = 'male' AND mc.personnel_type = 'cast'
                    GROUP BY p.first_name
                    ORDER BY COUNT(*) DESC
                    LIMIT 1;'''

query5_1_3 = '''SELECT p.first_name
                    FROM personnel p
                    JOIN movie_crew mc ON p.id = mc.personnel_id
                    WHERE p.gender = 'unknown' AND mc.personnel_type = 'cast'
                    GROUP BY p.first_name
                    ORDER BY COUNT(*) DESC
                    LIMIT 1;'''


query5_2 = '''SELECT title, EXTRACT(YEAR FROM date_added) - release_year AS gap
                    FROM shows
                    ORDER BY gap DESC
                    LIMIT 1'''


query5_3 = '''SELECT to_char(date_added, 'MM') AS month, COUNT(*) AS num_releases
                FROM shows
                GROUP BY month
                ORDER BY num_releases DESC
                LIMIT 1;
            '''


query5_4 = '''SELECT t1.release_year, t1.num_shows, t2.num_shows AS prev_num_shows,
                    (t1.num_shows - t2.num_shows) / t2.num_shows * 100 AS increase_percentage
                    FROM (
                        SELECT release_year, COUNT(*) AS num_shows
                        FROM shows
                        WHERE type = 'TV Show'
                        GROUP BY release_year
                    ) t1
                    JOIN (
                        SELECT release_year, COUNT(*) AS num_shows
                        FROM shows
                        WHERE type = 'TV Show'
                        GROUP BY release_year
                    ) t2 ON t1.release_year = t2.release_year + 1
                    ORDER BY increase_percentage DESC
                    LIMIT 1;'''

query5_5_1 = '''SELECT mc.show_id
                FROM movie_crew mc
                JOIN personnel p ON p.id = mc.personnel_id
                WHERE p.name = '{}';
                '''
query5_5_2 = '''SELECT p.name, COUNT(p.id) AS num_shows
                FROM personnel p
                JOIN movie_crew mc ON mc.personnel_id = p.id
                WHERE mc.show_id in ({}) AND p.gender IN ('female', 'unknown')
                GROUP BY p.name
                ORDER BY num_shows DESC;
                '''
