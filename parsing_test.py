import unittest
from datetime import date
import mysql_operations as db

class Test_parsing_test(unittest.TestCase):
    def test_add_identical_tournaments(self):
        first_tournament_row = ('Премьер лига','Россия',date(2003,1,1),date(2003,12,1))
        second_tournament_row = ('Премьер лига','Россия',date(2003,1,1),date(2003,12,1))

        tournaments_rows =[first_tournament_row,second_tournament_row]
        result = db.add_tournaments(tournaments_rows)

        self.assertFalse(result)

    def test_add_tournaments_with_edentical_name_and_dates(self):
        first_tournament_row = ('Премьер лига','Россия',date(2003,1,1),date(2003,12,1))
        second_tournament_row = ('Премьер лига','Англия',date(2003,1,1),date(2003,12,1))

        tournaments_rows =[first_tournament_row,second_tournament_row]
        result = db.add_tournaments(tournaments_rows)

        self.assertTrue(result)

    def test_add_exist_in_db_tournaments(self):
        tournament_row = ('Премьер лига','Россия',date(2003,1,1),date(2003,12,1))

        tournaments_rows =[tournament_row]
        result = db.add_tournaments(tournaments_rows)

        self.assertFalse(result)

    def test_tournament_exist(self):
        tournament_row = ('Премьер лига','Россия',date(2003,1,1),date(2003,12,1))

        result = db.tournament_exist(*tournament_row)

        self.assertTrue(result,'Исхоной записи '+str(tournament_row)+' нет в БД')
       
if __name__ == '__main__':
    unittest.main()
