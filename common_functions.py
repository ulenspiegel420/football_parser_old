import random

def get_rand_user_agent_from_file(filename):
    count_lines = 0
    with open(filename, 'r') as file: count_lines = sum(1 for line in file)
    rand_line_num = random.randint(1, count_lines)

    with open(filename, 'r') as file:
        current_line_num = 1
        for line in file:
            if rand_line_num == current_line_num:return line
            current_line_num += 1

