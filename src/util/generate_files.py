import random
import string
import os
import uuid
import time

ONE_MONTH_INT = 2629746;

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return  ''.join(random.choice(chars) for _ in range(size))

def generate_impressions(file_size=100, regenerate=False):
    path_to_impressions_file = "../resources/impression_log.txt"
    if (os.stat(path_to_impressions_file).st_size == 0 or regenerate):
        impression_file = open(path_to_impressions_file, "w")
        impression_file.write("%s %s %s %s\n" % ("timestamp", "customerId", "cookie", "auctionId"))
        for i in range(1, file_size):
            impression_file.write("%s %i %s %s\n" % (int(time.time()) + random.randint(-ONE_MONTH_INT, ONE_MONTH_INT), random.randint(2000, 3000), uuid.uuid4(), str(uuid.uuid4())[0:8]))
        impression_file.close()

def generate_logs(file_size=100, regenerate=False):
    path_to_click_log_file = "../resources/click_log.txt"
    if (os.stat(path_to_click_log_file).st_size == 0 or regenerate):
        click_file = open(path_to_click_log_file, "w")
        click_file.write("%s %s\n" % ("timestamp", "auctionId"))
        for i in range(1, file_size):
            click_file.write("%i %s\n" % ( int(time.time()) + random.randint(-ONE_MONTH_INT, ONE_MONTH_INT) , str(uuid.uuid4())[0:8]))
        click_file.close()

