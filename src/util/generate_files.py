import random
import string
import os

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return  ''.join(random.choice(chars) for _ in range(size))

def generate_impressions(file_size=100, regenerate=False):
    path_to_impressions_file = "../../resources/impression_log.txt"
    if (os.stat(path_to_impressions_file).st_size == 0 or regenerate):
        impression_file = open(path_to_impressions_file, "w")
        impression_file.write("%s %s %s %s\n" % ("timestamp", "customerId", "cookie", "auctionId"))
        for i in range(1, file_size):
            # time stampt current time +/- month
            # coockie uid
            # auctionid uid
            impression_file.write("%i %i %s %i\n" % (random.randint(1, 1000000), random.randint(2000, 3000), id_generator(), random.randint(1, 30)))
        impression_file.close()

def generate_logs(file_size=100, regenerate=False):
    path_to_click_log_file = "../../resources/click_log.txt"
    if (os.stat(path_to_click_log_file).st_size == 0 or regenerate):
        click_file = open(path_to_click_log_file, "w")
        click_file.write("%s %s\n" % ("timestamp", "auctionId"))
        click_file.write("%s %s\n" % ("timestamp", "auctionId"))

        for i in range(1, file_size):
            # time stampt current time +/- month
            click_file.write("%i %i\n" % (random.randint(1, 1000000), +random.randint(1, 30)))
        click_file.close()


generate_impressions(100000, regenerate=True)
generate_logs(10000, regenerate=True)