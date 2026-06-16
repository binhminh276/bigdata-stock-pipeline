# 1. CA SÁNG (Từ 09h00 đến 10h55): Cứ 5 phút chạy 1 lần
*/5 9-10 * * 1-5 /usr/bin/python3 /home/hadoop/scrape_and_insert.py >> /home/hadoop/cron_scrape.log 2>&1

# 2. CA TRƯA (Từ 11h00 đến 11h25): Chạy các mốc 0, 5, 10, 15, 20, 25 rồi NGHIÊM TÚC ĐI NGỦ
0,5,10,15,20,25 11 * * 1-5 /usr/bin/python3 /home/hadoop/scrape_and_insert.py >> /home/hadoop/cron_scrape.log 2>&1

# 3. CA CHIỀU (Từ 13h00 đến 14h55): Ngủ dậy, tiếp tục 5 phút chạy 1 lần cho đến hết phiên
*/5 13-14 * * 1-5 /usr/bin/python3 /home/hadoop/scrape_and_insert.py >> /home/hadoop/cron_scrape.log 2>&1