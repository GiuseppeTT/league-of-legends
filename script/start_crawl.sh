mkdir -p log/

nohup uv run crawl_matches.py --region=NA1 --tier=CHALLENGER --tier=GRANDMASTER --tier=MASTER >> log/na-crawler.log 2>&1 &
sleep 5
nohup uv run crawl_matches.py --region=EUW1 --tier=CHALLENGER --tier=GRANDMASTER --tier=MASTER >> log/euw-crawler.log 2>&1 &
sleep 5
nohup uv run crawl_matches.py --region=KR --tier=CHALLENGER --tier=GRANDMASTER --tier=MASTER >> log/kr-crawler.log 2>&1 &
sleep 5
nohup uv run crawl_matches.py --region=VN2 --tier=CHALLENGER --tier=GRANDMASTER --tier=MASTER >> log/vn-crawler.log 2>&1 &