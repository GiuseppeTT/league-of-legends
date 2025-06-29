pkill -f "uv run crawl_matches.py"
sudo rm -rf log
sudo docker rm -f postgres
sudo rm -rf database