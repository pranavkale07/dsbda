import requests
from bs4 import BeautifulSoup
import time
print(requests.get("https://httpbin.org/ip").text)
# Flipkart review page URL (replace with your actual product's review page)
url = "https://www.flipkart.com/vivo-t3-ultra-frost-green-256-gb/product-reviews/itme360ff5b7dbab?pid=MOBH4EACZ7SACMMM"

# Send request
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}
response = requests.get(url, headers=headers, timeout=10)
soup = BeautifulSoup(response.content, "lxml")

# Extract reviews
reviews = soup.find_all("div", class_="EKFha-")  # comment text

# Write to file
with open("flipkart_reviews2.txt", "w", encoding="utf-8") as f:
    for review in reviews:
        parent = review.find_parent("div", class_="col _2wzgFH K0kLPL")  # review block

        name = parent.select_one("p._2NsDsF.AwS1CA")
        rating = parent.select_one(".XQDdHH.Ga3i8K")
        comment = review.select_one(".ZmyHeo div div")

        f.write(f"Name: {name.get_text(strip=True) if name else 'N/A'}\n")
        f.write(f"Rating: {rating.get_text(strip=True) if rating else 'N/A'}\n")
        f.write(f"Comment: {comment.get_text(strip=True).replace('READ MORE', '') if comment else 'N/A'}\n")
        f.write("-" * 40 + "\n")

print("Saved to flipkart_reviews2.txt")
