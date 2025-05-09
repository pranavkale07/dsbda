from bs4 import BeautifulSoup
import os
import csv

print(os.getcwd())

# Load HTML
with open("flipkart_new.html", "r", encoding="utf-8") as file:
    soup = BeautifulSoup(file, "lxml")

# Extract reviews
reviews = soup.find_all("div", class_="EKFha-")

# Open CSV file for writing
with open("flipkart_reviews.csv", "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["Name", "Rating", "Comment"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Write the header
    writer.writeheader()

    # Write review data
    for review in reviews:
        name = review.select_one("p._2NsDsF.AwS1CA")
        rating = review.select_one(".XQDdHH.Ga3i8K")
        comment = review.select_one(".ZmyHeo div div")

        writer.writerow({
            "Name": name.get_text(strip=True) if name else 'N/A',
            "Rating": rating.get_text(strip=True)[:1] if rating else 'N/A',
            "Comment": comment.get_text(strip=True) if comment else 'N/A'
        })

print("Saved to flipkart_reviews.csv")
