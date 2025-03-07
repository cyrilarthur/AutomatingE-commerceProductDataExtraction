import asyncio
from pyppeteer import launch
import logging
import csv

# Setup Logging 
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s -%(message)s')


async def scrape_product_data():
    # Launch the headless browser 
    browser = await launch(executablePath="C:/Program Files/Google/Chrome/Application/chrome.exe")
    page = await browser.newPage()

    # List to store product data
    product_data = []

    # Open CSV file for writing 
    with open('product.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Product Name', 'Price', 'Rating'])

        page_num = 1
        while True:
            try:
                 # URL of the product listing page 
                 url = 'https://www.amazon.com/dp/B08N5M7S6K?th=1'
                 await page.goto(url)
                 logging.info(f"Scraping page {page_num}")

                # Wait for elements to appear (Handles dynamic content)
                 await page.waitForSelector('article.product_pod', timeout=5000)

                 # Take a screenshot 
                 await page.screenshot({'path': "C:\\Users\\cyril\\OneDrive\\Pictures\\screenshot.png"})
                 
                 # Extract product data 
                 products = await page.querySelectorAll('article.product_pod')
                 print(f"Found {len(products)} products on page {page_num}")  # Debugging output
                 
                 if not products:   # stop if no products found (last page)
                     break
                 
                 for product in products:
                     try:
                         name = await product.querySelectorAllEval('h3 a', 'el => el.innerText')
                         price = await product.querySelectorAllEval('p.price_color', 'el => el.innerText')
                         rating = await product.querySelectorAllEval('p.star-rating', 'el => el.className.split(" ")[1]')
                         product_data.append([name, price, rating])
                         writer.writerow([name, price, rating])

                     except Exception as e:
                         logging.error(f"Error extracting product details: {e}")
                         
                # Scroll to bottom for infinite scroll websites
                 await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                 await asyncio.sleep(3)  # Wait for content to load
                 
                # Move to next page
                 page_num += 1
                 
            except Exception as e:
                logging.error(f"Error scraping page {page_num}: {e}")
                break

            await browser.close()
            logging.info("Scrapping completed!")



# Run the async function
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scrape_product_data())




                         
    