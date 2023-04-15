#!/usr/bin/env python
# coding: utf-8

# # Creating an Auction House Database and ETL Pipelines for Data Analysis
# #### Version: Selenium 4.3.0 (june 2022)

# # Import libraries

# In[ ]:


from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
from xpathwebdriver.browser import Browser
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from datetime import datetime, timedelta
from selenium.common.exceptions import TimeoutException

from bs4 import BeautifulSoup 
import re
import pandas as pd
import time
import pyautogui
import os

import xlrd
import openpyxl
from PIL import Image
import cv2
from pytesseract import image_to_string
import pytesseract

import janitor

# Google Big Query 
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq


# In[ ]:


# setting the path to the webdriver
# webdriver drives the browser natively 
service = Service(executable_path="/path/to/chromedriver")
driver = webdriver.Chrome(service=service)


# In[ ]:


# create ChromeOptions object
options = webdriver.ChromeOptions()

# set the download directory
prefs = {'download.default_directory' : '/Users/megandba/Desktop/reports'}
options.add_experimental_option('prefs', prefs)

# create Chrome webdriver
driver = webdriver.Chrome(options=options)


# # Start of Auction Platform Reports
# 
# Below will be the code that goes through each software and extracts a report. Each software will open a new driver for ease of use between tabs. 

# ## ProxiBid Platform

# In[ ]:


#navigate to proxibid website
prefs = {'download.default_directory' : '/Users/megandba/Desktop/analytics'}
options.add_experimental_option('prefs', prefs)

# create Chrome webdriver
driver = webdriver.Chrome(options=options)
driver.get("https://www.proxibid.com/asp/LoginAuctioneer.asp")


# In[ ]:


#this is for the page to load and elements to populate
#not the best solution for easiest waiting strategy
driver.implicitly_wait(0.5)


# In[ ]:


#login into website
username_field = driver.find_element(By.ID, "auctioneerUsername")
username_field.send_keys("ENTER-USERNAME)

password_field = driver.find_element(By.ID, "auctioneerPassword")
password_field.send_keys("ENTER-PASSWORD")

element = WebDriverWait(driver, 10).until(
        lambda driver: driver.find_element(By.ID,'auctioneerLoginSubmitButton').is_enabled()
    )

driver.find_element(By.ID,"auctioneerLoginSubmitButton").click()


# In[ ]:


driver.implicitly_wait(2)


# In[ ]:


event_numbers = []
# read in the divs
divs = driver.find_elements(By.CSS_SELECTOR, 'div[class^="event"]')

# Loop through each div element
for div in divs:
    # Extract the date from the span element inside the div
    date_str = div.find_element(By.TAG_NAME, 'span').text
    # Convert the date to a datetime object
    date_obj = datetime.strptime(date_str, '%A, %B %d, %Y')
    # Get the current date and time
    now = datetime.now()
    # Calculate the difference between the current date and the date of the div
    diff = now - date_obj
    # Check if the difference is less than or equal to 14 days
    if diff <= timedelta(days=18):
        # Extract the event number from the div class
        event_number = div.get_attribute("class").split("event")[1]
        event_numbers.append(event_number)

# Click on the second link in the event_numbers list
for index, event_number in enumerate(event_numbers):
    if index == 0 :
        link = f"Invoicing.asp?aid={event_number}"
        driver.find_element(By.CSS_SELECTOR, f"a[href='{link}']").click()
        break


# In[ ]:


#this clicks the master list
masterlist = driver.find_element(By.CSS_SELECTOR,"svg[data-testid='SummarizeIcon']")
masterlist.click()


# In[ ]:


#switches to most recent tab. This is needed.
driver.switch_to.window(driver.window_handles[1])


# In[ ]:


#Downloads the file into the downloads section
WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "here"))).click()


# In[ ]:


#switches to most recent tab. This is needed.
driver.switch_to.window(driver.window_handles[0])


# In[ ]:


link_element = driver.find_element(By.CSS_SELECTOR, "a[href='https://www.proxibid.com/asp/Auctioneer/AuctioneerHome.asp'] img[alt='Auction Builder']")
link_element.click()


# In[ ]:


event_numbers = []
# read in the divs
divs = driver.find_elements(By.CSS_SELECTOR, 'div[class^="event"]')

# Loop through each div element
for div in divs:
    # Extract the date from the span element inside the div
    date_str = div.find_element(By.TAG_NAME, 'span').text
    # Convert the date to a datetime object
    date_obj = datetime.strptime(date_str, '%A, %B %d, %Y')
    # Get the current date and time
    now = datetime.now()
    # Calculate the difference between the current date and the date of the div
    diff = now - date_obj
    # Check if the difference is less than or equal to 14 days
    if diff <= timedelta(days=18):
        # Extract the event number from the div class
        event_number = div.get_attribute("class").split("event")[1]
        event_numbers.append(event_number)

# Click on the second link in the event_numbers list
for index, event_number in enumerate(event_numbers):
    if index == 1 :
        link = f"Invoicing.asp?aid={event_number}"
        driver.find_element(By.CSS_SELECTOR, f"a[href='{link}']").click()
        break


# In[ ]:


#this clicks the master list
masterlist = driver.find_element(By.CSS_SELECTOR,"svg[data-testid='SummarizeIcon']")
masterlist.click()


# In[ ]:


#switches to most recent tab. This is needed.
driver.switch_to.window(driver.window_handles[2])


# In[ ]:


#Downloads the file into the downloads section
WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "here"))).click()


# In[ ]:


#switches to most recent tab. This is needed.
driver.switch_to.window(driver.window_handles[0])


# In[ ]:


link_element = driver.find_element(By.CSS_SELECTOR, "a[href='https://www.proxibid.com/asp/Auctioneer/AuctioneerHome.asp'] img[alt='Auction Builder']")
link_element.click()


# In[ ]:


event_numbers = []
# read in the divs
divs = driver.find_elements(By.CSS_SELECTOR, 'div[class^="event"]')

# Loop through each div element
for div in divs:
    # Extract the date from the span element inside the div
    date_str = div.find_element(By.TAG_NAME, 'span').text
    # Convert the date to a datetime object
    date_obj = datetime.strptime(date_str, '%A, %B %d, %Y')
    # Get the current date and time
    now = datetime.now()
    # Calculate the difference between the current date and the date of the div
    diff = now - date_obj
    # Check if the difference is less than or equal to 14 days
    if diff <= timedelta(days=18):
        # Extract the event number from the div class
        event_number = div.get_attribute("class").split("event")[1]
        event_numbers.append(event_number)

# Click on the second link in the event_numbers list
for index, event_number in enumerate(event_numbers):
    if index == 2  :
        link = f"Invoicing.asp?aid={event_number}"
        driver.find_element(By.CSS_SELECTOR, f"a[href='{link}']").click()
        break


# In[ ]:


#this clicks the master list
masterlist = driver.find_element(By.CSS_SELECTOR,"svg[data-testid='SummarizeIcon']")
masterlist.click()


# In[ ]:


#switches to most recent tab. This is needed.
driver.switch_to.window(driver.window_handles[3])


# In[ ]:


#Downloads the file into the downloads section
WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "here"))).click()


# In[ ]:


#switches to most recent tab. This is needed.
driver.switch_to.window(driver.window_handles[0])


# ## Invaluable Auction Platform

# In[ ]:


# create ChromeOptions object
options = webdriver.ChromeOptions()

# set the download directory
prefs = {'download.default_directory' : '/Users/megandba/Desktop/analytics'}
options.add_experimental_option('prefs', prefs)

# create Chrome webdriver
driver = webdriver.Chrome(options=options)


# In[ ]:


#navigate to proxibid website
driver.get("https://www.invaluableauctions.com/index.cfm")


# In[ ]:


#this is for the page to load and elements to populate
#not the best solution for easiest waiting strategy
driver.implicitly_wait(0.5)


# In[ ]:


#Login to Invaluable 

#house ID 
House_ID_element = driver.find_element(By.NAME, 'houseName')
House_ID_element.send_keys('ENTER-AUCTION-HOUSE')

#username
username_element = driver.find_element(By.NAME, 'username')
username_element.send_keys('ENTER-USERNAME')

#password
pword_element = driver.find_element(By.NAME, 'password')
pword_element.send_keys('ENTER-PASSWORD')

#click login with lag built in 
element = WebDriverWait(driver, 10).until(
        lambda driver: driver.find_element(By.NAME,'Submit').is_enabled()
    )

driver.find_element(By.NAME,"Submit").click()


# In[ ]:


#get past the intro page by clicking continue
#click login with lag built in 
element = WebDriverWait(driver, 10).until(
        lambda driver: driver.find_element(By.ID,'continue').is_enabled()
    )

driver.find_element(By.ID,"continue").click()


# In[ ]:


#click on the reports in the menu
driver.find_element(By.LINK_TEXT,"Reports").click()

#click the sub-menu "sale Results" tab
driver.find_element(By.LINK_TEXT,"Sale Results").click()


# In[ ]:


#in this version we are trying to pull from a specific timeframe to get the old auction and test this function

# Get dates in range
end_date = datetime.datetime.now()  # Current date
start_date = end_date - datetime.timedelta(days=14)  # Subtract 14 days


rows = driver.find_elements(By.CSS_SELECTOR, 'tbody > tr')
for row in rows : 
    # get the dates for the rows
    date_string = row.find_elements(By.CSS_SELECTOR,'.dateCell')[0].text
    # remove the hour, minute and timezone
    date_string = date_string.split(' ')[0]
    # convert to datetime object
    date_object = datetime.strptime(date_string, '%m/%d/%Y')
    #check to see if the date is within two weeks
    if date_object >= start_date and date_object <= end_date : 
        row.find_elements(By.CSS_SELECTOR, ".js-sessionCheckbox")[0].click()


# In[ ]:


#click the view selected button
view_selected_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "js-viewSelectedSessionsButton")))
view_selected_button.click()


# In[ ]:


#Click on the format for spreadsheet 
#this then downloads the spreadsheet
driver.find_element(By.LINK_TEXT,"Format for spreadsheet").click()


# In[ ]:


#click over to the Auctions/Sales tab on the main bar
driver.find_element(By.LINK_TEXT,"Auctions / Sales").click()


# In[ ]:


#click the bubble for the past auction

driver.find_element(By.XPATH, "//input[@name='upcomingPastRadio']//following::label[1]").click()


# In[ ]:


#click cells within our timeframe
date_cells = driver.find_elements(By.XPATH, "//span[contains(text(), 'MST')]")

end_date = datetime.datetime.now()  # Current date
start_date = end_date - datetime.timedelta(days=14)  # Subtract 14 days

valid_date_cells = [cell for cell in date_cells if start_date <= datetime.strptime(cell.text, '%a, %b %d %Y %H:%M %Z') <= end_date]

for cell in valid_date_cells:
    # select the dropdown and click on "manage invoices"
    dropdown_btn = cell.find_element(By.XPATH, "../..//button[@class='dropdown-toggle btn btn-white']")
    dropdown_btn.click()
    manage_invoices_btn = driver.find_element(By.LINK_TEXT, "Manage invoices")
    manage_invoices_btn.click()
    break


# In[ ]:


#check the check all button top left grid
driver.find_elements(By.CSS_SELECTOR, ".js-selectAllCheckbox")[0].click()


# In[ ]:


#Select the view details button
view_details_button = driver.find_element(By.XPATH, "//button[contains(text(),'View Details')]")

view_details_button.click()


# In[ ]:


time.sleep(30)


# In[ ]:


# this will take about 2:30 minutes to run
# read in the html page with selenium 
html = driver.page_source

# set soup to the above html 
soup = BeautifulSoup(html, 'html.parser')

# Create a list of dictionaries that contains the values 
invoice_list = []

# create a list of invoice numbers 
invoice_nums = soup.find_all('li', attrs={'class': 'bold'})


# with our list of invoices, we will loop through them and pull the information that matches. 
for invoice_num in invoice_nums :
    
    # Get invoice number as a string
    invoice_num = str(invoice_num.text.strip('-'))
    
    # get full name
    bidder_name = soup.find('a', {'name': 'inv' + invoice_num}).find_next('a').text.strip()

    # Paddle Number
    bidder_number = soup.find('a', {'name': 'inv' + invoice_num }).find_next('a').nextSibling.text.strip()

    # total hammer price
    totalHammerElem = soup.find('td', attrs={'class': 'bold totalHammer totalCell-' + invoice_num, 'id': 'totalHammer-' + invoice_num})
    totalHammer = totalHammerElem.text if totalHammerElem is not None else ''
    
    # total bidder price
    totalBpElem = soup.find('td', attrs={'class': 'bold totalBp totalCell-' + invoice_num, 'id': 'totalBp-' + invoice_num})
    totalBp = totalBpElem.text if totalBpElem is not None else ''
    
    # live fee
    totalLiveFeeElem = soup.find('td', attrs={'class': 'bold totalLiveFee totalCell-' + invoice_num, 'id': 'totalLiveFee-' + invoice_num})
    totalLiveFee = totalLiveFeeElem.text if totalLiveFeeElem is not None else ''
    
    # We need this line below 
    totalShippingInputElem = soup.find('input', attrs={'id': 'totalShippingInput-' + invoice_num})
    
    # this could be redundant 
    totalShippingInput = totalShippingInputElem['value'] if totalShippingInputElem is not None else ''
    
    # total shipping code. This is needed because the last cell of the page is a different code call. 
    if totalShippingInputElem is not None and totalShippingInputElem['value'] != '':
        totalShippingInput = totalShippingInputElem['value']
    else:
        totalSH = soup.find('span', attrs={'id': 'totalSH-' + invoice_num})
        totalShippingInput = totalSH.text if totalSH is not None else ''


    # total tax
    totalTaxInputElem = soup.find('input', attrs={'id': 'totalTaxInput-' + invoice_num})
    totalTaxInput = totalTaxInputElem['value'] if totalTaxInputElem is not None else ''
    
    # insurance 
    totalInsuranceInputElem = soup.find('input', attrs={'id': 'totalInsuranceInput-' + invoice_num})
    totalInsuranceInput = totalInsuranceInputElem['value'] if totalInsuranceInputElem is not None else ''
    
    # any other fee in other column 
    totalOtherInputElem = soup.find('input', attrs={'id': 'totalOtherInput-' + invoice_num})
    totalOtherInput = totalOtherInputElem['value'] if totalOtherInputElem is not None else ''
    
    # total for everything 
    totalCellElem = soup.find('td', attrs={'class': 'bold totalCell-' + invoice_num})
    totalCell = totalCellElem['value'] if totalCellElem is not None else ''
    
    # Append values to invoice_list
    invoice_list.append({
        'name' : bidder_name,
        'invoice_num': invoice_num,
        #if this breaks take out the # and the plus
        'paddle_#': '#'+ bidder_number,
        'totalHammer': totalHammer,
        'totalBp': totalBp,
        'totalLiveFee': totalLiveFee,
        'totalShippingInput': totalShippingInput,
        'totalTaxInput': totalTaxInput,
        'totalInsuranceInput': totalInsuranceInput,
        'totalOtherInput': totalOtherInput,
        'totalCell': totalCell
    })
    
# Create a dataframe from the list of dictionaries
df = pd.DataFrame(invoice_list)


# In[ ]:


df.to_csv('invaluable_wepage_prices.csv')


# ## iCollector Auction Platform

# In[ ]:


# create ChromeOptions object
options = webdriver.ChromeOptions()

# set the download directory
prefs = {'download.default_directory' : '/Users/megandba/Desktop/analytics'}
options.add_experimental_option('prefs', prefs)

# create Chrome webdriver
driver = webdriver.Chrome(options=options)


# In[ ]:


#navigate to icollector website
driver.get("https://admin.liveauctiongroup.com/login.aspx?ReturnUrl=%2f")


# In[ ]:


#this is for the page to load and elements to populate
#not the best solution for easiest waiting strategy
driver.implicitly_wait(0.5)


# In[ ]:


#Login to iCollector 

#username
username_element = driver.find_element(By.ID, 'txtUsername')
username_element.send_keys('ENTER-USERNAME')

#password
pword_element = driver.find_element(By.ID, 'txtPassword')
pword_element.send_keys('ENTER-PASSWORD')

#click login with lag built in 
element = WebDriverWait(driver, 10).until(
        lambda driver: driver.find_element(By.ID,'btnLogin').is_enabled()
    )

driver.find_element(By.ID,"btnLogin").click()


# In[ ]:


# click auctions
driver.find_element(By.LINK_TEXT,"auctions").click()


# In[ ]:


# Click All Auctions
driver.find_element(By.LINK_TEXT, "All Auctions").click()


# In[ ]:


# Click 100 to view all auctions on page
driver.find_element(By.LINK_TEXT, "100").click()


# In[ ]:


# click the auction within the specific date
start_date = datetime(2023, 1, 24)
end_date = datetime(2023, 1, 30)

rows = driver.find_elements(By.TAG_NAME, "tr")

for row in rows:
    cols = row.find_elements(By.TAG_NAME, "td")
    if len(cols) >= 3:
        date_str = cols[2].text.split(" @ ")[0]
        date = datetime.strptime(date_str, '%Y %b %d')
        if start_date <= date <= end_date:
            link = cols[0].find_element(By.TAG_NAME, "a")
            link.click()
            break


# In[ ]:


# click invoicing
driver.find_element(By.LINK_TEXT, "invoicing").click()


# In[ ]:


# remove session filtering
driver.find_element(By.ID, "cphBody_aRemoveSessionFilter").click()


# In[ ]:


# Click All Invoices in Auction (as Quickbooks File) to download spreadsheet #1
driver.find_element(By.LINK_TEXT, "All Invoices in Auction (as Quickbooks File)").click()


# In[ ]:


# click reports
driver.find_element(By.LINK_TEXT, "reports").click()


# In[ ]:


element = driver.find_element(By.XPATH, '//td/a[@href="downloads/invoicessummaryall.ashx?a=59053"]')
element.click()


# ## LiveAuctioneers Platform

# In[ ]:


options = webdriver.ChromeOptions()

# set the download directory
prefs = {'download.default_directory' : '/Users/megandba/Desktop/analytics'}
options.add_experimental_option('prefs', prefs)

# create Chrome webdriver
driver = webdriver.Chrome(options=options)
#navigate to liveauctioneers website
driver.get("https://partners.liveauctioneers.com/login")


# In[ ]:


#Login to LiveAuctioneers 

#username
username_element = driver.find_element(By.ID, 'username')
username_element.send_keys('ENTER-USERNAME')

#password
pword_element = driver.find_element(By.ID, 'password')
pword_element.send_keys('ENTER-PASSWORD')
time.sleep(2)
#click login with lag built in 
login_button = driver.find_element(By.XPATH, '//button[@data-testid="button" and contains(., "Log In")]')
login_button.click()


# In[ ]:


# #click Auctions in main menu
link = driver.find_element(By.LINK_TEXT, 'Auctions ▾')
link.click()


# In[ ]:


#click the post auction button 
driver.find_element(By.LINK_TEXT, "Post Auction").click()


# In[ ]:


# click the auction within the specific date
end_date = datetime.datetime.now()  # Current date
start_date = end_date - datetime.timedelta(days=14)  # Subtract 14 days

# Get the table
table = driver.find_element(By.TAG_NAME, "table")

# Get the rows in the table
rows = table.find_elements(By.TAG_NAME, "tr")

# Iterate through the rows
for row in rows:
    # Get the cells in the row
    cells = row.find_elements(By.TAG_NAME, "td")
    # Check if the row contains any cells
    if len(cells) > 0:
        # Check if the first cell contains a date within the specified range
        if start_date <= datetime.strptime(cells[0].text, '%Y-%m-%d %I:%M%p') <= end_date:
            # Get the link element in the second cell
            link = cells[1].find_element(By.TAG_NAME, "a")
            # Click the link
            link.click()
            # Break out of the loop
            break


# In[ ]:


# click the EOA
driver.find_element(By.LINK_TEXT, "EOA").click()


# In[ ]:


# click full report to download file
driver.find_element(By.LINK_TEXT, "Full Report").click()


# In[ ]:


# click the Invoices tab
driver.find_element(By.LINK_TEXT, 'Invoices').click()


# In[ ]:


# set up the soup
# read in the html page with selenium 
html = driver.page_source

# Parse the HTML using Beautiful Soup
soup = BeautifulSoup(html, 'html.parser')


# In[ ]:


# Get the headers
heads = []
header = soup.find_all("table")[4].find("tr")
 
for items in header:
    if items.get_text().strip():  # check if not empty
        heads.append(items.get_text())
        
heads = [head.strip('\n') for head in heads]


# In[ ]:


table_rows = soup.find_all('table')[4].find_all('tr')

data = []

for tr in table_rows:
    td = tr.find_all('td')
    row = [tr.text for tr in td]
    data.append(row)


# In[ ]:


# write data to df
dataFrame = pd.DataFrame(data = data, columns = heads)


# In[ ]:


#write the file out to the page title 
page_title = driver.find_element(By.ID, "pagetitle").text
page_title = page_title.replace(' ', '')
page_title = page_title.replace('-', '')

dataFrame.to_csv(page_title + "LA.csv")


# In[ ]:


# #click Auctions in main menu
link = driver.find_element(By.LINK_TEXT, 'Auctions ▾')
link.click()


# In[ ]:


#click the post auction button 
driver.find_element(By.LINK_TEXT, "Post Auction").click()


# In[ ]:


start_date = datetime(2023, 1, 24)
end_date = datetime(2023, 1, 30)

# Initialize a counter
counter = 0

# Get the table
table = driver.find_element(By.TAG_NAME, "table")

# Get the rows in the table
rows = table.find_elements(By.TAG_NAME, "tr")

# Iterate through the rows
for row in rows:
    # Get the cells in the row
    cells = row.find_elements(By.TAG_NAME, "td")
    # Check if the row contains any cells
    if len(cells) > 0:
        # Check if the first cell contains a date within the specified range
        if start_date <= datetime.strptime(cells[0].text, '%Y-%m-%d %I:%M%p') <= end_date:
            
            #set up a counter
            counter += 1
            
            #make it select the second date match
            if counter == 2 : 
            
                # Get the link element in the second cell
                link = cells[1].find_element(By.TAG_NAME, "a")
                # Click the link
                link.click()
                # Break out of the loop
                break


# In[ ]:


# click the EOA
driver.find_element(By.LINK_TEXT, "EOA").click()


# In[ ]:


# click full report to download file
driver.find_element(By.LINK_TEXT, "Full Report").click()


# In[ ]:


# click the Invoices tab
driver.find_element(By.LINK_TEXT, 'Invoices').click()


# In[ ]:


# set up soup
# read in the html page with selenium 
html = driver.page_source

# Parse the HTML using Beautiful Soup
soup = BeautifulSoup(html, 'html.parser')


# In[ ]:


heads = []
header = soup.find_all("table")[4].find("tr")
 
for items in header:
    if items.get_text().strip():  # check if not empty
        heads.append(items.get_text())
        
heads = [head.strip('\n') for head in heads]


# In[ ]:


table_rows = soup.find_all('table')[4].find_all('tr')

data = []

for tr in table_rows:
    td = tr.find_all('td')
    row = [tr.text for tr in td]
    data.append(row)


# In[ ]:


# write data to df
dataFrame = pd.DataFrame(data = data, columns = heads)


# In[ ]:


#write the file out to the page title 
page_title = driver.find_element(By.ID, "pagetitle").text
page_title = page_title.replace(' ', '')
page_title = page_title.replace('-', '')

dataFrame.to_csv(page_title + "LA.csv")


# #### This code will go through each day for the auction

# In[ ]:


# #click Auctions in main menu
link = driver.find_element(By.LINK_TEXT, 'Auctions ▾')
link.click()


# In[ ]:


#click the post auction button 
driver.find_element(By.LINK_TEXT, "Post Auction").click()


# In[ ]:


start_date = datetime(2023, 1, 24)
end_date = datetime(2023, 1, 30)

# Initialize a counter
counter = 0

# Get the table
table = driver.find_element(By.TAG_NAME, "table")

# Get the rows in the table
rows = table.find_elements(By.TAG_NAME, "tr")

# Iterate through the rows
for row in rows:
    # Get the cells in the row
    cells = row.find_elements(By.TAG_NAME, "td")
    # Check if the row contains any cells
    if len(cells) > 0:
        # Check if the first cell contains a date within the specified range
        if start_date <= datetime.strptime(cells[0].text, '%Y-%m-%d %I:%M%p') <= end_date:
            
            #set up a counter
            counter += 1
            
            #make it select the second date match
            if counter == 3 : 
            
                # Get the link element in the second cell
                link = cells[1].find_element(By.TAG_NAME, "a")
                # Click the link
                link.click()
                # Break out of the loop
                break


# In[ ]:


# click the EOA
driver.find_element(By.LINK_TEXT, "EOA").click()


# In[ ]:


# click full report to download file
driver.find_element(By.LINK_TEXT, "Full Report").click()


# In[ ]:


# click the Invoices tab
driver.find_element(By.LINK_TEXT, 'Invoices').click()


# In[ ]:


# set up soup
# read in the html page with selenium 
html = driver.page_source

# Parse the HTML using Beautiful Soup
soup = BeautifulSoup(html, 'html.parser')


# In[ ]:


heads = []
header = soup.find_all("table")[4].find("tr")
 
for items in header:
    if items.get_text().strip():  # check if not empty
        heads.append(items.get_text())
        
heads = [head.strip('\n') for head in heads]


# In[ ]:


table_rows = soup.find_all('table')[4].find_all('tr')

data = []

for tr in table_rows:
    td = tr.find_all('td')
    row = [tr.text for tr in td]
    data.append(row)


# In[ ]:


# write data to df
dataFrame = pd.DataFrame(data = data, columns = heads)


# In[ ]:


#write the file out to the page title 
page_title = driver.find_element(By.ID, "pagetitle").text
page_title = page_title.replace(' ', '')
page_title = page_title.replace('-', '')

dataFrame.to_csv(page_title + "LA.csv")


# In[ ]:


driver.quit()


# # Hibid Auction Platform
# 
# This section is specific for the desktop.

# In[ ]:


# launch hibid app
pyautogui.moveTo(138, 1228, duration=1)
pyautogui.click(x=138, y = 1228)

# accept server
pyautogui.moveTo(1205, 394, duration=1)
pyautogui.click(x=1205, y = 394)

#go to enter username 
pyautogui.moveTo(1116, 558, duration=3)
pyautogui.click(x=1116, y = 558)

# write in user name 
pyautogui.write('USERNAME')

# move to password box
pyautogui.moveTo(1120, 595, duration=1)
pyautogui.click(x=1120, y = 595)

#enter password
pyautogui.write('PASSWORD')

# click submit
pyautogui.moveTo(1078, 651, duration=1)
pyautogui.click(x=1078, y = 651)

#click on the icon to launch
pyautogui.moveTo(353, 310, duration=1)

pyautogui.click(x=353, y=310, clicks=3, interval=.5)


# In[ ]:


# click "Select/new"
pyautogui.moveTo(1369, 523, duration=1)
pyautogui.click(x=1369, y = 523)

#click on dates to arrange dates DESC
pyautogui.moveTo(1384, 571, duration=2)
pyautogui.click(x=1384, y = 571, clicks= 2, interval= 1)

# Auction One
auction_recent_one = (2535, 1154, 130 ,25)
# Use PyAutoGUI to locate the date range for auction one
auction_recent_one_image = pyautogui.screenshot(region=auction_recent_one)
auction_recent_one_image.save('/Users/megandba/Desktop/analytics/auction_recent_one_image.png')
auction_recent_one_image_text = pytesseract.image_to_string(auction_recent_one_image)


# Auction Two
auction_recent_two = (2538, 1180, 130 ,25)
# Use PyAutoGUI to locate the date range
auction_recent_two_image = pyautogui.screenshot(region=auction_recent_two)
auction_recent_two_image.save('/Users/megandba/Desktop/analytics/auction_recent_two_image.png')
auction_recent_two_image_text = pytesseract.image_to_string(auction_recent_two_image)


# Auction Three 
auction_recent_three = (2538, 1208, 130 ,25)
# Use PyAutoGUI to locate the date range
auction_recent_three_image = pyautogui.screenshot(region=auction_recent_three)
auction_recent_three_image.save('/Users/megandba/Desktop/analytics/auction_recent_three_image.png')
auction_recent_three_image_text = pytesseract.image_to_string(auction_recent_three_image)

# Auction Four
auction_recent_four = (2538, 1236, 130 ,26)
# Use PyAutoGUI to locate the date range
auction_recent_four_image = pyautogui.screenshot(region=auction_recent_four)
auction_recent_four_image.save('/Users/megandba/Desktop/analytics/auction_recent_four_image.png')
auction_recent_four_image_text = pytesseract.image_to_string(auction_recent_four_image)


# Auction Five 
auction_recent_five = (2538, 1261, 130 ,26)
# Use PyAutoGUI to locate the date range
auction_recent_five_image = pyautogui.screenshot(region=auction_recent_five)
auction_recent_five_image.save('/Users/megandba/Desktop/analytics/auction_recent_five_image.png')
auction_recent_five_image_text = pytesseract.image_to_string(auction_recent_five_image)


# Auction six 
auction_recent_six = (2538, 1289, 130 ,26)
# Use PyAutoGUI to locate the date range
auction_recent_six_image = pyautogui.screenshot(region=auction_recent_six)
auction_recent_six_image.save('/Users/megandba/Desktop/analytics/auction_recent_six_image.png')
auction_recent_six_image_text = pytesseract.image_to_string(auction_recent_six_image)


# Auction seven 
auction_recent_seven = (2538, 1317, 130 ,26)
# Use PyAutoGUI to locate the date range
auction_recent_seven_image = pyautogui.screenshot(region=auction_recent_seven)
auction_recent_seven_image.save('/Users/megandba/Desktop/analytics/auction_recent_seven_image.png')
auction_recent_seven_image_text = pytesseract.image_to_string(auction_recent_seven_image)


# Auction eight 
auction_recent_eight = (2538, 1344, 130 ,26)
# Use PyAutoGUI to locate the date range
auction_recent_eight_image = pyautogui.screenshot(region=auction_recent_eight)
auction_recent_eight_image.save('/Users/megandba/Desktop/analytics/auction_recent_eight_image.png')
auction_recent_eight_image_text = pytesseract.image_to_string(auction_recent_eight_image)

# Turn all the coordinates into a dict with their dates
auct_dict = {('2535, 1154'): auction_recent_one_image_text, 
             ('2538, 1180'): auction_recent_two_image_text, 
             ('2538, 1208'): auction_recent_three_image_text,
             ('2538, 1236'): auction_recent_four_image_text,
             ('2538, 1261'): auction_recent_five_image_text,
             ('2538, 1289'): auction_recent_six_image_text,
             ('2538, 1317'): auction_recent_seven_image_text,
             ('2538, 1344'): auction_recent_eight_image_text,
            }

# set up your time frame here 
today = datetime(2023, 2, 26).date()
two_weeks_ago = datetime(2023, 2, 24).date()
two_weeks_ago = datetime.combine(two_weeks_ago, datetime.min.time())
today = datetime.combine(today, datetime.min.time())

# add in the counter 
counter = 0

# here we will check the dates against our selection
# we will take the coordiantes divided by two with some math to click our one date
for key, value in auct_dict.items() :
    item_date_str = value.strip()
    item_date = datetime.strptime(item_date_str, '%m/%d/%Y')
    
    if two_weeks_ago <= item_date <= today : 
        
        counter += 1
        
        if counter == 1 :
            print(key, value)
            x, y = map(int, key.split(','))
            #we divide bc retina display doubles the screenshot coords
            new_key = ((x // 2)+3, (y // 2)+5)
            print(new_key)
            pyautogui.moveTo(new_key, duration=1)
            pyautogui.click(new_key)
            break
            
# click OKAY to return to menu
pyautogui.moveTo(x = 1350, y = 844, duration=1)
pyautogui.click(x = 1350, y = 844)

# click Check-out bidders
pyautogui.moveTo(x = 1169, y = 733, duration=1)
pyautogui.click(x = 1169, y = 733)

# click invoices
pyautogui.moveTo(x = 681, y = 660, duration=1)
pyautogui.click(x = 681, y = 660)

# click reports
pyautogui.moveTo(x = 686, y = 924, duration=1)
pyautogui.click(x = 686, y = 924)

# click custom reports
pyautogui.moveTo(x = 834, y = 708, duration=1)
pyautogui.click(x = 834, y = 708)

# click print icon in the top right of the current box 
pyautogui.moveTo(x = 1420, y = 552, duration=5)
pyautogui.click(x = 1420, y = 552)

# click yes (would uou like to export to Excel)
pyautogui.moveTo(x = 1179, y = 681, duration=5)
pyautogui.click(x = 1179, y = 681)

# click this pc to save to This PC
pyautogui.moveTo(x = 56, y = 305, duration=5)
pyautogui.click(x = 56, y = 305)


# click the Macintosh HD
pyautogui.moveTo(x = 858, y = 213, duration=5)
pyautogui.click(x = 858, y = 213, clicks = 3)


# click the USERS folder
pyautogui.moveTo(x = 144, y = 354, duration=5)
pyautogui.click(x = 144, y = 354, clicks = 3)

# click the megandba
pyautogui.moveTo(x = 163, y = 125, duration=5)
pyautogui.click(x = 163, y = 125, clicks = 3)

# click the desktop folder
pyautogui.moveTo(x = 163, y = 182, duration=5)
pyautogui.click(x = 163, y = 182, clicks = 3)

# click the Analytics Folder 
pyautogui.moveTo(x = 157, y = 145, duration=5)
pyautogui.click(x = 157, y = 145, clicks = 4)

# click the save button
pyautogui.moveTo(x = 2187, y = 1082, duration=5)
pyautogui.click(x = 2187, y = 1082, clicks = 4)

# click the okay
pyautogui.moveTo(x = 1299, y = 689, duration=5)
pyautogui.click(x = 1299, y = 689, clicks = 2)

# click the red x to get back to the auction page 
pyautogui.moveTo(x = 1598, y = 462, duration=5)
pyautogui.click(x = 1598, y = 462, clicks = 2)


# In[ ]:


# check for the second auction

# click "Select/new"
pyautogui.moveTo(1369, 523, duration=1)
pyautogui.click(x=1369, y = 523)

#click on dates to arrange dates DESC
pyautogui.moveTo(1384, 571, duration=2)
pyautogui.click(x=1384, y = 571, clicks= 2, interval= 1)


# reset the counter
counter = 0

# here we will check the dates against our selection
# we will take the coordiantes divided by two with some math to click our one date
for key, value in auct_dict.items() :
    item_date_str = value.strip()
    item_date = datetime.strptime(item_date_str, '%m/%d/%Y')
    
    if two_weeks_ago <= item_date <= today : 
        
        counter += 1
        
        if counter == 2 :
            print(key, value)
            x, y = map(int, key.split(','))
            #we divide bc retina display doubles the screenshot coords
            new_key = ((x // 2)+3, (y // 2)+5)
            print(new_key)
            pyautogui.moveTo(new_key, duration=1)
            pyautogui.click(new_key)
            break
            
# click OKAY to return to menu
pyautogui.moveTo(x = 1350, y = 844, duration=1)
pyautogui.click(x = 1350, y = 844)

# click Check-out bidders
pyautogui.moveTo(x = 1169, y = 733, duration=1)
pyautogui.click(x = 1169, y = 733)

# click invoices
pyautogui.moveTo(x = 681, y = 660, duration=1)
pyautogui.click(x = 681, y = 660)

# click reports
pyautogui.moveTo(x = 686, y = 924, duration=1)
pyautogui.click(x = 686, y = 924)

# click custom reports
pyautogui.moveTo(x = 834, y = 708, duration=1)
pyautogui.click(x = 834, y = 708)

# click print icon in the top right of the current box 
pyautogui.moveTo(x = 1420, y = 552, duration=5)
pyautogui.click(x = 1420, y = 552)

# click yes (would uou like to export to Excel)
pyautogui.moveTo(x = 1179, y = 681, duration=5)
pyautogui.click(x = 1179, y = 681)

# click this pc to save to This PC
pyautogui.moveTo(x = 56, y = 305, duration=5)
pyautogui.click(x = 56, y = 305)


# click the Macintosh HD
pyautogui.moveTo(x = 858, y = 213, duration=5)
pyautogui.click(x = 858, y = 213, clicks = 3)


# click the USERS folder
pyautogui.moveTo(x = 144, y = 354, duration=5)
pyautogui.click(x = 144, y = 354, clicks = 3)

# click the megandba
pyautogui.moveTo(x = 163, y = 125, duration=5)
pyautogui.click(x = 163, y = 125, clicks = 3)

# click the desktop folder
pyautogui.moveTo(x = 163, y = 182, duration=5)
pyautogui.click(x = 163, y = 182, clicks = 3)

# click the Analytics Folder 
pyautogui.moveTo(x = 157, y = 145, duration=5)
pyautogui.click(x = 157, y = 145, clicks = 4)

# click the save button
pyautogui.moveTo(x = 2187, y = 1082, duration=5)
pyautogui.click(x = 2187, y = 1082, clicks = 4)

# click the okay
pyautogui.moveTo(x = 1299, y = 689, duration=5)
pyautogui.click(x = 1299, y = 689, clicks = 2)

# click the red x to get back to the auction page 
pyautogui.moveTo(x = 1598, y = 462, duration=5)
pyautogui.click(x = 1598, y = 462, clicks = 2)


# In[ ]:


def export_auction_report_3(auct_dict) : 
    # THE THIRD AUCTION

    # click "Select/new"
    pyautogui.moveTo(1369, 523, duration=1)
    pyautogui.click(x=1369, y = 523)

    #click on dates to arrange dates DESC
    pyautogui.moveTo(1384, 571, duration=2)
    pyautogui.click(x=1384, y = 571, clicks= 2, interval= 1)


    # reset the counter
    counter = 0

    # here we will check the dates against our selection
    # we will take the coordiantes divided by two with some math to click our one date
    for key, value in auct_dict.items() :
        item_date_str = value.strip()
        item_date = datetime.strptime(item_date_str, '%m/%d/%Y')

        if two_weeks_ago <= item_date <= today : 

            counter += 1

            if counter == 3 :
                found_third_auction = True
                x, y = map(int, key.split(','))
                #we divide bc retina display doubles the screenshot coords
                new_key = ((x // 2)+3, (y // 2)+5)
                pyautogui.moveTo(new_key, duration=1)
                pyautogui.click(new_key)
                break
    else :
        print('no third auction')
            
    if found_third_auction: 
        # click OKAY to return to menu
        pyautogui.moveTo(x = 1350, y = 844, duration=1)
        pyautogui.click(x = 1350, y = 844)

        # click Check-out bidders
        pyautogui.moveTo(x = 1169, y = 733, duration=1)
        pyautogui.click(x = 1169, y = 733)

        # click invoices
        pyautogui.moveTo(x = 681, y = 660, duration=1)
        pyautogui.click(x = 681, y = 660)

        # click reports
        pyautogui.moveTo(x = 686, y = 924, duration=1)
        pyautogui.click(x = 686, y = 924)

        # click custom reports
        pyautogui.moveTo(x = 834, y = 708, duration=1)
        pyautogui.click(x = 834, y = 708)

        # click print icon in the top right of the current box 
        pyautogui.moveTo(x = 1420, y = 552, duration=5)
        pyautogui.click(x = 1420, y = 552)

        # click yes (would uou like to export to Excel)
        pyautogui.moveTo(x = 1179, y = 681, duration=5)
        pyautogui.click(x = 1179, y = 681)

        # click this pc to save to This PC
        pyautogui.moveTo(x = 56, y = 305, duration=5)
        pyautogui.click(x = 56, y = 305)


        # click the Macintosh HD
        pyautogui.moveTo(x = 858, y = 213, duration=5)
        pyautogui.click(x = 858, y = 213, clicks = 3)


        # click the USERS folder
        pyautogui.moveTo(x = 144, y = 354, duration=5)
        pyautogui.click(x = 144, y = 354, clicks = 3)

        # click the megandba
        pyautogui.moveTo(x = 163, y = 125, duration=5)
        pyautogui.click(x = 163, y = 125, clicks = 3)

        # click the desktop folder
        pyautogui.moveTo(x = 163, y = 182, duration=5)
        pyautogui.click(x = 163, y = 182, clicks = 3)

        # click the Analytics Folder 
        pyautogui.moveTo(x = 157, y = 145, duration=5)
        pyautogui.click(x = 157, y = 145, clicks = 4)

        # click the save button
        pyautogui.moveTo(x = 2187, y = 1082, duration=5)
        pyautogui.click(x = 2187, y = 1082, clicks = 4)

        # click the okay
        pyautogui.moveTo(x = 1299, y = 689, duration=5)
        pyautogui.click(x = 1299, y = 689, clicks = 2)

        # click the red x to get back to the auction page 
        pyautogui.moveTo(x = 1598, y = 462, duration=5)
        pyautogui.click(x = 1598, y = 462, clicks = 2)


# In[ ]:


export_auction_report_3(auct_dict)


# In[ ]:


# THE THIRD AUCTION

# click "Select/new"
pyautogui.moveTo(1369, 523, duration=1)
pyautogui.click(x=1369, y = 523)

#click on dates to arrange dates DESC
pyautogui.moveTo(1384, 571, duration=2)
pyautogui.click(x=1384, y = 571, clicks= 2, interval= 1)


# reset the counter
counter = 0

# here we will check the dates against our selection
# we will take the coordiantes divided by two with some math to click our one date
for key, value in auct_dict.items() :
    item_date_str = value.strip()
    item_date = datetime.strptime(item_date_str, '%m/%d/%Y')
    
    if two_weeks_ago <= item_date <= today : 
        
        counter += 1
        
        if counter == 3 :
            print(key, value)
            x, y = map(int, key.split(','))
            #we divide bc retina display doubles the screenshot coords
            new_key = ((x // 2)+3, (y // 2)+5)
            print(new_key)
            pyautogui.moveTo(new_key, duration=1)
            pyautogui.click(new_key)
            break
            
# click OKAY to return to menu
pyautogui.moveTo(x = 1350, y = 844, duration=1)
pyautogui.click(x = 1350, y = 844)

# click Check-out bidders
pyautogui.moveTo(x = 1169, y = 733, duration=1)
pyautogui.click(x = 1169, y = 733)

# click invoices
pyautogui.moveTo(x = 681, y = 660, duration=1)
pyautogui.click(x = 681, y = 660)

# click reports
pyautogui.moveTo(x = 686, y = 924, duration=1)
pyautogui.click(x = 686, y = 924)

# click custom reports
pyautogui.moveTo(x = 834, y = 708, duration=1)
pyautogui.click(x = 834, y = 708)

# click print icon in the top right of the current box 
pyautogui.moveTo(x = 1420, y = 552, duration=5)
pyautogui.click(x = 1420, y = 552)

# click yes (would uou like to export to Excel)
pyautogui.moveTo(x = 1179, y = 681, duration=5)
pyautogui.click(x = 1179, y = 681)

# click this pc to save to This PC
pyautogui.moveTo(x = 56, y = 305, duration=5)
pyautogui.click(x = 56, y = 305)


# click the Macintosh HD
pyautogui.moveTo(x = 858, y = 213, duration=5)
pyautogui.click(x = 858, y = 213, clicks = 3)


# click the USERS folder
pyautogui.moveTo(x = 144, y = 354, duration=5)
pyautogui.click(x = 144, y = 354, clicks = 3)

# click the megandba
pyautogui.moveTo(x = 163, y = 125, duration=5)
pyautogui.click(x = 163, y = 125, clicks = 3)

# click the desktop folder
pyautogui.moveTo(x = 163, y = 182, duration=5)
pyautogui.click(x = 163, y = 182, clicks = 3)

# click the Analytics Folder 
pyautogui.moveTo(x = 157, y = 145, duration=5)
pyautogui.click(x = 157, y = 145, clicks = 4)

# click the save button
pyautogui.moveTo(x = 2187, y = 1082, duration=5)
pyautogui.click(x = 2187, y = 1082, clicks = 4)

# click the okay
pyautogui.moveTo(x = 1299, y = 689, duration=5)
pyautogui.click(x = 1299, y = 689, clicks = 2)

# click the red x to get back to the auction page 
pyautogui.moveTo(x = 1598, y = 462, duration=5)
pyautogui.click(x = 1598, y = 462, clicks = 2)


# # Process files and send to GBQ

# In[ ]:


# set up directory and read in our data 
directory = '/Users/megandba/Desktop/analytics'
dfs = {}
counter = 1 

for filename in os.listdir(directory):
    if filename.startswith("AUCTION_"):
        file_path = os.path.join(directory, filename)
        df = pd.read_excel(file_path, engine='xlrd')
        df = df.rename(mapper=lambda name: f"{filename}_df")
        dfs[filename] = df
    elif filename.endswith((".csv", ".xls", ".xlsx")):
        file_path = os.path.join(directory, filename)
        if filename.lower().endswith((".xls", ".xlsx")):
            if filename.startswith("sessionResults"):
                df = pd.read_excel(file_path)
                dfs["sessionResults"] = df
            elif filename.startswith("EOA"):
                df = pd.read_excel(file_path)
                if f"EOA{counter}" not in dfs:
                    dfs[f"EOA{counter}"] = df
                counter += 1
            else: 
                df = pd.read_excel(file_path, engine='xlrd')
        else: 
            if filename.startswith("WinningBidderReport"):
                df = pd.read_csv(file_path, skiprows=10, encoding='utf-8')
                if f"proxibid_{counter}" not in dfs:
                    dfs[f"proxibid_{counter}"] = df
                counter += 1
            else:
                df = pd.read_csv(file_path, encoding='utf-8')
                df = df.rename(mapper=lambda name: f"{filename}_df")
                dfs[filename] = df
for key in dfs.keys():
    dfs[key] = dfs[key].clean_names()


# ### Merging Invaluable Files

# In[ ]:


# define a function to extract the numbers before the colon and return as a list
def extract_lottitles(s):
    pattern = r'^(\d+):'
    matches = re.findall(pattern, s)
    return [int(m) for m in matches]

# apply the function to the lottitle column and store in a new column as a list
dfs['sessionResults']['lottitle_list'] = dfs['sessionResults']['lottitle'].apply(extract_lottitles)

# merging invaluable 
df = dfs['sessionResults']
df['lottitle_numbers'] = df['lottitle_list'].apply(lambda x: [int(str(s).split(':')[0]) for s in x])
new_df = df.groupby('paddle_#').agg({
    'session': 'first',
    'lotid': 'first',
    'hammer': 'first',
    'premium': 'first',
    'live_fee': 'first',
    'total': 'first',
    'username': 'first',
    'email': 'first',
    'phone': 'first',
    'firstname': 'first',
    'lastname': 'first',
    'street': 'first',
    'city': 'first',
    'stateid': 'first',
    'postcode': 'first',
    'countryid': 'first',
    'source': 'first',
    'lottitle_numbers': 'sum'
}).reset_index()

# # # # This part will be deleted because we renamed the variables in the output for the file in invalueable
# 
new_df = new_df.rename(columns={'paddle_#': 'paddle_number'})


dfs['invaluable_wepage_prices.csv']['paddle_number'] = dfs['invaluable_wepage_prices.csv']['paddle_number'].str.replace('#', '').astype(int)

merged_df = pd.merge(new_df, dfs['invaluable_wepage_prices.csv'], on='paddle_number', how='left')

merged_df = merged_df.drop(['lotid', 'hammer', 'premium', 'live_fee', 'total', 'username'], axis=1)

invalueablefinal = merged_df.assign(source='invaluable')


# In[ ]:


invalueablefinal['lottitle_numbers'] = invalueablefinal['lottitle_numbers'].apply(lambda x: str(x).encode())


# ### iCollector

# In[ ]:


# create a function to create a list of the text 
# hoping to use this in the future for text analysis
def get_title_list(group_df):
    return group_df['title'].tolist()

# create df from dict
df = dfs['ic-59053-qg.csv']

# create title lists df
title_lists = df.groupby('paddle').apply(get_title_list)
title_lists = title_lists.reset_index(name='title_list')

# merge the new column with the original DataFrame on the 'paddle' column

agg_funcs = {
    'invoice': 'first',
    'lotid': 'first',
    'lotnum': 'first',
    'email': 'first',
    'paddle': 'first',
    'name': 'first',
    'cellphone': 'first',
    'homephone': 'first',
    'workphone': 'first',
    'address': 'first',
    'city': 'first',
    'state': 'first',
    'country': 'first',
    'zip': 'first',
    'invoice_date': 'first',
    'auctiondatetime': 'first',
    'declaredvalue': 'first',
    'customerid': 'first',
    'siteid': 'first',
    'utm_source': 'first',
    'utm_medium': 'first',
    'utm_campaign': 'first',
    'utm_content': 'first',
    'utm_term': 'first'
}

# group the data by the unique values in the 'email' column, and apply the aggregation functions to each group
df_grouped = df.groupby('paddle').agg(agg_funcs)

df_grouped = df_grouped.reset_index(drop=True)  # Drop the existing 'paddle' column


df_with_title_lists = pd.merge(df_grouped, title_lists, on='paddle')

#read in other invoice
invoicedf = dfs['invoicesSummaryAll_Auction_59053.csv']

icollectorfinal = pd.merge(invoicedf, df_with_title_lists, on='paddle')

# final icollector
icollectorfinal = icollectorfinal.assign(source='icollector')

icollectorfinal.head()


# In[ ]:


icollectorfinal['title_list'] = icollectorfinal['title_list'].apply(lambda x: str(x).encode())


# ### Liveauctioneers

# In[ ]:


eoa_dfs = []

# loop through the dictionary keys
for key in dfs.keys():
    # check if the file name starts with 'EOA'
    if key.startswith('EOA'):
        # create a DataFrame and append it to the list
        eoa_dfs.append(dfs[key])

# concatenate all the EOA DataFrames into one big DataFrame
eoa_combined = pd.concat(eoa_dfs)


# In[ ]:


eoa_combined = eoa_combined.drop(['lot_reference_number', 'listing_agent_id', 'listing_agent', 'commission_rate', 'hammer', 'commission', 'processing_fee_3%_of_hammer_', 'sales_tax', 'net_to_pay_listing_agent', 'domestic_flat_shipping', 'paid'], axis=1)

eoa_combined.head()


# In[ ]:


eoa_combined['sale_price'] = eoa_combined['sale_price'].str.replace('$', '').replace(',', '', regex=True).astype(float)
eoa_combined['buyer_premium'] = eoa_combined['buyer_premium'].str.replace('$', '').replace(',', '', regex=True).astype(float)

grouped = eoa_combined.groupby('username').agg({'lot_number': list,
                                                     'sale_price': 'sum',
                                                     'buyer_premium': 'sum',
                                                     'first_name': 'first',
                                                     'last_name': 'first',
                                                     'paddle_number': 'first',
                                                     'email': 'first',
                                                     'account_phone': 'first',
                                                     'shipping_method': 'first',
                                                     'shipping_status': 'first',
                                                     'ship_to_phone': 'first',
                                                     'ship_to_name': 'first',
                                                     'ship_to_surname': 'first',
                                                     'company': 'first',
                                                     'address': 'first',
                                                     'city': 'first',
                                                     'state': 'first',
                                                     'country': 'first',
                                                     'postal_code': 'first',
                                                     'premium_bidder': 'first'})

# Rename the index column
grouped = grouped.rename_axis('username').reset_index()

# Print the resulting DataFrame


# In[ ]:


la_dfs = []

# loop through the dictionary keys
for key in dfs.keys():
    # check if the file name starts with 'EOA'
    if key.endswith('LA.csv'):
        # create a DataFrame and append it to the list
        la_dfs.append(dfs[key])

# concatenate all the EOA DataFrames into one big DataFrame
la_combined = pd.concat(la_dfs)


# In[ ]:


la_combined['item_total'] = la_combined['item_total'].str.replace('$', '').replace(',', '', regex=True).astype(float)
la_combined['shipping'] = la_combined['shipping'].str.replace('$', '').replace(',', '', regex=True).astype(float)
la_combined['sales_tax'] = la_combined['sales_tax'].astype(str).str.replace('$', '').replace(',', '', regex=True).astype(float)


# In[ ]:



la_combined['item_total'] = pd.to_numeric(la_combined['item_total'])
la_combined['shipping'] = pd.to_numeric(la_combined['shipping'])
la_combined['sales_tax'] = pd.to_numeric(la_combined['sales_tax'])

la_combined['invoice_total'] = (la_combined['item_total'] + la_combined['shipping'] + la_combined['sales_tax'])

la_combined['invoice_total'] = la_combined['invoice_total'].astype(float)

agg_df = la_combined.groupby('username').agg({
    'item_total': 'sum',
    'shipping': 'sum',
    'sales_tax': 'sum',
    'items': 'sum',
    'invoice_total': 'sum',
    'invoice_#': 'first',
    'invoice_status': 'first',
    'hammer_price': 'first',
    'premium': 'first',
    'bidder_name': 'first',
    'state': 'first',
    'country': 'first',
    'email': 'first',
    'paid': 'first',
    'shipping_method': 'first',
    'shipped': 'first',
    'last_payment': 'first',
    'last_payment_status': 'first',
    'autopay': 'first',
    'autopay_attempts': 'first',
    'failure_reason': 'first'
})


agg_df['invoice_total'] = (agg_df['item_total'] + agg_df['shipping'] + agg_df['sales_tax'])

agg_df['invoice_total'] = agg_df['invoice_total'].astype(float)

merged_df = grouped.merge(agg_df[['invoice_#', 'invoice_total']], on=['username'], how='left')

merged_df['source'] ='liveauctioneers'

# trying to get the date in there
# this needs to be updated or we re-do everything by day
merged_df['start_date'] = '1/27/2023'
merged_df['end_date'] = '1/29/2023'


# In[ ]:


liveauctioneers_final = merged_df


# In[ ]:


liveauctioneers_final.rename(columns={'invoice_#': 'invoice_num'}, inplace=True)


# In[ ]:


liveauctioneers_final['lot_number'] = liveauctioneers_final['lot_number'].apply(lambda x: re.sub(r'[^0-9]+', '', str(x)))


# In[ ]:


liveauctioneers_final


# ### ProxiBid

# In[ ]:


p_dfs = []

# loop through the dictionary keys
for key in dfs.keys():
    # check if the file name starts with 'EOA'
    if key.startswith('proxibid'):
        # create a DataFrame and append it to the list
        p_dfs.append(dfs[key])
# concatenate all the EOA DataFrames into one big DataFrame
p_combo = pd.concat(p_dfs)
p_combo.drop(columns=['internet_premium_%'], inplace=True)


# In[ ]:


p_combo['source'] = 'proxibid'


# In[ ]:


p_combo = p_combo.rename(columns={'internet_premium_%': 'internet_premium'})
p_combo['invoice_total'] = p_combo['invoice_total'].astype(float)
p_combo = p_combo.applymap(lambda x: str(x).replace('/', ''))


# In[ ]:


p_combo['invoice_total'].unique


# In[ ]:


pcombo = p_combo
# pcombo.drop(columns=['internet_premium'], inplace=True)
pcombo.drop(columns=['sales_tax_%'], inplace = True)


# ### HiBid

# In[ ]:


h_dfs = []

# loop through the dictionary keys
for key in dfs.keys():
    # check if the file name starts with 'EOA'
    if key.startswith('AUCTION'):
        # create a DataFrame and append it to the list
        h_dfs.append(dfs[key])
# concatenate all the EOA DataFrames into one big DataFrame
h_combo = pd.concat(h_dfs)


# In[ ]:


h_combo['source'] = 'hibid'


# In[ ]:


hcombo = h_combo


# ## Uploading to GBQ
# 
# *This is updated to keep my information safe.*

# In[ ]:


#Set Paths for GBQ
service_path = "my-service-path"
service_file = 'my-key-file' # My Key   
gbq_proj_id = 'my-gbq-proj-id' # My GBQ 
dataset_id = 'my-dataset' #Set to the Wedge

#Private Key
private_key = service_path + service_file


# In[ ]:


# Pass in our credentials so that Python has permission to access our project
credentials = service_account.Credentials.from_service_account_file(service_path + service_file)


# In[ ]:


# Establish our connection
client = bigquery.Client(credentials = credentials, project=gbq_proj_id)


# In[ ]:


# hey let's see what is there :
for item in client.list_datasets() : 
    print(item.full_dataset_id)


# In[ ]:


#check to see if there are tables in the dataset 
tables = client.list_tables(dataset_id)  

for table in tables:
    if table :
        print(table.table_id)


# In[ ]:


#Regex to rename files
file_pattern = re.compile(r"(\D{12})")


# In[ ]:


table_name = "my-table-name"
table_id = ".".join([gbq_proj_id,dataset_id,table_name])
pandas_gbq.to_gbq(table_name, table_id, project_id=gbq_proj_id, if_exists="replace")


# In[ ]:


table_name = "my-table-name"
table_id = ".".join([gbq_proj_id,dataset_id,table_name])
pandas_gbq.to_gbq(table_name, table_id, project_id=gbq_proj_id, if_exists="replace")


# In[ ]:


table_name = "my-table-name"
table_id = ".".join([gbq_proj_id,dataset_id,table_name])
pandas_gbq.to_gbq(table_name, table_id, project_id=gbq_proj_id, if_exists="replace")


# In[ ]:


# for proxibid
for col in table_name.columns:
    table_name = table_name.astype({f'{col}': 'bytes'})


# In[ ]:


table_name = "my-table-name"
table_id = ".".join([gbq_proj_id,dataset_id,table_name])
pandas_gbq.to_gbq(table_name, table_id, project_id=gbq_proj_id, if_exists="replace")


# In[ ]:


table_name = "my-table-name"
table_id = ".".join([gbq_proj_id,dataset_id,table_name])
pandas_gbq.to_gbq(table_name, table_id, project_id=gbq_proj_id, if_exists="replace")

