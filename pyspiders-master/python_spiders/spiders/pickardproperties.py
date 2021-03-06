# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import js2xml
import re
import math
import json
from bs4 import BeautifulSoup
import requests
from ..loaders import ListingLoader
from ..items import ListingItem
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date


def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode, city = zip_city.split(" ")
    return zipcode, city

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

def getPrice(text):
    list_text = re.findall(r'\d+',text)


    if "," in text:
        if len(list_text) > 1:
            output = float(list_text[0]+list_text[1])
        elif len(list_text) == 1:
            output = int(list_text[0])
        else:
            output=0
    elif "." in text:
        if len(list_text) > 1:
            output = float(list_text[0]+"."+list_text[1])
        elif len(list_text) == 1:
            output = int(list_text[0])
        else:
            output=0


    return int(output)


def cleanText(text):
    text = ''.join(text.split())
    text = re.sub(r'[^a-zA-Z0-9]', ' ', text).strip()
    return text.replace(" ","_").lower()


def num_there(s):
    return any(i.isdigit() for i in s)


def cleanKey(data):
    if isinstance(data,dict):
        dic = {}
        for k,v in data.items():
            dic[cleanText(k)]=cleanKey(v)
        return dic
    else:
        return data



class QuotesSpider(scrapy.Spider):
    name = "pickardproperties_PySpider_united_kingdom_en"
    allowed_domains = ['www.pickardproperties.co.uk']
    start_urls = ['www.pickardproperties.co.uk']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    external_source = "Pickardproperties_PySpider_united_kingdom_en"

    def start_requests(self):
        type = [1,2]
        for t in type:
            url ='https://www.pickardproperties.co.uk/search/?page=1&type_id={}&bedrooms=&area_id=&min_price=&max_price=&page=1&order_by=Sort'.format(t)

            yield scrapy.Request(
                url=url, 
                callback=self.parse,
                meta={'t' : t})

    def parse(self, response):
        soup = BeautifulSoup(response.body)
        t = response.meta.get('t')
        imax = 0
        for page in soup.find("div", class_="pagination").findAll("li"):
            if page.text and int(page.text) > imax:
                imax = int(page.text)
        
        for i in range(1, imax+1):
            sub_url ='https://www.pickardproperties.co.uk/ajax/properties/search.ajax.php'
            data = {
            "type_id":"{}".format(t),
            "bedrooms" : "",
            "area_id" :  "",
            "min_price" : "",
            "max_price" : "",
            "page" : "{}".format(i),
            "order_by": "Sort"
            }
            # print(data)
            headr = {"X-Requested-With": "XMLHttpRequest",
                        "Referer": "https://www.pickardproperties.co.uk/search/?page=1&type_id=1&bedrooms=&area_id=&min_price=&max_price=&page=1&order_by=Sort",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36"}
            yield scrapy.FormRequest(
                url=sub_url, 
                callback=self.get_external_link,
                headers = headr,
                formdata = data)

    def get_external_link(self, response):

        json_response = json.loads(response.body)
        prop_items = json_response["items"]

        for prop in prop_items:

            external_id = prop.get("property")["id"]
            external_link = 'https://www.pickardproperties.co.uk/property-details/' + prop.get("property")["uri"]
            title = prop.get("property")["title"]
            address = prop.get("property")["address"]
            zipcode = prop.get("property")["postcode"]
            city = prop.get("property")["city"]
            rent = prop.get("property")["rent_all_inc"]
            room_count = int(prop.get("property")["bedrooms"])
            date_available = prop.get("property")["date_available"]
            lat = prop.get("property")["latitude"]
            lng = prop.get("property")["longitude"]

            yield scrapy.Request(
                url=external_link, 
                callback=self.get_property_details, 
                meta={'external_link': external_link,
                    'external_id' : external_id,
                    'title' : title,
                    'address' : address,
                    'zipcode' : zipcode,
                    'city' : city,
                    'rent' : rent,
                    'room_count' : room_count,
                    'date_available': date_available,
                    'lat': lat,
                    'lng': lng})

    def get_property_details(self, response):
        item = ListingItem()
        soup2 = BeautifulSoup(response.body)

        external_link = response.meta.get('external_link')
        # print(external_link)
        item["external_link"] = external_link
        item["external_id"] = response.meta.get('external_id')
        item["title"] = response.meta.get('title')
        item["address"] = response.meta.get('address')
        item["zipcode"] = response.meta.get('zipcode')
        item["city"] = response.meta.get('city')
        # if getSqureMtr(response.meta.get('rent')):
        #     item["rent"] = getSqureMtr(response.meta.get('rent'))
        rent = response.xpath("//div[contains(@class,'property-spec-box ')]//i[contains(@class,'calendar')]//following-sibling::span[contains(.,'??')]/text()").get()
        if rent:
            rent_pw = response.xpath("//div[contains(@class,'property-spec-box ')]//i[contains(@class,'calendar')]//following-sibling::span[contains(.,'??')]/span//text()").get()
            if rent_pw and "pw" in rent_pw.lower():
                rent = rent.split("??")[1].split(".")[0].replace(",","")
                item["rent"] = int(rent)*4
            else:                
                rent = rent.split("??")[1].split(".")[0].replace(",","")
                item["rent"] = rent
        else:
            rent = response.xpath("//i[contains(@class,'calendar')]//following-sibling::span[contains(.,'??')]/text()").get()
            if rent:
                # rent_pw = response.xpath("//div[contains(@class,'property-spec-box ')]//i[contains(@class,'calendar')]//following-sibling::span[contains(.,'??')]/span//text()").get()
                if rent and "pw" in rent.lower():
                    print(rent)
                    rent = rent.split("??")[1].split(".")[0].replace(",","")
                    item["rent"] = int(rent)*4
                else:                
                    rent = rent.split("??")[1].split(".")[0].replace(",","")
                    item["rent"] = rent

        if response.meta.get('room_count'):
            item["room_count"] = response.meta.get('room_count')
        item["latitude"] = response.meta.get('lat')
        item["longitude"] = response.meta.get('lng')
        item["currency"]='EUR'
        item["landlord_phone"] = "8164306"
        item["landlord_name"] = "Pickard Properties"


        if soup2.find("i",alt="Bathrooms"):
            bath_count =  int(soup2.find("i",alt="Bathrooms").find_next("span").text.strip())
            item["bathroom_count"] = bath_count

        # if "rent" not in item:
        #     if soup2.find("i",class_="p-calendar").find_next("span"):
        #         text_price = soup2.find("i",class_="p-calendar").find_next("span").text.strip()
        #         if getPrice(text_price) and "pcm" in text_price.lower():
        #             item["rent"] = getPrice(text_price)
        #         elif getPrice(text_price) and "pppw" in text_price.lower():
        #             item["rent"] = getPrice(text_price)*4

        images = []
        for img in soup2.findAll("div", class_="block-grid-item property-image-thumbnail-container"):
            images.append('https://www.pickardproperties.co.uk'+img.find("a")['href'].strip())
        if images:
            item["images"]= images

        floor_image = []
        if soup2.find("div",id="floor-plan"):
            url_text = soup2.find("div",id="floor-plan").find("a")["href"]
            floor_image = ["https://www.pickardproperties.co.uk"+url_text]
            item["floor_plan_images"] = floor_image
        if floor_image or images:
            item["external_images_count"]= len(images)+len(floor_image)

        if soup2.find("div", id="epc"):
            item["energy_label"] = soup2.find("div", id="epc").find("a")['href'].split('_')[-1].replace('.png', '')[:2]

        for dep in soup2.find("div", class_="property-spec-boxes block-grid-lg-2 block-grid-md-1 block-grid-sm-1 block-grid-xs-2").findAll("div", class_="block-grid-item"):
            if re.findall("(.+)deposit",str(dep.text.strip())):
                item["deposit"] = getPrice(re.findall("(.+)deposit",str(dep.text.strip()))[0])
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//i[contains(@class,'calendar')]//following-sibling::span[contains(.,'available')]/text()").get()
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item["available_date"] = date2

        description = soup2.find("div", class_="more-property").text.strip()
        item["description"] = description
        if "swimming" in description.lower():
            item["swimming_pool"] = True
        if "furnish" in description.lower():
            item["furnished"]=True
        if "parking" in description.lower():
            item["parking"] = True
        if "balcony" in description.lower():
            item["balcony"]=True
        if "lift" in description.lower() or "elevator" in description.lower():
            item["elevator"]=True

        if "flat" in description.lower() or "apartment" in description.lower():
            property_type = "apartment"
        elif "house" in description.lower() or "maisonette" in description.lower() or "bungalow" in description.lower():
            property_type = "house" 
        else:
            property_type = "NA"
        item["property_type"] = property_type

        item["external_source"] = 'pickardproperties_PySpider_united_kingdom_en'

        if property_type in ["apartment", "house", "room", "property_for_sale", "student_apartment", "studio"]:
            yield item
