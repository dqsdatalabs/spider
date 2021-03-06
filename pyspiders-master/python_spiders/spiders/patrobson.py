# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import js2xml
import re
import math
import json
from bs4 import BeautifulSoup
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
    name = "patrobson_com_PySpider_united_kingdom_en"#com
    allowed_domains = ['www.patrobson.com']
    start_urls = ['www.patrobson.com']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'

    def start_requests(self):
        urls = {'week':'https://www.patrobson.com/student-lettings/?loc=&type=&bmin=&pmin=40&pmax=300&radius=0.75&order=desc&view=grid&show_all=1',
                'month':'https://www.patrobson.com/lettings/?loc=&type=&bmin=&pmin=300&pmax=4000&radius=0.75&order=desc&view=grid&show_all=1'}
        for type_,url in urls.items():
            yield scrapy.Request(
                url=url, 
                callback=self.parse,
                meta = {"type":type_})

    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'column-')]/a"):
            f_url = response.urljoin(item.xpath("./@href").get())
            status = item.xpath(".//span[@class='status']/text()").get()
            if not status:
                yield scrapy.Request(
                    url=f_url, 
                    callback=self.get_property_details, 
                    meta={'type': response.meta.get("type")})

    def get_property_details(self, response):
        item = ListingItem()
        soup1 = BeautifulSoup(response.body,"html.parser")

        type_ = response.meta.get("type")
        external_link = response.url
        item["external_link"] = external_link

        item["title"] = soup1.find("div", class_="title").find("h1").text.strip()
        item["address"] = soup1.find("div", class_="title").find("h1").text.strip()
        splt_txt = item["address"].split(",")[-1].split("/")[-1]
        item["city"] = splt_txt.strip()
        price = getSqureMtr(soup1.find("span", class_="price test").text.strip().replace(',', ''))
        if type_ == "week":
            item["rent"] = price*4
        elif type_ == "month":
            item["rent"] = price


        floor_image=[]
        if soup1.find("a",class_="floor-plan-img cboxElement"):
            floor_image = [soup1.find("a",class_="floor-plan-img cboxElement")["href"]]
            item["floor_plan_images"] = floor_image

        temp_dic = {}
        for desc in soup1.find("div", class_="desc").findAll("li"):
            try:
                if desc.text.strip().split(" ")[1]:
                    temp_dic[desc.text.strip().split(" ")[1]] = desc.text.strip().split(" ")[0]
            except Exception as e:
                temp_dic[desc.text.strip().split(" ")[0]] = desc.text.strip().split(" ")[0]
            
        temp_dic = cleanKey(temp_dic)

        if "bedrooms" in temp_dic:
            item["room_count"] = getSqureMtr(temp_dic["bedrooms"])
        elif "bedroom" in temp_dic:   
                item["room_count"] = getSqureMtr(temp_dic["bedroom"])    
        if "bathrooms" in temp_dic:
            item["bathroom_count"] = getSqureMtr(temp_dic["bathrooms"])

        elif "bathroom" in temp_dic:
            item["bathroom_count"] = getSqureMtr(temp_dic["bathroom"])
        else:
            pass

        description = soup1.find("div", class_="text-content").find("p").text.strip()
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

        external_id = [value for key, value in temp_dic.items() if 'ref' in key.lower()]
        if external_id:
            item["external_id"] = external_id[0].replace('Ref:', '')

        try:
            if soup1.find("div", class_="tab-energy-reports").find("img"):
                item["energy_label"] = soup1.find("div", class_="tab-energy-reports").find("img")['src'].replace('.png', '').split('_')[-2]
        except Exception as e:
            pass

        images = []
        for img in soup1.find("div", class_="large-bg-images").findAll("div", class_="slide"):
            if "lettings-property" in external_link:
                images.append(img['style'].replace("background-image:url('", '').replace("');", ''))
            else:
                images.append(img['style'].replace("background-image:url('//", 'https://www.').replace("');", ''))
        if images:
            item["images"]= images
        if images or floor_image:
            item["external_images_count"]= len(images) + len(floor_image)

        item["currency"]='GBP'
        item["external_source"] = 'patrobson_com_PySpider_united_kingdom_en'
        
        item["landlord_phone"] = "01912090100"
        # item["landlord_email"] = "info@cubixestateagents.co.uk"
        item["landlord_name"] = "Pat Robson"

        if 'city-centre' in item["external_link"]:
            item["landlord_email"] = 'citycentre@patrobson.com'
        if 'jesmond' in item["external_link"]:
            item["landlord_email"] = 'jesmond@patrobson.com' 
        if 'gosforth' in item["external_link"]:
            item["landlord_email"] = 'gosforth@patrobson.com'        

        if property_type in ["apartment", "house", "room", "property_for_sale", "student_apartment", "studio"]:
            yield item

