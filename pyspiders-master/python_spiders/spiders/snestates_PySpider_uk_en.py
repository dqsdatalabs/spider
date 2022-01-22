# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import js2xml
from ..loaders import ListingLoader
from ..items import ListingItem
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
import re,json
from bs4 import BeautifulSoup
import requests,time
from word2number import w2n

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) > 1:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

def getPrice(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) > 1:
        output = float(list_text[0]+list_text[1])
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

class laforet(scrapy.Spider):
    name = 'snestates_PySpider_united_kingdom_en'
    allowed_domains = ['www.snestates.com']
    start_urls = ['www.snestates.com']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'

    custom_settings = {
        "PROXY_CA_ON": True
    }

    def start_requests(self):
        url = "http://www.snestates.com/properties/to-let/?Page=1&O=Price&Dir=ASC&branch=&Country=&Location=&Town=&Area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&sleeps=&propType=&Furn=&FA=&LetType=&Cat=&Avail=&searchbymap=&locations=&SS=&fromdate=&todate=&minbudget=&maxbudget="
        yield scrapy.Request(
            url = url,
            callback=self.parse
            )

    def parse(self,response,**kwargs):
        soup = BeautifulSoup(response.body,"html.parser")
        mail = soup.find('div',class_='banner-info').find('a').text
        con = soup.find('div',class_='banner-info').text.replace(mail,'')
        con = con.strip().replace('|','').strip()
        pages = int(re.findall('\d+',soup.find('div',class_='howmany').text)[-1])

        for i in range(1,pages+1):
            url = 'http://www.snestates.com/properties/to-let/?Page={}&O=Price&Dir=ASC&branch=&Country=&Location=&Town=&Area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&sleeps=&propType=&Furn=&FA=&LetType=&Cat=&Avail=&searchbymap=&locations=&SS=&fromdate=&todate=&minbudget=&maxbudget='.format(str(i))

            yield scrapy.Request(
                url = url,
                callback=self.get_page_details
                )

    def get_page_details(self,response,**kwargs):
        soup = BeautifulSoup(response.body,"html.parser")

        for li in soup.find_all('div',class_='searchprop'):
            prop_type = li.find('div',class_='proptype').text
            rent = int(re.findall('\d+',li.find(class_='price').text.replace(',',''))[0])
            add = li.find('div',class_='address').text
            
            external_link = 'http://www.snestates.com/'+li.find('a')['href']
            external_link = external_link.split("?Page=")[0]

            if "tudiant" in prop_type.lower() or  "studenten" in prop_type.lower() and ("appartement" in prop_type.lower() or "apartment" in prop_type.lower()):
                property_type = "student_apartment"
            elif "appartement" in prop_type.lower() or "apartment" in prop_type.lower() or "flat" in prop_type.lower() or "duplex" in prop_type.lower() or "appartement" in prop_type.lower():
                property_type = "apartment"
            elif "woning" in prop_type.lower() or "maison" in prop_type.lower() or "huis" in prop_type.lower():
                property_type = "house"
            elif "chambre" in prop_type.lower() or "kamer" in prop_type.lower():
                property_type = "room"
            elif "studio" in prop_type.lower():
                property_type = "studio"
            else:
                property_type = "NA"

            if property_type in ["apartment", "house", "room", "property_for_sale", "student_apartment", "studio"]:

                yield scrapy.Request(
                    url = external_link,
                    callback=self.get_property_details,
                    meta = {"property_type":property_type,"address":add,"rent":rent}
                    )

    def get_property_details(self, response, **kwargs):
        item = ListingItem()
        soup = BeautifulSoup(response.body,"html.parser")

        
        
        if soup.find('div',class_='description'):
            description = soup.find('div',class_='description').text.strip()
            
            room_count = response.xpath("//span[@class='bedswrap']/text()").get()
            if room_count:
                if "studio" in room_count.lower():
                    item["room_count"] = 1
                else:
                    item["room_count"] = int(room_count.strip().split(" ")[0])
            elif "studio" in description.lower():
                item["room_count"] = 1
            elif "bedroom" in description:
                room = description.split("bedroom")[0].replace("double","").strip().split(" ")[-1].replace("-","")
                if room.isdigit():
                    item["room_count"] = int(room)
                else:
                    try:
                        item["room_count"] = w2n.word_to_num(room)
                    except: pass
            
            if "garage" in description.lower() or "parking" in description.lower() or "autostaanplaat" in description.lower():
                item["parking"]=True
            if "terras" in description.lower() or "terrace" in description.lower():
                item["terrace"]=True
            if "balcon" in description.lower() or "balcony" in description.lower():
                item["balcony"]=True
            if "zwembad" in description.lower() or "swimming" in description.lower():
                item["swimming_pool"]=True
            if "gemeubileerd" in description.lower() or "furnished" in description.lower():
                item["furnished"]=True
            if "machine à laver" in description.lower():
                item["washing_machine"]=True
            if "lave" in description.lower() and "vaisselle" in description.lower():
                item["dishwasher"]=True
            if "lift" in description.lower():
                item["elevator"]=True

            item["description"] = description

        if soup.find('img',src='images/trandot.gif'):
            lat = soup.find('img',src='images/trandot.gif')['onload'].replace('javascript:loadGoogleMap(','').split(',')[0]
            lng = soup.find('img',src='images/trandot.gif')['onload'].replace('javascript:loadGoogleMap(','').split(',')[1]

            item["latitude"] = lat
            item["longitude"] = lng
            
        img = set()
        for im in soup.find('div',id='photocontainer').find_all('img'):
            img.add(im['src'])
        img = list(img)
        if img:
            item["images"] = img
            item["external_images_count"] = len(img)

        item["property_type"] = response.meta["property_type"]
        rent = "".join(response.xpath("//div[@class='price']//text()").getall())
        if rent:
            if "pw" in rent:
                rent = "".join(filter(str.isnumeric, rent.split('.')[0].replace(',', '').replace('\xa0', '')))
                item["rent"] = str(int(float(rent)*4))
            else:
                rent = rent.replace(",","")
                item["rent"] = rent
        furnished = "".join(response.xpath("//span[@class='furnishwrap']/text()[contains(.,' furnished')]").getall())
        if furnished:
            item["furnished"] = True
        # item["rent"] = response.meta["rent"]
        item["external_link"] = response.url
        item["landlord_name"] = "SN ESTATES LONDON LTD"
        item["landlord_phone"] = "02070961297"
        item["landlord_email"] = "sales@snestates.com"
        item["external_source"] = "snestates_PySpider_united_kingdom_en"
        item["currency"] = "GBP"
        item["title"] = response.meta["address"]
        item["address"] = response.meta["address"]
        city = response.meta["address"].split(",")[-1].strip().replace(".","")
        if not city.replace(" ","").isalpha() and len(city.split(","))>=2:
            item["zipcode"] = city
            item["city"] = response.meta["address"].split(",")[-2].strip().replace(".","")
        elif " " in city:
            if not city.split(" ")[-1].isalpha():
                zipcode = city.split(" ")[-1]
                if not city.split(" ")[-2].isalpha():
                    zipcode = f"{city.split(' ')[-2]} {city.split(' ')[-1]}"
                city = city.split(zipcode)[0].strip()
                item["city"] = city
                item["zipcode"] = zipcode
            else: item["city"] = city
        else: item["city"] = city

        yield item
