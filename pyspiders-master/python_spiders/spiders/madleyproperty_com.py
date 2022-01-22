# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from html.parser import HTMLParser 
import dateparser
from word2number import w2n
from  geopy.geocoders import Nominatim 
from math import floor
import math

class MySpider(Spider):
    name = 'madleyproperty_com'
    start_urls = ["https://www.madleyproperty.com/search/all-properties-to-rent/"]
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='details']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
            
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        prop_type = "".join(response.xpath("//div[@class='doublecol']/p/text()").extract())
        if prop_type and "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        elif prop_type and ("apartment" in prop_type.lower() or "flat" in prop_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower():
            item_loader.add_value("property_type", "house")
        elif prop_type and "room" in prop_type.lower():
            item_loader.add_value("property_type", "room")
        else:
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1].replace("/",""))

        item_loader.add_value("external_source", "Madleyproperty_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)
 
        latitude_longitude = response.xpath("//script[contains(.,'add_map')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        address = response.xpath("//div[@class='label']/h1/text()").get()
        if address:
            latitude = latitude_longitude.split('(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(",")[-1].strip())           
            item_loader.add_value("city", address.split(",")[0].strip())           
        
        description = response.xpath("//div[@class='doublecol']/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        # if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
        #     square_meters = desc_html.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
        #     square_meters = str(int(float(square_meters.replace(',', '.')) * 0.09290304))
        #     item_loader.add_value("square_meters", square_meters)
 
         
        if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
            square_meters = desc_html.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
            square_meters0=square_meters.replace(',', '')
            if len(square_meters0)<5:    
                square_meters1= str(int(float(square_meters.replace(',', '.')) * 0.09290304))
                item_loader.add_value("square_meters", square_meters1) 
            if len(square_meters0)>4: 
                item_loader.add_value("square_meters", str(math.floor(float(square_meters0)* 0.09290304)))
 
        room_count = response.xpath("//span[contains(.,'Bed')]/text()").re_first(r"\d+")
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "studio" in "".join(description).lower():
            item_loader.add_value("room_count", "1")
        elif 'suite' in "".join(description).lower():
            item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("//li[contains(.,'bedroom')]/text()").get()
            if room_count:
                bedroom = room_count.split('bedroom')[0].strip().replace('\xa0', '').split(' ')
                for b in bedroom:
                    err = False 
                    try:
                        room_count = str(int(float(w2n.word_to_num(room_count))))
                    except:
                        err = True
                    if not err:
                        item_loader.add_value("room_count", room_count)
                        break

        bathroom_count = response.xpath("//span[contains(.,'Bath')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//span[@class='price']/text()").get()
        term = response.xpath("//span[@class='pricesuffix']/text()").get()
        if rent and term:
            rent = rent.split('£')[1].strip().replace('\xa0', '').replace(',', '')
            rent = str(int(float(rent)))
            if 'pw' in term.lower():
                rent = str(int(rent) * 4)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        available_date = response.xpath("//li[contains(.,'Available')]/text()").get()
        if available_date:
            try:
                if ":" in available_date:
                    available_date = available_date.split(":")[1].strip()
                elif "-" in available_date:
                    available_date = available_date.split('-')[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['en'])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            except:
                pass

        images = [x.split("url('")[1].split(";")[0] for x in response.xpath("//div[contains(@class,'cycle-container')]//@style[contains(.,'background')]").getall()]
        if images:
            item_loader.add_value("images", images)

        floor = response.xpath("//li[contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.split("Floor")[0].strip().split(" ")[-1].replace("High","")
            item_loader.add_value("floor", floor)
        energy_label = response.xpath("//div[@class='doublecol']/p[contains(.,'EPC')]/text()").get()
        if energy_label:
            energy_label = energy_label.strip().replace('\xa0', '').split(' ')[-1].strip()
            item_loader.add_value("energy_label", energy_label)

        furnished = response.xpath("//li[contains(.,'Furnished')]").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        parking = response.xpath("//li[contains(.,'arking')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        balcony = response.xpath("//li[contains(.,'alcony')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//li[contains(.,'errace')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//li[contains(.,'wimming pool')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        landlord_phone = response.xpath("//span[@class='tel']/a[@class='tellink']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        item_loader.add_value("landlord_name", "MP Estate Agents")
        item_loader.add_value("landlord_email", "info@madleyproperty.com")
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data