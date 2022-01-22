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
from  geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'londongoldenkey_com'
    start_urls = ["https://www.londongoldenkey.com/properties.asp?page=1&pageSize=50&orderBy=Id&orderDirection=DESC&propind=L&businessCategoryId=1"]
    execution_type='testing'
    country='united_kingdom'
    locale='en' 

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='details']"):
            f_url = response.urljoin(item.xpath("./div[@class='address']/a/@href").get())
            desc = item.xpath("./div[@class='description']/text()").get()
            if desc and ("flat" in desc.lower() or "apartment" in desc.lower()):
                prop_type = "apartment"
            elif desc and "room" in desc.lower():
                prop_type = "room"
            elif desc and "house" in desc.lower():
                prop_type = "house"
            else:
                prop_type = None
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"prop_type":prop_type}
            )
            
        next_page = response.xpath("//a[.='next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        prop_type = response.meta.get("prop_type")
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return

        title = response.xpath("//div[@class='headline']/text()").get()
        item_loader.add_value("title", title)
        item_loader.add_value("address", title)
        item_loader.add_value("external_link", response.url)
        externalid=response.url.split("?Id=")[-1].split("&")[0]
        if externalid:
            item_loader.add_value("external_id",externalid)
        

        if title:
            zipcode = title.split(",")[-1].strip()
            if zipcode.isalpha():
                zipcode = ""
            elif " " in zipcode:
                zipcode = zipcode.split(" ")[-1]
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", title.split(",")[-2].strip())

        item_loader.add_value("external_source", "Londongoldenkey_PySpider_"+ self.country + "_" + self.locale)

        address_=response.xpath("//div[@class='headline']/text()").get()
        latitude_longitude = response.xpath("//div[@id='map']/following-sibling::img/@onload").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('(')[1].split(',')[1].strip()

            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

                
        description = response.xpath("//div[@class='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
            square_meters = desc_html.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1].replace("*","")
            square_meters = str(int(float(square_meters.replace(',', '.')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)
            
        if "lift" in desc_html.lower():
            item_loader.add_value("elevator", True)
            
        if "washing machine" in desc_html.lower():
            item_loader.add_value("washing_machine", True)

        room_count = response.xpath("//div[@class='beds']/text()").get()
        if room_count:
            room_count = room_count.split(':')[-1].strip().replace('\xa0', '').split(' ')[0].strip()
            if 'Studio' in room_count:
                room_count = '1'
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//span[@class='displayprice']/text()").get()
        term = response.xpath("//span[@class='displaypricequalifier']/text()").get()
        if rent and term:
            rent = rent.split('£')[1].strip().replace('\xa0', '').replace(',', '')
            if not 'POA' in rent:
                rent = str(int(float(rent)))
                if 'pw' in term.lower():
                    rent = str(int(rent) * 4)
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'GBP')

        images = [x for x in response.xpath("//div[@class='propertyimagelist']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//div[@class='description']/text()[contains(.,'nfurnished')]").get()
        if furnished:
            furnished = False
            item_loader.add_value("furnished", furnished)
        else:
            furnished = response.xpath("//div[@class='description']/text()[contains(.,'urnished')]").get()
            if furnished:
                furnished = True
                item_loader.add_value("furnished", furnished)

        floor = response.xpath("//div[@class='description']/text()[contains(.,'floor')]").get()
        if floor:
            floor = floor.strip().replace('\xa0', '').split(' ')[0].lower()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//div[@class='description']/text()").getall()
        for i in parking:
            if "parking" in i.lower():
                item_loader.add_value("parking", True)

        balcony = response.xpath("//div[@class='description']/text()[contains(.,'balcony')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//div[@class='description']/text()[contains(.,'terrace')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        washing_machine = response.xpath("//div[@class='description']/text()[contains(.,'washing machine')]").get()
        if washing_machine:
            washing_machine = True
            item_loader.add_value("washing_machine", washing_machine)

        dishwasher = response.xpath("//div[@class='description']/text()[contains(.,'dishwasher')]").get()
        if dishwasher:
            dishwasher = True
            item_loader.add_value("dishwasher", dishwasher)

        item_loader.add_value("landlord_name","LONDON GOLDEN KEY")
        landlord_phone = response.xpath("//div[@class='telephone']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//div[@class='email']/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data