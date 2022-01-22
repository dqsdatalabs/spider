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


class MySpider(Spider):
    name = 'greaterlondonproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'        
    external_source = "Greaterlondonproperties_PySpider_united_kingdom_en"
    custom_settings = {
        "PROXY_ON":"True",
        # "HTTPCACHE_ENABLED":False
    }
    start_urls = ["https://www.greaterlondonproperties.co.uk/property-status/to-let/"]
    
    def start_requests(self):
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
        }
        yield Request(self.start_urls[0], headers=headers, callback=self.parse)
    
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//h3[@class='entry-title']/a"):
            f_url = response.urljoin(item.xpath("./@href").get())
            prop_type = item.xpath("./text()").get()
            if prop_type and ("flat" in prop_type.lower() or "studio" in prop_type.lower() or "apartment" in prop_type.lower()):
                prop_type = "apartment"
            elif prop_type and ("House" in prop_type or "Penthouse" in prop_type) :
                prop_type = "house"
            else: prop_type = None
            if prop_type:
                yield Request(
                    f_url, 
                    callback=self.populate_item, 
                    meta={"property_type" : prop_type},
                )
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop_type = response.meta.get('property_type')
        if prop_type:
            item_loader.add_value("property_type", prop_type)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")

        item_loader.add_value("external_source", self.external_source)

        bathroom_count = response.xpath("//span[contains(text(),'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split('Bathroom')[0].strip())
        
        parking = response.xpath("//li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", ",".join(address.split(',')[1:]).strip())
            address = " ".join(address.split(',')[1:]).strip().replace("  "," ")
            zipcode = address.split(" ")[-1].strip()
            if zipcode.isalpha():
                item_loader.add_value("city", address.split(" ")[-1])
            else:
                item_loader.add_value("zipcode", address.split(" ")[-1].strip())
                item_loader.add_value("city", address.split(" ")[-2])
            
      
        latitude_longitude = response.xpath("//script[@id='property-google-map-js-extra']/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"lat":"')[1].split('"')[0].strip()
            longitude = latitude_longitude.split('"lang":"')[1].split('"')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        square_meters = response.xpath("//div[@id='description']//text()").getall()
        if square_meters:
            sqm = ''
            for text in square_meters:
                if 'sq ft' in text.lower():
                    sqm = text.lower().split('sq ft')[0].strip().split(' ')[-1].strip().replace(',', '').replace('.', '')
                    break
            if sqm != '':
                sqm = str(int(float(sqm) * 0.09290304))
                item_loader.add_value("square_meters", sqm)

        rent = response.xpath("//span[contains(@class,'pcm')]/text()").get()
        if rent:
            rent = rent.split('pcm')[0].split('£')[1].strip().replace('\xa0', '')
            rent = rent.replace(',', '').replace('.', '')
            rent = rent.split(' ')
            reg_rent = []
            for i in rent:
                if i.isnumeric():
                    reg_rent.append(i)
            r = "".join(reg_rent)
            item_loader.add_value("rent", r)
            item_loader.add_value("currency", 'GBP')

        external_id = response.url.split('-')[-1].strip().strip('/')
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@id='description']//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        
        if desc_html:
            if 'floor' in desc_html.lower():
                floor_number = "".join(filter(str.isnumeric, desc_html.lower().split('floor')[0].strip().split(' ')[-1])).strip()
                if floor_number:
                    item_loader.add_value("floor", floor_number)
            if 'dishwasher' in desc_html.lower():
                item_loader.add_value("dishwasher", True)
            if 'washing machine' in desc_html.lower():
                item_loader.add_value("washing_machine", True)

        room_count = response.xpath("//div[contains(@class,'property-meta')]/span[contains(.,'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.split(':')[-1].strip().replace('\xa0', '').split(' ')[0].strip()
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)
        else:
            room = response.xpath("//div[@id='description']//text()[contains(.,'studio') or contains(.,'Studio')]").get()
            if room:
                room_value = "1"
                item_loader.add_value("room_count", room_value)

        available_date = response.xpath("//strong[contains(.,'Available on')]/following-sibling::text()[1]").get()
        if available_date:
            available_date = available_date.split(':')[-1].strip()
            if len(available_date.split('-')) > 2 or len(available_date.split('.')) > 2 or len(available_date.split('/')) > 2:
                if available_date.isalpha() != True:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//a[contains(@data-rel,'gallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = response.xpath("//div[@class='floor-plan']/a/@href").get()
        if floor_plan_images:
            floor_plan_images = floor_plan_images.strip()
            item_loader.add_value("floor_plan_images", floor_plan_images)

        furnished = response.xpath("//strong[contains(.,'Furnished')]/following-sibling::text()[1]").get()
        if furnished:
            if furnished.strip(':').strip() == 'Furnished':
                furnished = True
            elif furnished.strip(':').strip() == 'Unfurnished':
                furnished = False
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)

        landlord_name = response.xpath("//a[@class='office']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip().strip(':')
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//a[@class='phone']/span/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        item_loader.add_value("landlord_email", "info@glp.co.uk")
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data

