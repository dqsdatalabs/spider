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
import dateparser
import re

class MySpider(Spider):
    name = 'apsestate_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.apsestate.co.uk/notices?c=44&p=1&premium_property=2&q=&filter_attribute[categorical][1]=Flat&min_price=&max_price=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][3][min]=&key_fea=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.apsestate.co.uk/notices?c=44&p=1&premium_property=2&q=&filter_attribute[categorical][1]=Studio&min_price=&max_price=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][3][min]=&key_fea=",
                "property_type" : "studio"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='feature_property_list_inner']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("&p=")[0] + f"&p={page}" + "&premium_property=" + response.url.split("&premium_property=")[1]
            yield Request(
                p_url, 
                callback=self.parse, 
                meta={"property_type" : response.meta.get("property_type"), "page":page+1},
            )  
        

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Apsestate_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))

        address = "".join(response.xpath("//div[@class='property-detail-location']//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.strip().split(',')[-1].strip()
            
            if zipcode.count(" ") <= 1 and any(char.isdigit() for char in zipcode):
                item_loader.add_value("zipcode", zipcode.replace("London","").strip())
            elif zipcode.count(" ") > 1:
                zipcode = zipcode.split(" ")[-1]
                item_loader.add_value("zipcode", zipcode)
                
            city =  address.strip().split(zipcode)[0].replace(",","").strip()
            if city !="":
                item_loader.add_value("city", city.strip())

        
        latitude_longitude = response.xpath("//script[contains(.,'showMap')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('showMap(')[2].split(',')[0].strip()
            longitude = latitude_longitude.split('showMap(')[2].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        bathroom_count = response.xpath("//span[contains(.,'Bathrooms')]/../span[@class='label-content']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        description = " ".join(response.xpath("//div[@id='full_notice_description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ',description))

        if 'sq ft' in description.lower() or 'sq. ft.' in description.lower() or 'sqft' in description.lower():
            square_meters = description.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1].replace("(","")
            square_meters = str(int(float(square_meters.replace(',', '.').strip('+')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)
        if 'balcony' in description.lower():
            item_loader.add_value("balcony", True)
        if 'lift' in description.lower():
            item_loader.add_value("elevator", True)
        if ("furnished" in description.lower()) and ("unfurnished" not in description.lower()):
            item_loader.add_value("furnished", True)
        
        room_count = response.xpath("//span[contains(.,'Bedroom')]/preceding-sibling::span/text()").get()
        if room_count:
            room_count = room_count.strip().replace('\xa0', '')
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//div[@id='price_value']/text()").get()
        if rent:
            if 'pcm' in rent:
                rent = rent.split('£')[1].strip().replace('\xa0', '').strip('pcm').replace(',', '')
            if 'pw' in rent:
                rent = str(int(rent.split('£')[1].strip().replace('\xa0', '').strip('pw').replace(',', '')) * 4)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        external_id = response.xpath("//span[contains(.,'Reference number')]/following-sibling::text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        available_date = response.xpath("//span[contains(.,'Available from')]/following-sibling::text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['en'])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@id='slider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//span[contains(.,'Furnished')]/following-sibling::text()").get()
        if furnished:
            if furnished.strip().lower() == 'no':
                item_loader.add_value("furnished", False)
            elif furnished.strip().lower() == 'yes':
                item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//span[contains(.,'Furnishing')]/following-sibling::text()").get()
            if furnished:
                if furnished.strip().lower() == 'unfurnished':
                    item_loader.add_value("furnished", False)
                elif furnished.strip().lower() == 'furnished':
                    item_loader.add_value("furnished", True)

        elevator = response.xpath("//span[contains(.,'Elevator')]/following-sibling::text()").get()
        if elevator:
            if elevator.strip().lower() == 'yes':
                elevator = True
            elif elevator.strip().lower() == 'no':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        washing_machine = response.xpath("//span[contains(.,'Washing Machine')]/following-sibling::text()").get()
        if washing_machine:
            if washing_machine.strip().lower() == 'yes':
                washing_machine = True
            elif washing_machine.strip().lower() == 'no':
                washing_machine = False
            if type(washing_machine) == bool:
                item_loader.add_value("washing_machine", washing_machine)

        dishwasher = response.xpath("//span[contains(.,'Dishwasher')]/following-sibling::text()").get()
        if dishwasher:
            if dishwasher.strip().lower() == 'yes':
                dishwasher = True
            elif dishwasher.strip().lower() == 'no':
                dishwasher = False
            if type(dishwasher) == bool:
                item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("landlord_name", "APSESTATE")
        landlord_phone = response.xpath("//a[contains(@href,'tel')]/text()").getall()[-1]
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//a[contains(@href,'mailto')]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()