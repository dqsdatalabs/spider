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
import re

class MySpider(Spider): 
    name = 'kbroman_co_uk'
    start_urls = ["http://www.birminghamstudentaccommodation.com/?s=Search1&propery_type=residential-let&propery_contract_type=all&propery_location=Location&beds=Any&baths=Any&min_price=Any&max_price=Any&min_area&max_area&search_by=Search+By&search_by_keyword"]
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page",2)

        seen = False
        for item in response.xpath("//div[@class='property-info']/h3/a"):
            f_url = response.urljoin(item.xpath("./@href").get())
            
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
            seen = True
        
        if page == 2 or seen:
            url = f"http://www.birminghamstudentaccommodation.com/?s=Search1&propery_type=residential-let&propery_contract_type=all&propery_location=Location&beds=Any&baths=Any&min_price=Any&max_price=Any&min_area&max_area&search_by=Search+By&search_by_keyword&paged={page}"
            yield Request(
                url=url,
                callback=self.parse,
                meta={"page":page+1}
            )     

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        let = response.xpath("//div[@class='single-property']/div[@class='price']/span/text()[.='NOW LET']").get()
        if let:
            return
        item_loader.add_value("external_source", "Kbromanco_PySpider_"+ self.country + "_" + self.locale)

        f_text = "".join(response.xpath("//div[@id='description']/p/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        item_loader.add_value("external_link", response.url)

        address = response.xpath("//h2[@class='page-title']/text()").get()
        if address:
            address = address.split(',')[1].strip()
            item_loader.add_value("address", address)
        zipcode=response.xpath("//div[@class='property-amenities clearfix']//span[2]/strong/text()").get()
        if zipcode:
            zipcode=re.search("[A-Z]+[0-9A-Z].*",zipcode)
            if zipcode:
                item_loader.add_value("zipcode",zipcode.group())

        description = response.xpath("//div[@id='description']/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            item_loader.add_value("description", desc_html)

        if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
            square_meters = desc_html.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
            square_meters = str(int(float(square_meters.replace(',', '.').strip('+').strip('(')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)
        bathroom_count =" ".join(response.xpath("//div[contains(@class,'single-property')]//span[@class='baths']//text()").get())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bath")[0])
        room_count = response.xpath("//span[@class='area']/text()[contains(.,'Bedroom')]").get()
        if room_count:
            room_count = room_count.strip().replace('\xa0', '').split(' ')[0].strip()
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)
        elif response.xpath("//span[@class='area']/text()[contains(.,'Studio')]").get():
            item_loader.add_value("room_count","1")
        rent = response.xpath("//div[@class='single-property']/div[@class='price']/span/text()").get()
        if rent:
            if 'pcm' in rent.lower():
                rent =  str(int(float(rent.split('£')[-1].strip().replace('\xa0', '').replace(',', '').split(' ')[0])))
            if 'p.w' in rent.lower() or 'p/w' in rent.lower() or 'pw' in rent.lower() :
                rent = str(int(float(rent.split('£')[-1].strip().replace('\xa0', '').replace(',', '').split(' ')[0])) * 4)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        title = response.xpath("//h2/text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id = response.xpath("//div[@id='address']/text()[contains(.,'ID')]").get()
        if external_id:
            external_id = external_id.split(':')[-1].strip()
            item_loader.add_value("external_id", external_id)
        
        city = response.xpath("//h2/span[@class='location']/text()").get()
        if city:
            city = city.strip()
            item_loader.add_value("city", city)
        zipcode = response.xpath("//div[@id='address']/text()[contains(.,'Pin Code:')]").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(":")[-1].strip())

        images = [x for x in response.xpath("//div[@id='property-images']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        parking = " ".join(response.xpath("//span[@class='parking']/strong/text()").getall())
        if parking:
            if "AVAILABLE" in parking.upper()  or "ON STREET" in parking.upper():
                parking = True
            elif parking.strip().lower() == 'n/a':
                parking = False        
            if type(parking) == bool:
                item_loader.add_value("parking", parking)
        washing_machine = response.xpath("//p[contains(.,'Facilities:')]/following-sibling::p[1]/text()[contains(.,'Washing Machine')]").get()
        if washing_machine:
            washing_machine = True
            item_loader.add_value("washing_machine", washing_machine)

        dishwasher = response.xpath("//p[contains(.,'Facilities:')]/following-sibling::p[1]/text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            dishwasher = True
            item_loader.add_value("dishwasher", dishwasher)

        item_loader.add_value("landlord_name", "KBROMAN")
        landlord_phone = response.xpath("//i[contains(@class,'fa-phone')]/../span/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//i[contains(@class,'fa-envelope')]/../span/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"    
    else:
        return None
