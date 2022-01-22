# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'nationalre_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        yield Request("https://nationalre.com.au/for-lease/for-rent-residential/page/1/", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'body-right')]//a[contains(.,'Details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)

        next_button = response.xpath("//a[@rel='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//li[@class='prop_type']/text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        
        item_loader.add_value("external_source", "Nationalre_Com_PySpider_australia")
        
        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        
        rent = response.xpath("//div[@class='header-right']/span[contains(@class,'price')]/text()").get()
        if rent:
            price = rent.lower().split(" ")[0].replace("$","").replace("pw","")
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency","AUD")
        
        address = response.xpath("//div[@class='header-left']/address/text()").get()
        item_loader.add_value("address", address)
        
        city = response.xpath("//li/strong[contains(.,'Suburb')]/following-sibling::text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        zipcode = response.xpath("//li/strong[contains(.,'Postcode')]/following-sibling::text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        room_count = response.xpath("//li/strong[contains(.,'Bedroom')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//li/strong[contains(.,'Bathroom')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        parking = response.xpath("//li/strong[contains(.,'Parking')]/following-sibling::text()").get()
        if parking and parking.strip() !="0":
            item_loader.add_value("parking", True)
        
        import dateparser
        available_date = response.xpath("//div[@id='description']/p//text()[contains(.,'Available')]").get()
        if available_date:
            available_date = available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        external_id = response.xpath("//li/strong[contains(.,'ID')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        description = " ".join(response.xpath("//div[@id='description']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        if "sqm" in description:
            item_loader.add_value("square_meters", description.split("sqm")[0].strip().split(" ")[-1])
        
        images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[@class='detail-slider-wrap']//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'property_lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('property_lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('property_lng":"')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        name = response.xpath("//i[contains(@class,'user')]/following-sibling::text()").get()
        if name:
            item_loader.add_value("landlord_name", name.strip())
        
        phone ="".join(response.xpath("//i[contains(@class,'phone')]/following-sibling::text()").getall())
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        else:
            item_loader.add_value("landlord_phone", "02 9721 1611")
            
        item_loader.add_value("landlord_email", "admin@nationalre.com.au")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None