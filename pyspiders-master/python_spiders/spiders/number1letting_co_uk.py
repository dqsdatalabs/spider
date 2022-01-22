# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'number1letting_co_uk'
    execution_type = "testing"
    country = "united_kingdom"
    locale = "en"
    thousand_separator = ','
    scale_separator = '.'    
    start_urls = ["https://www.number1letting.co.uk/properties"]
    
    formdata = {
        "price_from": "0",
        "price_to": "5000",
        "bedrooms": "0"
    }

    def start_requests(self):
        yield FormRequest(self.start_urls[0], callback=self.parse, formdata=self.formdata)
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='image']"):
            status = item.xpath("./div[@class='comcorner']").get()
            if status:
                continue
            follow_url = response.urljoin(item.xpath("./a[@class='view']/@href").get())
            yield Request(follow_url, callback=self.populate_item)
      
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Number1letting_Co_PySpider_united_kingdom")
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        f_text = " ".join(response.xpath("//a[@class='house']/text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//section[@id='details-section']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return

        rent = "".join(response.xpath("normalize-space(//span[@class='price']/text())").getall())
        if rent:
            price = rent.split("PCM")[0].replace("POA","").strip()
            item_loader.add_value("rent_string",price.strip())
        else:
            item_loader.add_value("currency","EUR") 

        room_count = "".join(response.xpath("//a[@class='beds']/text()").getall())
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0].strip())  

        bathroom_count = "".join(response.xpath("//a[@class='baths']/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0].strip()) 

        floor = "".join(response.xpath("//div[contains(@class,'bullets')]/p/a[contains(.,'Floor')]/text()").getall())
        if floor:
            item_loader.add_value("floor",floor.split(" ")[0].strip()) 

        address = "".join(response.xpath("//h4[@class='property-name']/text()").getall()) 
        if address:
            zipcode = address.split(",")[-1].strip()
            city = address.split(",")[-2].strip()
            item_loader.add_value("address", re.sub("\s{2,}", " ", address.strip()))          
            item_loader.add_value("city", city)

        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        item_loader.add_xpath("zipcode", "//section[@id='map-section']/input[@id='postcode']/@value")
        available_date=response.xpath("//div[contains(@class,'bullets')]/p/a[contains(.,'Available')]/text()").get()
        if available_date:
            date2 =  available_date.split(" ")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        description = " ".join(response.xpath("//section[@id='details-section']/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [x for x in response.xpath("//ul[@class='slides']/li/@data-thumb").getall()]
        if images:
            item_loader.add_value("images", images)


        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@class='links ']/a[contains(.,'Floor')]/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_xpath("latitude", "//section[@id='map-section']/input[@id='lat']/@value")
        item_loader.add_xpath("longitude", "//section[@id='map-section']/input[@id='long']/@value")

        terrace = response.xpath("//a[@class='house']/text()[contains(.,'Terraced')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_phone", "01977 799333")
        item_loader.add_value("landlord_name", "Number 1 Letting")
        item_loader.add_value("landlord_email", "info@number1letting.co.uk")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None