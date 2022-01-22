# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser
class MySpider(Spider):
    name = 'scpattonproperties_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ['https://pattonproperties.appfolio.com/listings/listings?utf8=%E2%9C%93&filters%5Border_by%5D=rent_asc&filters%5Bproperty_list%5D=&filters%5Bmarket_rent_from%5D=&filters%5Bmarket_rent_to%5D=&filters%5Bbedrooms%5D=&filters%5Bbathrooms%5D=&filters%5Bcities%5D%5B%5D=&filters%5Bpostal_codes%5D%5B%5D=&filters%5Bcats%5D=&filters%5Bdogs%5D=&filters%5Bdesired_move_in%5D=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'View')]/@href[not(contains(.,'#'))]").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        description = "".join(response.xpath("//p[contains(@class,'description')]//text() | //h1//text()").getall())
        property_type = ""
        if get_p_type_string(description):
            property_type = get_p_type_string(description)
        else: return
        
        if response.xpath("//p[contains(@class,'header__summary') and contains(.,'Studio')]").get():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", property_type)
        
        item_loader.add_value("external_source", "Scpattonproperties_PySpider_united_kingdom")
        title = response.xpath("//h1/text()[normalize-space()]").get()
        if title:
            item_loader.add_value("title", title.strip())
     
        rent = response.xpath("//div/div[.='RENT']/following-sibling::h2/text()").get()
        if rent:                              
            item_loader.add_value("rent_string", rent) 
        deposit = response.xpath("//li[contains(.,'Deposit')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit)
        description = " ".join(response.xpath("//p[contains(@class,'listing-detail__description')]//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        address = response.xpath("//h1/text()[normalize-space()]").get()
        if address:  
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())

        room_count = response.xpath("//div/div[.='BED / BATH']/following-sibling::h3/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("bd")[0])
            item_loader.add_value("bathroom_count", room_count.split("ba")[0].split(".")[0].split("/")[-1])
        square_meters = response.xpath("//div[@class='header']/p//text()").get()
        if square_meters:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sqft|sq.ft|sq ft|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)

        images = [response.urljoin(x) for x in response.xpath("//div/a[contains(@class,'swipebox')]/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        available_date = response.xpath("//li[contains(.,'Available')]/text()[not(contains(.,'Now'))]").getall()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['en'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
            
        item_loader.add_value("landlord_name", "Patton Properties")
        item_loader.add_value('landlord_phone', '(803) 256-2184')
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "building" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None