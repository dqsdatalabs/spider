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

class MySpider(Spider):
    name = 'mcinnes_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_url = "https://www.mcinnes.com.au/tenants/residential-rentals/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='thumbnail-mode']/div[contains(@class,'listing')]//a[contains(.,'View Details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)

        next_page = response.xpath("//a[@title='Next Page']/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        property_type = "".join(response.xpath("//div[@class='copy']//text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Mcinnes_Com_PySpider_australia")
        item_loader.add_value("external_id", response.url.split("/")[-2])

        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        if title:
            item_loader.add_value("address", title)
            city = ""
            for i in title.split(" "):
                if not i.isdigit():
                    city = i+" "
            item_loader.add_value("city", city.strip())
                
        zipcode = response.xpath("//script[contains(.,'postalCode')]/text()").get()
        if zipcode:
            zipcode = zipcode.split('Code" : "')[1].split('"')[0].strip()
            item_loader.add_value("zipcode", f"VIC {zipcode}")
        else:
            zipcode_v = response.xpath("//meta[contains(@itemprop,'addressRegion')]//@content").get()
            zipcode = response.xpath("//meta[contains(@itemprop,'postalCode')]//@content").get()
            if zipcode_v and zipcode:
                item_loader.add_value("zipcode", zipcode_v + " " + zipcode)
        
        deposit = response.xpath("//text()[contains(.,'BOND') or contains(.,'Bond')]").get()
        if deposit:
            deposit = deposit.lower().split("bond")[1].split(";")[0].replace(":","").strip().replace("$","").replace(",","")
            item_loader.add_value("deposit", deposit)
        
        rent = response.xpath("//div[@class='property-details']//p[@class='price']/text()").get()
        if rent:
            price = rent.split(" ")[0].split("$")[1].strip().replace(",","")
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//li[@class='bedrooms']/span[contains(@class,'num')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[@class='bathrooms']/span[contains(@class,'num')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = " ".join(response.xpath("//div[@class='copy']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            if "polished" not in floor:
                item_loader.add_value("floor", floor)
        
        import dateparser
        available_date = response.xpath("//text()[contains(.,'AVAILABLE')]").get()
        if available_date:
            available_date = available_date.lower().split("available:")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[@class='carspaces']/span[contains(@class,'num')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_xpath("latitude","//meta[@itemprop='latitude']/@content")
        item_loader.add_xpath("longitude","//meta[@itemprop='longitude']/@content")
        
        images = [x for x in response.xpath("//div[@id='gallery']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        item_loader.add_value("landlord_name", "MCINNES")
        item_loader.add_value("landlord_phone", "03 9818 7838")
        item_loader.add_value("landlord_email", "info@mcinnes.com.au")

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