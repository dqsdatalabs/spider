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
    name = 'carringtongroup_com'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["https://www.carringtongroup.com/renting/residential-properties-for-lease/"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'listing-item')]/div/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.carringtongroup.com/renting/residential-properties-for-lease/page/{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={
                    "page":page+1,
                }
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Carringtongroup_PySpider_australia")

        prop_type = response.xpath("//li[label[contains(.,'Type')]]/div/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        
        external_id = response.xpath("//li[label[contains(.,'ID')]]/div/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)
        
        rent = response.xpath("//div[@class='suburb-price']/text()").get()
        if rent:
            if "deposit" in rent.lower():
                return
            if "$" in rent:
                rent = rent.split("$")[1].strip().split(" ")[0]
                item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "USD")

        deposit = response.xpath("//li[label[contains(.,'Bond')]]/div/text()").get()
        if deposit:
            deposit =deposit.split("$")[1].strip().replace(",","")
            item_loader.add_value("deposit", deposit)

        address = "".join(response.xpath("//div[@class='suburb-address']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        desc = " ".join(response.xpath("//div[contains(@class,'description ')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[label[contains(.,'Bedroom')]]/div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif prop_type and "studio" in prop_type.lower():
            item_loader.add_value("room_count", "1")
        else:
            if "studio" in desc.lower():
                item_loader.add_value("room_count", "1")
        
        bathroom_count = response.xpath("//li[label[contains(.,'Bathroom')]]/div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//li[label[contains(.,'Garage')]]/div/text()[.!='0'] | //li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        swimming_pool = response.xpath("//li[contains(.,'Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li[label[contains(.,'Available')]]/div/text()").get()
        if available_date:
            if "now" not in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@id='gallery']//@srcset").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)    
        
        landlord_name = response.xpath("//strong/p/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//p[contains(@class,'email')]/a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        
        landlord_phone = "".join(response.xpath("//p[contains(@class,'phone')]/a/text()").getall())
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None