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
    name = 'svance_co'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.onthemarket.com/agents/branch/s-vance-and-co-liverpool/properties/?search-type=to-rent&view=grid"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'main-image property-image')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@title,'Next page')]//@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//div[contains(@class,'details-heading')]//h1//text()").get()
        if get_p_type_string(prop_type): item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return

        item_loader.add_value("external_source", "Svance_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])

        title = " ".join(response.xpath("//div[contains(@class,'details-heading')]//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'details-heading')]//h1//following-sibling::p//text()").get()
        if address:
            address = address.replace("),","")
            city = address.split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//span[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//li[contains(.,'Deposit')]//text()").get()
        if deposit:
            if "£" in deposit:
                deposit = deposit.split("£")[1].strip()
                item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@id,'description-text')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(.,'Bedroom')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[contains(@id,'description-text')]//text()[contains(.,'bedroom')]").get()
            if room_count:
                room_count = room_count.split("bedroom")[0].replace("double","").strip().split(" ")[-1]
                item_loader.add_value("room_count", room_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'property-image-carousel')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li[contains(.,'Availability date')]//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                available_date = available_date.split(":")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Lift Access')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        latitude_longitude = response.xpath("//img[contains(@src,'center')]//@src").get()
        if latitude_longitude:
            print(latitude_longitude)
            latitude = latitude_longitude.split('center=')[1].split(',')[0]
            longitude = latitude_longitude.split('center=')[1].split(",")[1].split('&')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "S Vance & Co")
        item_loader.add_value("landlord_phone", "0151 382 8610")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower()):
        return "house"
    else:
        return None