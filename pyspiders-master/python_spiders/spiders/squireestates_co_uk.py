# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'squireestates_co_uk'
    external_source = "Squireestates_Co_PySpider_united_kingdom" 
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    start_urls = ['https://squireestates.co.uk/properties/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@data-type='residential-lettings' and not(@data-av='hide')]"):
            follow_url = response.urljoin(item.xpath("./div/a/@href").get())
            rent = item.xpath("./@data-price").get()
            room = item.xpath("./@data-beds").get()
            title = item.xpath("./@data-title").get()
            lat = item.xpath("./@data-lat").get()
            lng = item.xpath("./@data-lat").get()
            item = {}
            item["rent"] = rent
            item["room_count"] = room
            item["title"] = title
            item["lat"] = lat
            item["lng"] = lng
            
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "item": item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        item = response.meta.get('item')

        item_loader.add_value("title", item["title"])
        item_loader.add_value("rent", item["rent"])
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("room_count", item["room_count"])
        
        address = response.xpath("//h1/text()").get()
        if address:
            zipcode = f"{address.strip().split(' ')[-2]} {address.strip().split(' ')[-1]}"
            city = address.split(zipcode)[0].split(",")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        bathroom_count = response.xpath("//span[contains(.,'Bathroom')]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        latitude=response.xpath("//div[@class='map__marker']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude=response.xpath("//div[@class='map__marker']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
    
        
        parking = response.xpath("//div[@class='c-features__item' and (contains(.,'parking') or contains(.,'garage'))]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        import dateparser
        available_date = response.xpath("//div[@class='c-features__item' and (contains(.,'Available'))]/text()").get()
        if available_date and "now" not in available_date.lower():
            available_date = available_date.split("Available")[1].replace("from","").strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        availablecheck=item_loader.get_output_value("available_date")
        if not availablecheck:
            available= response.xpath("//div[@class='c-features flex flex-wrap -mx-15']//div[contains(.,'Available')]/text()").getall()
            for i in available:
                if "Available" in i:
                    item_loader.add_value("available_date",i.split("From")[-1].strip())


        desc = "".join(response.xpath("//div[contains(@class,'c-content')]//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            print(response.url)
            return
        
        images = [x for x in response.xpath("//div[@class='js-spSlider']//@src").getall()]
        item_loader.add_value("images", images)
        
        furnished = response.xpath("//div[@class='c-features__item' and (contains(.,'Furnished') or contains(.,'furnished'))]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        energy_label = response.xpath("//div[@data-target='EPCs']//@src").get()
        if energy_label and "_" in energy_label:
            energy_label = energy_label.split("_")[-2]
            if energy_label.isdigit():
                item_loader.add_value("energy_label", str(int(energy_label)))
        
        item_loader.add_value("landlord_name","Squire Estates")
        item_loader.add_value("landlord_phone","01442 233533")
        item_loader.add_value("landlord_email","homes@squireestates.co.uk")
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "flatshare" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    elif p_type_string and "single room" in p_type_string.lower():
        return "room"
    elif p_type_string and "bedroom" in p_type_string.lower():
        return "house"
    else:
        return None