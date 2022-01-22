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
    name = "pmestates"
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.pmestates.com/properties/?search=&min-price=0&max-price=999999999&min-beds=0&max-beds=99999",},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//a[contains(@class,'item-link')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        
        if page ==2 or seen:        
            f_url = f"https://www.pmestates.com/properties/page/{page}/?search&min-price=0&max-price=999999999&min-beds=0&max-beds=99999"
            yield Request(f_url, callback=self.parse, meta={"page": page+1})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath("//div[contains(@class,'property-meta')]//div[contains(@class,'icon-house')]//text()").get().strip()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "pmestates_com_PySpider_united_kingdom_en")

        title = response.xpath("//div[contains(@class,'grid single-header')]//h2//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'grid single-header')]//h2//text()").get()
        if address:
            city = address.split(",")[0].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[contains(@class,'grid single-header')]//div[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.replace("£","").strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'grid')]//div[contains(@class,'content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(@class,'bed')]//text()[.!=' Bedrooms: 0']").get()
        if room_count:
            room_count = room_count.split(":")[1].strip()
            if room_count > "0":
                item_loader.add_value("room_count", room_count)
        elif get_p_type_string(property_type) == "studio" or get_p_type_string(property_type) == "room":
            item_loader.add_value("room_count", "1")
        bathroom_count = response.xpath("//li[contains(@class,'bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(":")[1].strip()
            if bathroom_count > "0":
                item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x.split("url('")[1].split("'")[0] for x in response.xpath("//div[contains(@class,'single-gallery')]//@style[contains(.,'background')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'grid single-header')]//p[contains(@class,'availability')]//text()").getall())
        if available_date:
            available_date = available_date.split(":")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'Garage') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        washing_machine = response.xpath("//li[contains(.,'Washing Machine')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", "Peter Michael Estates")
        item_loader.add_value("landlord_phone", "0208886 4050")
        item_loader.add_value("landlord_email", "info@pmestates.com")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "semi" in p_type_string.lower() or "villa" in p_type_string.lower() or "terraced" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"   
    else:
        return None

        

