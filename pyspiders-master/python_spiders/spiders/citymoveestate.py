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
    name = "citymoveestate"
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    custom_settings = {
        "PROXY_ON": "True",
    }


    def start_requests(self):
        start_urls = [
            {'url': 'https://www.onthemarket.com/agents/branch/city-move-estate-bow/properties/?let-agreed=true&page=0&search-type=to-rent&view=grid', 'property_type': 'apartment'}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 1)
        seen=False
        for item in response.xpath("//li[contains(@class,'property-result')]//div[contains(@class,'media')]//@href[contains(.,'details')]").extract():
            follow_url = f"https://www.onthemarket.com{item}"
            yield Request(url=follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"page={page-1}", f"page={page}")
            yield Request(f_url, callback=self.parse, meta={"page": page+1,'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//div[contains(@class,'details-heading')]//h1//text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        else:
            property_type = response.xpath("//div[contains(@class,'title')]//h1//text()").get()
            if property_type:
                if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
                else: return

        item_loader.add_value("external_source", "Citymoveestate_PySpider_united_kingdom_en")
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])

        title = response.xpath("//div[contains(@class,'details-heading')]//h1//text()").get()
        if title:
            item_loader.add_value("title", title)
        else:
            title = response.xpath("//div[contains(@class,'title')]//h1//text()").get()
            if title:
                item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'details-heading-top')]//p[2]//text()").get()
        if address:
            city = address.split(",")[-2]
            zipcode = address.split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())
        else:
            address = response.xpath("//p[contains(@class,'title-address')]//text()").get()
            if address:
                city = address.split(",")[-2]
                zipcode = address.split(",")[-1]
                item_loader.add_value("address", address.strip())
                item_loader.add_value("city", city.strip())
                item_loader.add_value("zipcode", zipcode.strip())

        rent = response.xpath("//span[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@id,'description-text')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        else:
            desc = " ".join(response.xpath("//div[contains(@class,'description-truncate')]//text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'property-icon property-bedrooms')]//span//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            if get_p_type_string(property_type) == "studio": item_loader.add_value("room_count", "1")
            else:
                room_count = response.xpath("//h1[contains(.,'bedroom')]/text()").get()
                if room_count:
                    room_count = room_count.split("bedroom")[0].strip().split(" ")[-1]
                    if room_count.isdigit(): item_loader.add_value("room_count", room_count)
            
        bathroom_count = response.xpath("//div[contains(@class,'property-icon property-bathrooms')]//span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'property-image-carousel')]//@src | //div[contains(@class,'img')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li[contains(.,'Availability date')]//text()").getall())
        if available_date:
            available_date = available_date.split(":")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        latitude_longitude = response.xpath("//script[contains(.,'center=')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('center=')[1].split("%2C")[0].strip()
            longitude = latitude_longitude.split('center=')[1].split("%2C")[1].split("&")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "City Move Estate")
        item_loader.add_value("landlord_phone", "020 8115 7686")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("flat" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maisonette" in p_type_string.lower() or "house" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None