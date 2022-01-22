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
    name = 'lloydsres_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.lloydsres.com/properties-to-let']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 12)
        
        seen = False
        for item in response.xpath("//div[@class='eapow-property-thumb-holder']"):
            follow_url = response.urljoin(item.xpath(".//@href").get())
            status = item.xpath(".//img[contains(@alt,'Let STC') or contains(@alt,'Under')]/@alt").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 12 or seen:
            url = f"https://www.lloydsres.com/properties-to-let?start={page}"
            yield Request(url, callback=self.parse, meta={"page": page+12})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        desc = "".join(response.xpath("//div[contains(@class,'desc-wrapper')]//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", "Lloydsres_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("property/")[1].split("-")[0])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = "".join(response.xpath("//h1/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
        zipcode=" ".join(response.xpath("//address/strong/following-sibling::text()").getall())
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(" ")[-2:])

        rent = response.xpath("//h1/small/text()").get()
        if rent:
            rent = rent.split("£")[1].strip().replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//img[contains(@alt,'Bedroom')]/following-sibling::strong[1]/text()").get()
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//img[contains(@alt,'Bathroom')]/following-sibling::strong[1]/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished')][not(contains(.,'Un'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]//text()").get()
        if available_date:
            available_date = available_date.split("Available")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        latitude_longitude = response.xpath("//script[contains(.,'lat:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat: "')[1].split('"')[0]
            longitude = latitude_longitude.split('lon: "')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        images = [x for x in response.xpath("//ul[@class='slides']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        name = response.xpath("//div[@class='span8']//b/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        else:
            item_loader.add_value("landlord_name", "Lloyds Residential")
        
        phone = response.xpath("//b[contains(.,'T:')]/following-sibling::text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        else:
            item_loader.add_value("landlord_phone", "020 7033 9888")
            
        email = response.xpath("//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email", email)
        else:
            item_loader.add_value("landlord_email", "info@lloydsres.com")
            
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and  "room" in p_type_string.lower():
        return "room"
    else:
        return None