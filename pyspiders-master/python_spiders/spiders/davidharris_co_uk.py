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
from word2number import w2n

class MySpider(Spider):
    name = 'davidharris_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.davidharris.co.uk/Search?listingType=6&obc=Price&obd=Ascending&areainformation=&minprice=&maxprice=&radius=&bedrooms=&page=1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):        
        page = response.meta.get('page', 1)        
        seen = False
        for url in response.xpath("//div[@class='searchResultBoxBg']/figure/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            url = f"https://www.davidharris.co.uk/Search?listingType=6&obc=Price&obd=Ascending&areainformation=&minprice=&maxprice=&radius=&bedrooms=&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = " ".join(response.xpath("//div[@class='descriptionText']//text()").getall()).strip()
        if get_p_type_string(property_type): 
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: 
            return

        item_loader.add_value("external_source", "Davidharris_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        address = response.xpath("//script[contains(.,'<h3>')]//text()").get()
        if address:
            address = address.split(">")[1].split("<")[0]
            city = address.split(",")[-2]
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            

        zipcode = "".join(response.xpath("//title/text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(",")[-1].strip())

        rent = "".join(response.xpath("//div[contains(@class,'container')]//a[contains(@class,'fees')]//parent::div/text()").getall())
        if rent:
            if "let agreed" in rent.lower():
                return
            if "pw" in rent.lower():
                price = rent.split("£")[1].split(" ")[0].replace(",","")
                price = int(price)*4
            else:
                price = rent.split("£")[1].split(" ")[0].replace(",","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'descriptionText')]/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)


        room_count = response.xpath("//div[contains(@class,'fullDetailTop')]//li[contains(.,'BEDROOM') or contains(.,'Bedroom')]//text()").get()
        if room_count:
            room_count = room_count.lower().replace("double","").split("bed")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except : pass

        bathroom_count = response.xpath("//div[contains(@class,'fullDetailTop')]//li[contains(.,'BATHROOM') or contains(.,'Bathroom') or contains(.,'bathroom')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.lower().split("bath")[0].replace("ensuite","").replace("modern","")
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                try:
                    item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
                except : pass
        
        images = [x for x in response.xpath("//div[contains(@class,'gallery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li//span[contains(.,'PARKING') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        washing_machine = response.xpath("//li//span[contains(.,'WASHING MACHINE')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", "David Harris & Co")
        item_loader.add_value("landlord_phone", "info@davidharris.co.uk")
        item_loader.add_value("landlord_email", "info@davidharris.co.uk")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "conversion" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None