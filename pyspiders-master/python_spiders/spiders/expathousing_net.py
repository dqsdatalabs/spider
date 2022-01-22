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
    name = 'expathousing_net'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://expathousing.net/woningen/"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='item-content']//a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        if page == 2 or seen:
            p_url = f"https://expathousing.net/woningen/page/{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Expathousing_Net_PySpider_netherlands")
        item_loader.add_xpath("title", "//title/text()")

        f_text = " ".join(response.xpath("//article[@class='woning']/div[not(@class)]//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        rent = "".join(response.xpath("//dl//dt[contains(.,'Huurprijs')]/following-sibling::dd[1]/text()").getall())
        if rent:
            price = rent.split(",")[0].strip().replace(",",".").replace(".","")
            item_loader.add_value("rent_string",price.strip())
        else:
            item_loader.add_value("currency","EUR")

        meters = "".join(response.xpath("//dl//dt[contains(.,'Oppervlakte')]/following-sibling::dd[1]/text()").getall())
        if meters:
            item_loader.add_value("square_meters",meters.split("m")[0].strip())

        item_loader.add_xpath("zipcode", "//dl//dt[contains(.,'Postcode')]/following-sibling::dd[1]/text()")
        item_loader.add_xpath("city", "//dl//dt[contains(.,'Plaats')]/following-sibling::dd[1]/text()")
        item_loader.add_xpath("room_count", "//dl//dt[contains(.,'Slaapkamers')]/following-sibling::dd[1]/text()")

        description = " ".join(response.xpath("//article[@class='woning']/div/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [x for x in response.xpath("//div[@class='picture']/a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        address = "".join(response.xpath("//header/h2/text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        balcony = "".join(response.xpath("//dl//dt[contains(.,'Balkon')]/following-sibling::dd[1]/text()").getall())
        if balcony:
            if "yes" in balcony.lower():
                item_loader.add_value("balcony",True)
            elif "no" in balcony.lower():
                item_loader.add_value("balcony",False)

        furnished = "".join(response.xpath("//dl//dt[contains(.,'Interieur')]/following-sibling::dd[1]/text()").getall())
        if furnished:
            item_loader.add_value("furnished",True)


        item_loader.add_value("landlord_phone", "+31 (0)70 4150 150")
        item_loader.add_value("landlord_email", "info@expathousing.net")
        item_loader.add_value("landlord_name", "Expat Housing Servi")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None