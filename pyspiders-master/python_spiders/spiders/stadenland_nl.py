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
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'stadenland_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://stadenland.nl/woningen/huur/"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//article[@class='woning']"):
            status = item.xpath(".//div[contains(@class,'item-status')]/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://stadenland.nl/woningen/huur/page/{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Stadenland_PySpider_netherlands")
        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//div[@class='omschrijving']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = "".join(response.xpath("//section[contains(@class,'intro')]/div[@class='container']//text()").getall())
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
        
        city = response.xpath("//section[contains(@class,'intro')]/div[@class='container']/p/text()").get()
        if city:
            city = city.strip().split(" ")
            item_loader.add_value("city", city[-1])
            item_loader.add_value("zipcode", city[0]+" "+city[1])
        
        rent = response.xpath(
            "//div[contains(@class,'feature')]//strong[contains(.,'prij')]/following-sibling::span/text()"
        ).get()
        if rent:
            rent = rent.split(",-")[0].split("€")[1].strip()
            item_loader.add_value("rent", rent.replace(".",""))
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath(
            "//div[contains(@class,'feature')]//strong[contains(.,'Woonoppervlakte')]/following-sibling::span/text()"
        ).get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath(
            "//div[contains(@class,'feature')]//strong[contains(.,'slaapkamer')]/following-sibling::span/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        desc = " ".join(response.xpath("//div[@id='omschrijving']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@class='images']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        bathroom_count = response.xpath(
            "//div[contains(@class,'feature')]//strong[contains(.,'badkamer')]/following-sibling::span/text()"
        ).get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        if "deposit:" in desc.lower():
            deposit = desc.lower().split("deposit:")[1].split("month")[0].strip().split(" ")[-1]
            if deposit.isdigit():
                deposit = int(deposit)*int(rent.replace(".",""))
                item_loader.add_value("deposit", deposit)
            elif w2n.word_to_num(deposit):
                deposit = int(w2n.word_to_num(deposit))*int(rent.replace(".",""))
                item_loader.add_value("deposit", deposit)

        external_id = response.xpath("substring-after(//link[@rel='shortlink']/@href,'p=')").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
                
        if "preferred starting date:" in desc:
            available_date = desc.split("preferred starting date:")[1].split(";")[0]
            available_date = available_date.replace("per","").strip()
            if "immediate" in available_date or "direct" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            date_parsed = dateparser.parse(available_date.replace(",",""), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        floor = response.xpath(
            "//div[contains(@class,'feature')]//strong[contains(.,'woonlagen')]/following-sibling::span/text()"
        ).get()
        if floor:
            item_loader.add_value("floor", floor)
        
        floor_plan_images = [x for x in response.xpath("//div[@class='plattegronden']//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        parking = response.xpath("//div[contains(@class,'feature')]//strong[contains(.,'Parkeer')]/following-sibling::span/text()").get()
        garage = response.xpath("//div[contains(@class,'feature')]//strong[contains(.,'Garage')]/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        elif garage:
            if "Parkeerkelder" in garage or "Garagebox" in garage or "Inpandig" in garage:
                item_loader.add_value("parking", True)
            elif "Geen" in garage:
                item_loader.add_value("parking", False)
        
        utilities = response.xpath("//text()[contains(.,'utilities')]").get()
        if utilities and "€" in utilities:
            utilities = utilities.split("€")[1].replace(")","").strip().split(" ")[0]
            if "," in utilities:
                utilities = utilities.split(",")[0]
            item_loader.add_value("utilities", utilities)
        
        item_loader.add_value("landlord_name", "Stad & Land")
        item_loader.add_value("landlord_phone", "010 - 452 666 6")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None