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
import dateparser
from datetime import datetime
from word2number import w2n

class MySpider(Spider):
    name = 'anotherhomeabroad_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.anotherhomeabroad.nl/woningen/"]
    external_source='Anotherhomeabroad_PySpider_netherlands'
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='grid']"):
            follow_url = response.urljoin(item.xpath(".//h2/a/@href").get())
            furnished = item.xpath(".//li[@class='furniture']/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"furnished": furnished})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.anotherhomeabroad.nl/woningen/page/{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//div[@class='content']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", "Anotherhomeabroad_PySpider_netherlands")          
        
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = " ".join(response.xpath("//div[@class='address']//li//text()").getall())
        if address:
            item_loader.add_value("address", address)
        
        zipcode = response.xpath("//div[@class='address']//li[1]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
            
        city = response.xpath("//div[@class='address']//li[2]//text()").get()
        if city:
            item_loader.add_value("city", city)
            
        rent = "".join(response.xpath("//dt[contains(.,'price')]/following-sibling::dd[1]//text()").getall())
        price = ""
        if rent:
            price = rent.split(",-")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//dt[contains(.,'Bedroom')]/following-sibling::dd[1]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//dt[contains(.,'Living')]/following-sibling::dd[1]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        available_date = response.xpath("//li[contains(.,'Available:') or contains(.,'Available from:')]//text()").get()
        if available_date:
            if "direct" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.split(":")[-1].strip(), date_formats=["%d %m %Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        furnished = response.meta.get("furnished")
        if furnished:
            if "Gemeubileerd" in furnished or "Gestoffeerd" in furnished:
                item_loader.add_value("furnished", True)
        
        desc = " ".join(response.xpath("//div[@class='content']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@class='main-slider']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        bathroom_count = "".join(response.xpath("//p//text()[contains(.,'bathrooms:')]").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())
        elif "bathroom" in desc:
            bathroom_count = desc.split("bathroom")[0].strip().split(" ")[-1]
            try:
                    item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except:
                pass
            
        floor = "".join(response.xpath("//p//text()[contains(.,'Floor') or contains(.,'floor')]").getall())
        if floor:
            floor = floor.lower().split("floor")[0].strip().split(" ")[-1]
            if "wood" not in floor and "lamin" not in floor and "parq" not in floor and "•" not in floor:
                item_loader.add_value("floor", floor)
        
        deposit = "".join(response.xpath("//p//text()[contains(.,'Deposit') or contains(.,'Waarborgsom') or contains(.,'deposit')]").getall())
        if deposit:
            depos = ""
            if "euro" in deposit:
                deposit = deposit.split("euro")[1].strip()
                item_loader.add_value("deposit", deposit)              
            elif "€" in deposit:
                deposit = deposit.split("€")[1].strip().replace(",","")
                item_loader.add_value("deposit", deposit)
            elif "month" in deposit:
                depos = deposit.split("month")[0].strip().split(" ")[-1]
            elif "Waarborgsom" in deposit:
                depos = deposit.split("Waarborgsom:")[1].strip().split(" ")[0]
            if depos.isdigit():
                depos = int(depos)*int(price)
            else:
                try:
                    depos = w2n.word_to_num(depos)*int(price)
                except:
                    pass
            if depos:
                item_loader.add_value("deposit", depos)
                
        balcony = response.xpath("//p//text()[contains(.,'Balcony') or contains(.,'balcony') or contains(.,'Balkon') or contains(.,'balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//p//text()[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//p//text()[contains(.,'Elevator') or contains(.,'elevator')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        energy_label = response.xpath("//p//text()[contains(.,'Energielabel')]").get()
        if energy_label:
            energy_label = energy_label.split("Energielabel")[1].split(";")[0].strip()
            item_loader.add_value("energy_label", energy_label)
        
        washing = response.xpath("//p//text()[contains(.,'Washer') or contains(.,'washer') or contains(.,'Washing') or contains(.,'washing')]").get()
        if washing:
            item_loader.add_value("washing_machine", True)
        
        pets_allowed = response.xpath("//p//text()[contains(.,'No pet') or contains(.,'no animal') or contains(.,'no pet') or contains(.,'NO PET')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        
        latitude_longitude = response.xpath("//a[contains(@href,'maps')]/@href").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('/@')[1].split(',')[0]
            longitude = latitude_longitude.split('/@')[1].split(',')[1].split(',')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
                
        item_loader.add_value("landlord_name", "Another Home Abroad BV")
        
        phone = response.xpath("//p/strong[contains(.,'Telefoon')]/parent::p/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        
        email = response.xpath("//p/strong[contains(.,'Email')]/parent::p/text()").get()
        if email:
            item_loader.add_value("landlord_email", email.strip())
        
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