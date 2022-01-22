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
    name = 'gtf_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.gtf.fr/liste-des-biens-loueur"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):

        if response.meta.get("pagination", False):
            data = json.loads(response.body)
            sel = Selector(text=data["html"], type="html")
            for item in sel.xpath("//a[contains(@class,'link__property')]/@href").getall():
                follow_url = response.urljoin(item)
                yield Request(follow_url, callback=self.populate_item)
        else:
            for item in response.xpath("//a[contains(@class,'link__property')]/@href").getall():
                follow_url = response.urljoin(item)
                yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@class,'btn__more-results')]/@class").get()
        if next_page:
            p_url = "https://www.gtf.fr/vaneau-search/search?field_ad_type[eq][]=renting&limit=28&mode=list&offset=30&offset_additional=0&search_page_id=665"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"pagination":True}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Gtf_PySpider_"+ self.country)
        item_loader.add_value("external_link", response.url)
        zipcode_title = response.xpath("//div[@class='informations__main']/h1//text()").get()
        if zipcode_title:
            if "MEUBLÉ" in zipcode_title.upper():
                item_loader.add_value("furnished", True)
            if "A LOUER" in zipcode_title.upper():
                zipcode = zipcode_title.upper().split("A LOUER")[1].strip().split(" ")[0]
                if zipcode.isdigit() and len(zipcode)>4:
                    item_loader.add_value("zipcode", zipcode)
     
        title = "".join(response.xpath("//div[@class='informations__main']/h2//text()").getall())
        property_type = ""
        if get_p_type_string(title):
            property_type = get_p_type_string(title)
            item_loader.add_value("property_type", property_type)
        else:
            return
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            item_loader.add_value("address", title.split(" ")[0])
            item_loader.add_value("city", title.split(" ")[0])

            if "Ref" in title:
                external_id = title.split("Ref")[1].strip()
                item_loader.add_value("external_id", external_id)
                
        rent = response.xpath("//span[@class='price']//text()").get()
        if rent:
            price = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency","EUR")
        
        square_meters = response.xpath("//div[@class='specifications']/div[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split(" ")[0])
        
        room_count = response.xpath("//div[@class='specifications']/div[contains(.,'Chambre')]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        else:
            room_count = response.xpath(
                "//div[@class='specifications']/div[contains(.,'Pièce')]/span/text()").get()
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        desc = "".join(response.xpath("//div[@class='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "salle de bains" in desc:
            bathroom_count = desc.split("salle de bains")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
        
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1]
            floor = floor.replace("ème","").replace("er","").replace("ém","").replace("e","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        images = [ x for x in response.xpath("//picture//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//div[@class='specifications']/div[contains(.,'garantie')]/span/text()").get()
        if deposit and deposit.strip() != '0 €':
            deposit = deposit.split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//div[@class='specifications']/div[contains(.,'Charge')]/span/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().replace(" ","")
            item_loader.add_value("utilities", utilities)
        
        elevator = response.xpath("//div[@class='specifications']/div[contains(.,'Ascenseur')]/span/text()").get()
        if elevator:
            if "Oui" in elevator:
                item_loader.add_value("elevator", True)
        
        latitude = response.xpath("//div/@data-lat").get()
        longitude = response.xpath("//div/@data-lng").get()
        if latitude or longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        landlord_name = response.xpath("//div[@class='informations']//div[@class='name']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        
        landlord_phone = response.xpath("//div[@class='informations']//div[@class='phone']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip().split(" ")[-1])

        yield item_loader.load_item()
    
def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None