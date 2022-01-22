# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'gaiaimmobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    start_urls = ['https://www.gaiaimmobilier.com/location/1']  # LEVEL 1

    # 1. FOLLOWING 
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'links-group__link')]/@href").extract():
            follow_url = response.urljoin(item)
            if "location" in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        try:
            page = response.xpath("//ul[@class='pagination__items']/li/a/text()").getall()[-3].strip()
            for i in range(2,int(page)+1):
                url = f"https://www.gaiaimmobilier.com/location/{i}"
                yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type')})
        except: pass
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//div[contains(@class,'main-info__text-block')]//p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            if get_p_type_string(response.url):
                item_loader.add_value("property_type", get_p_type_string(response.url))
            else:
                return
        item_loader.add_value("external_source", "GaiaImmobilier_PySpider_france")
        
        title = response.xpath("//h1/span/text()").get()
        item_loader.add_value("title", title)
        
        zipcode = response.xpath("//div[span[contains(.,'Code postal')]]/span[2]/text()").get()
        item_loader.add_value("zipcode", zipcode)
        
        address = response.xpath("//div[span[contains(.,'Ville')]]/span[2]/text()").get()
        item_loader.add_value("address", f"{address} {zipcode}")
        item_loader.add_value("city", address)
        
        square_meters = response.xpath("//div[span[contains(.,'habitable')]]/span[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//div[span[contains(.,'chambre')]]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[span[contains(.,'pièces')]]/span[2]/text()").get()
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[span[contains(.,'salle')]]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//div[span[contains(.,'Etage')]]/span[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        rent = response.xpath("//div[span[contains(.,'Loyer')]]/span[2]/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//div[span[contains(.,'Dépôt')]]/span[2]/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//div[span[contains(.,'Charges')]]/span[2]/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().replace(" ","")
            item_loader.add_value("utilities", utilities)
        
        furnished = response.xpath("//div[span[contains(.,'Meublé')]]/span[2]/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//div[span[contains(.,'Ascenseur')]]/span[2]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)

        balcony = response.xpath("//div[span[contains(.,'Balcon')]]/span[2]/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[span[contains(.,'Terrasse')]]/span[2]/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True) 
        
        external_id = response.xpath("substring-after(//div[@class='main-info__info-id']/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        desc = "".join(response.xpath("//div[contains(@class,'main-info__text-block')]//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [f"https:{x}" for x in response.xpath("//div[contains(@class,'js-lightbox-swiper')]//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//div/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Gaiaimmobilier")
        item_loader.add_value("landlord_phone", "09 83 37 12 97")
        item_loader.add_value("landlord_email", "contact@gaiaimmobilier.com")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None