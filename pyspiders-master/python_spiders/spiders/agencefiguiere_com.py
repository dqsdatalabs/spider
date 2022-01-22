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
    name = 'agencefiguiere_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agencefiguiere.com/locations-appartements-croissant-1.html",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agencefiguiere.com/locations-maisons+et+villas-croissant-1.html",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//div[contains(@class,'grille-annonce')]/div//a[contains(.,'Plus de détails')]/@onclick").getall():
            seen = True
            follow_url = "https://www.agencefiguiere.com/" + item.split("jalik('")[-1].split("')")[0].strip()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("croissant-" + str(page - 1), "croissant-" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response): 
        item_loader = ListingLoader(response=response)
 
        status = response.xpath("//h1/text()").get()
        if 'garage' in status.lower():
            return

        if response.xpath("//h1[not(a)]/text()[contains(.,'Studio') or contains(.,'studio')]").get(): item_loader.add_value("property_type", "studio")
        else: item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agencefiguiere_PySpider_france")
        ext_id = response.xpath("//span[@class='ann-ref']/text()").get()
        if ext_id:
            item_loader.add_value('external_id', ext_id.strip())

        if response.xpath("//h1[not(a)]/text()[contains(.,'non meublé')]").get(): item_loader.add_value("furnished", False)
        elif response.xpath("//h1[not(a)]/text()[contains(.,'meublé')]").get(): item_loader.add_value("furnished", True)

        title = " ".join(response.xpath("//h1[contains(@class,'fiche-titre')]//text()").getall())
        if title:
            if "place de stationnement" in title:
                return
            if "parking" in title.lower():
                return
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            if "provence" in title.lower():
                item_loader.add_value("address", "Aix-en-Provence")
                item_loader.add_value("city", "Aix-en-Provence")
            elif "duranne" in title.lower():
                item_loader.add_value("address", "Duranne")
                item_loader.add_value("city", "Duranne")
            elif "eguilles" in title.lower():
                item_loader.add_value("address", "Eguilles")
                item_loader.add_value("city", "Eguilles")
            elif "luynes" in title.lower():
                item_loader.add_value("address", "Luynes")
                item_loader.add_value("city", "Luynes")
            elif "cabries" in title.lower():
                item_loader.add_value("address", "Cabries")
                item_loader.add_value("city", "Cabries")
            elif "city center" in title.lower():
                item_loader.add_value("address", "City Center")
                item_loader.add_value("city", "City Center")
            elif "puyricard" in title.lower():
                item_loader.add_value("address", "Puyricard")
                item_loader.add_value("city", "Puyricard")
        
        rent = response.xpath("//span[contains(@class,'ann-prix')]//text()").get()
        if rent:
            rent = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
 
        desc = " ".join(response.xpath("//div[contains(@class,'fiche-desc')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        room_count = response.xpath("//td[contains(.,'chambre')]//following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = " ".join(response.xpath("//h1[contains(@class,'fiche-titre')]//text()").re(r'(\d+)\spièce'))
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        floor = response.xpath("//td[contains(.,'Etage')]//following-sibling::td//text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        square_meters = response.xpath("//td[contains(.,'Surface')]//following-sibling::td//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))

        images = [x for x in response.xpath("//div[contains(@class,'grille-images')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "FIGUIERE L'AGENCE")
        
        landlord_phone = response.xpath("//text()[contains(.,'Tél :')]").get()
        if landlord_phone: item_loader.add_value("landlord_phone", landlord_phone.split(':')[-1].strip().replace(".", " "))
        else: item_loader.add_value("landlord_phone", "04 84 88 51 13")
        
        item_loader.add_value("landlord_email", "lagence@figuiere.com")
        
        if 'parking' in title.lower():
            return
        else:
            yield item_loader.load_item()