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
    name = 'lesclesdusoleil_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    post_url = "https://www.lesclesdusoleil.com/immobilier/"
    current_index = 0
    other_prop = ["maison"]
    other_type = ["house"]
    def start_requests(self):
        formdata = {
            "moteur[type]": "location",
            "moteur[categorie]": "appartement",
            "moteur[surface_min]": "",
            "moteur[prix]": "",
            "button": "",
            "mensualite": "",
            "apport": "",
            "duree": "25",
            "taux": "2,5",
            "calcul": "",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})
      
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for url in response.xpath("//a[@class='center-block']/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//a[@aria-label='Next']/@href").get():
            p_url = f"https://www.lesclesdusoleil.com/immobilier/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "moteur[type]": "location",
                "moteur[categorie]": self.other_prop[self.current_index],
                "moteur[surface_min]": "",
                "moteur[prix]": "",
                "button": "",
                "mensualite": "",
                "apport": "",
                "duree": "25",
                "taux": "2,5",
                "calcul": "",
            }
            yield FormRequest(self.post_url,
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': self.other_type[self.current_index],})
            self.current_index += 1

                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Lesclesdusoleil_PySpider_france")

        external_id = response.xpath("//h3[contains(@class,'contact-form')]//text()").get()
        if external_id:
            external_id = external_id.split("réf.")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            address = title.split(" ")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
            item_loader.add_value("title", title)

        rent = "".join(response.xpath("//div[contains(@class,'Prix')]//span/text()").getall())
        if rent:
            rent = rent.strip().replace("€","").replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//p//text()[contains(.,'depot de garantie')]").get()
        if deposit:
            deposit = deposit.strip().split(":")[1].replace("€","").replace(" ","")
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'contenuetxt')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//small[contains(.,'chambre')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        meters = "".join(response.xpath("//div[@class='row contenuetxt']/div/h3/text()").getall())
        if meters:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",meters.replace(",",""))
            item_loader.add_value("square_meters", unit_pattern)

        bathroom_count = response.xpath("//small[contains(.,'salle')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'gallery')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'setCentre')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('setCentre(')[2].split(',')[0]
            longitude = latitude_longitude.split('setCentre(')[2].split(",")[1].split(',')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'detail-offre-contact-agence')]//h3//strong//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        landlord_phone = response.xpath("//i[contains(@class,'phone')]//parent::li/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1].strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        parking = "".join(response.xpath("//div[contains(@class,'detail-offre-liste-caracteristiques')]/div/small[contains(.,'parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking",True)

        yield item_loader.load_item()