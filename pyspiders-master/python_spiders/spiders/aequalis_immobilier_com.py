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
from python_spiders.helper import ItemClear
import re
class MySpider(Spider):
    name = 'aequalis_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.aequalis-immobilier.com/immobilier/"
    current_index = 0
    other_prop = []
    other_prop_type = []
    def start_requests(self):
        formdata = {
            "moteur[type]": "location",
            "moteur[categorie]": "appartement",
            "moteur[pieces]": "1p",
            "false": "2p",
            "moteur[prix]": "",
            "button": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//li[@class='liste-item-wrapper']/article/@onclick").getall():
            follow_url = response.urljoin(item.split(".href = '")[1].split("'")[0].strip())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:
            if response.meta["property_type"] == "apartment":
                p_url = f"https://www.aequalis-immobilier.com/immobilier/location-type/appartement-categorie/1p-pieces/{page}.html"
            else:
                p_url = ""
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "moteur[type]": "location",
                "moteur[categorie]": self.other_prop[self.current_index],
                "moteur[pieces]": "1p",
                "false": "2p",
                "moteur[prix]": "",
                "button": "",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Aequalis_Immobilier_PySpider_france")

        external_id = "".join(response.xpath("//h2[contains(@class,'detail-header-titre')]/text()").getall())
        if external_id:
            external_id = external_id.split("réf.")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h3[contains(@class,'detail-offre-titre')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//h3[contains(@class,'detail-offre-titre')]/text()").getall())
        if address:
            address = address.strip().split(" ")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        square_meters = "".join(response.xpath("//h3[contains(@class,'detail-offre-titre')]//small//text()").getall())
        if square_meters:
            if "m²" in square_meters:
                square_meters = square_meters.split("m²")[0].strip().split(" ")[-1]
                item_loader.add_value("square_meters", square_meters.strip())
        
        rent = "".join(response.xpath("//p[contains(@class,'detail-offre-prix')]/text()").getall())
        if rent:
            rent = rent.strip().replace("€","").replace(" ","")
            if rent.isdigit():
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = "".join(response.xpath("//li[contains(.,' dépôt de garantie')]//text()").getall())
        if deposit:
            deposit = deposit.split("dépôt de garantie")[0].replace("€","").strip().split(" ")[-1].replace("\u00a0","")
            item_loader.add_value("deposit", deposit)

        utilities = "".join(response.xpath("//li[contains(.,' dont')]//text()").getall())
        if utilities:
            utilities = utilities.split(" dont")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//p[contains(@class,'detail-offre-texte')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'chambre')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = "".join(response.xpath("//h3[contains(@class,'detail-offre-titre')]//small//text()").getall())
            if room_count and "pièce" in room_count:
                room_count = room_count.split("pièce")[0].strip().split(" ")[-1]
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'salle')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'gallery')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'garage') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'terrasse')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        balcony = response.xpath("//li[contains(@class,'detail-offre-prestation')]//text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        elevator = response.xpath("//li[contains(@class,'detail-offre-prestation')]//text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        swimming_pool = response.xpath("//li[contains(@class,'detail-offre-prestation')]//text()[contains(.,'Piscine')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        latitude = response.xpath("//div/@data-latgps").get()
        longitude = response.xpath("//div/@data-longgps").get()
        if latitude:     
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "AEQUALIS IMMOBILIER")
        item_loader.add_value("landlord_phone", "+33 (0)4 93 07 13 47")
        item_loader.add_value("landlord_email", "info@aequalis-immobilier.com")
      
        yield item_loader.load_item()