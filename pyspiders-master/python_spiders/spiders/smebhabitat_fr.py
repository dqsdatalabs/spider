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
    name = 'smebhabitat_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    # 1. FOLLOWING
    def start_requests(self): 
        formdata = {
            "typeTransaction": "location",
            "BudgetMini": "",
            "BudgetMaxi": "",
            "SurfaceMini": "",
            "SurfaceMaxi": "",
            "NbPiecesMini": "",
            "origin": "form",
            "Carto": "",
            "Ville": "",
            "p": "1",
        }
        yield FormRequest(
            "https://smebhabitat.fr/php/getBiens.php",
            callback=self.parse,
            formdata=formdata
            )
    def parse(self, response):
        data = json.loads(response.body)
        if "biens" in data:
            for item in data["biens"]:
                follow_url = response.urljoin(item["link"])
                prop_type = item["nature"]
                property_type = ""
                if "appartement" in prop_type.lower():
                    property_type = "apartment"
                elif "maison" in prop_type.lower():
                    property_type = "house"
                elif "studio" in prop_type.lower():
                    property_type = "apartment"
                elif "duplex" in prop_type.lower():
                    property_type = "apartment"
                elif "villa" in prop_type.lower():
                    property_type = "house"
                elif "immeuble" in prop_type.lower():
                    property_type = "house"
                if property_type != "":
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type, "item":item})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item = response.meta.get('item')

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Smebhabitat_PySpider_france")
        external_id = item['ref']
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = item['ville']
        if address:
            item_loader.add_value("address", address)

        city = item['ville']
        if city:
            item_loader.add_value("city", city)

        square_meters = item['surface']
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters)))

        rent = item['prix']
        if rent:
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//span[contains(@class,'depot_garantie')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].replace("€","").strip()
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//span[contains(@class,'charges')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0]
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'container')]/text()").getall()).strip()
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        else:
            desc = " ".join(response.xpath("//div[contains(@class,'global-home')]/text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bed')]//parent::div//text()[contains(.,'chambre')]").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//i[contains(@class,'cube')]//parent::div//text()[contains(.,'pièce')]").get()
            if room_count:
                room_count = room_count.strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)
        
        images = [x.split("url('")[1].split("'")[0] for x in response.xpath("//div[contains(@id,'photosDetail')]//@style[contains(.,'background')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//p[contains(@class,'disponible-date')]//text()").getall())
        if available_date and ":" in available_date:
            available_date = available_date.split(":")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        latitude = item['lat']
        if latitude:     
            item_loader.add_value("latitude", latitude)
        longitude = item['long']
        if longitude:     
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "Smeb Habitat")
        item_loader.add_value("landlord_phone", "02 41 20 82 81")
        item_loader.add_value("landlord_email", "smebhabitat.agence@orange.fr")
        yield item_loader.load_item()