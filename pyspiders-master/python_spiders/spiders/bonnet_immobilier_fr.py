# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from html.parser import HTMLParser
import dateparser
import re

class MySpider(Spider):
    name = 'bonnet_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr' 
    external_source='Bonnetimmobilier_PySpider_france_fr'
    url = "https://www.bonnet-immobilier.fr/location/1"

    def start_requests(self):
        yield Request(url=self.url,
                        callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//button[contains(.,'Détails')]//@data-url").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item,
            )
            seen = True

        if page == 2 or seen:
            f_url = response.url.replace(f"location/{page-1}", f"location/{page}")
            yield Request(
                url=f_url,
                callback=self.parse,
                meta={"page":page+1},
            )
           
        
        
#     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.url
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Bonnetimmobilier_PySpider_"+ self.country + "_" + self.locale)
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        address = response.xpath("//li[contains(@class,'data')]//text()[contains(.,'Ville')]").get()
        if address:
            address = address.split(":")[1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        zipcode = response.xpath("//li[contains(@class,'data')]//text()[contains(.,'Code')]").get()
        if zipcode:
            zipcode = zipcode.split(":")[1].strip()
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//li[contains(@class,'data')]//text()[contains(.,'Surface')]").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].strip().split(" ")[0]
            square_meters = str(int(float(square_meters)))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[contains(@class,'data')]//text()[contains(.,'chambre')]").get()
        if room_count:
            room_count = room_count.split(':')[-1].strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(@class,'data')]//text()[contains(.,'pièce')]").get()
            if room_count:
                room_count = room_count.split(':')[-1].strip()
                item_loader.add_value("room_count", room_count)

        rent = response.xpath("//p[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.replace("€","").strip().replace(" ","")
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//p[contains(@class,'ref')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[-1].strip()
            item_loader.add_value("external_id", external_id)
        
        desc = " ".join(response.xpath("//div[contains(@class,'offreContent')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//ul[contains(@class,'slider_Mdl')]//li//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//li[contains(@class,'data')]//text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            deposit = deposit.split(":")[-1].replace("€","").replace(" ","").strip()
            item_loader.add_value("deposit", deposit)

        utilities = "".join(response.xpath("//li[contains(@class,'data')]//text()[contains(.,'Charges')]").extract())
        if utilities:
            utilities = utilities.split(":")[-1].replace("€","").strip()
            item_loader.add_value("utilities", utilities)

        energy_label = response.xpath("//div[contains(@class,'offreContent')]//text()[contains(.,'CLASSE ENERGIE :')]").get()
        if energy_label:
            energy_label = energy_label.split('CLASSE ENERGIE :')[-1].strip()
            if not "NR" in energy_label:
                item_loader.add_value("energy_label", energy_label)

        furnished = response.xpath("//li[contains(@class,'data')]//text()[contains(.,'Meublé ')][not(contains(.,'NON'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        terrace = response.xpath("//li[contains(@class,'data')]//text()[contains(.,'Terrasse')][not(contains(.,'NON'))]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        floor = response.xpath("//li[contains(@class,'data')]//text()[contains(.,'Etage')]").get()
        if floor:
            floor = floor.split(":")[-1].strip()
            item_loader.add_value("floor", floor)

        latitude_longitude = response.xpath("//script[contains(.,'lat :')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat :')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Bonnet Immobilier")
        item_loader.add_value("landlord_phone", "04 89 41 08 97")
        item_loader.add_value("landlord_email", "bonnet.immobilier@wanadoo.fr")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("studio" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() ):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data