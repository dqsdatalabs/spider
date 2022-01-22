# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest 
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'affittoromacasa_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Affittoromacasa_PySpider_italy"
    start_urls = ['https://www.soloaffitti.it/immobili?company=177']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='property']/@data-property-url").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.soloaffitti.it/immobili?company=177&selected_page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//div[@class='detail__title' and contains(.,'Tipologia')]/following-sibling::div/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)

        if "arredato" in title.lower():
            item_loader.add_value("furnished",True)
        
        external_id = response.xpath("//div[@class='detail__title' and contains(.,'Riferimento')]/following-sibling::div/text()").get()
        item_loader.add_value("external_id", external_id)
        
        address = "".join(response.xpath("//div[@class='detail__title' and contains(.,'Zona')]/following-sibling::div//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[0].strip())
        
        square_meters = response.xpath("//div[@class='detail__title' and contains(.,'Superficie')]/following-sibling::div//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//div[@class='detail__title' and contains(.,'Locali')]/following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("(")[0].strip())
        
        floor = response.xpath("//div[@class='detail__title' and contains(.,'Piano')]/following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("°")[0].strip())
        
        energy_label = response.xpath("//div[@class='detail__title' and contains(.,'Energetica')]/following-sibling::div//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        desc = "".join(response.xpath("//div[@class='property-description']//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        elevator = response.xpath("//div[@id='caratteristiche_esterne' and contains(.,'Ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//div[@id='caratteristiche_esterne' and contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        washing_machine = response.xpath("//div[@id='caratteristiche_esterne' and contains(.,'Lavatrice')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        terrace = response.xpath("//div[@id='caratteristiche_esterne' and contains(.,'Terrazzo')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        images = [x.split("'")[1] for x in response.xpath("//div[@class='property-gallery-image']//@style").getall()]
        item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'longitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('longitude:')[1].split(',')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        parking=response.xpath("//div[.='Posti auto']").get()
        if parking:
            item_loader.add_value("parking",True)
        item_loader.add_value("landlord_name", "SoloAffitti")
        item_loader.add_value("landlord_phone", "0643597522")
        item_loader.add_value("landlord_email", "roma13@soloaffitti.it")

        item_loader.add_value("currency","EUR")

        rent = response.xpath("//div[@class='property-price']/text()").get()
        if rent:
            rent = rent.split(",")[0].strip("€ ")
            item_loader.add_value("rent",rent)

        utilities = response.xpath("//div[text()='Spese mensili']/following-sibling::div/div/text()").get()
        if utilities:
            utilities = utilities.split(".")[0]
            item_loader.add_value("utilities",utilities)

        bathroom_count = response.xpath("//div[text()='Locali']/following-sibling::div/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("Bagni:")[-1].split(",")[0]
            item_loader.add_value("bathroom_count",bathroom_count)
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower() or "mansarda" in p_type_string.lower()):
        return "house"
    else:
        return None