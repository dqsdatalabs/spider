# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re
import dateparser
from datetime import datetime


class MySpider(Spider):
    name = 'immojeanselme_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Immojeanselme_PySpider_france_fr"
    def start_requests(self):
        start_urls = [
            {"url": "https://www.immojeanselme.com/location-maisons-croissant-1.html", "property_type": "house"},
            {"url": "https://www.immojeanselme.com/location-appartements-croissant-1.html", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='grille-annonce--l2']/div//a[contains(.,'savoir')]/@onclick").extract():
            url = item.split("jalik('")[1].split("');")[0]
            follow_url = response.urljoin(url)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        price = response.xpath("//span[@class='ann-prix']/span/text()").extract_first()
        if price:
            price=price.split(",")[0]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        external_id = response.xpath("//span[@class='ann-ref']/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        room_count = response.xpath("//tr/td[contains(.,'chambre')]/following-sibling::td/text()").extract_first()
        if room_count:
            room = room_cout_calculate(room_count)
            item_loader.add_value("room_count", str(room))
        else:
            room_count = response.xpath("//h1[@class='fiche-titre titre1 vsy']/text()").get()
            if room_count:
                room_count = room_count.lower().split('piece')[0].strip().split(' ')[-1].strip()
                if room_count.isnumeric():
                    item_loader.add_value("room_count", room_count)
            else:
                room_count = room_count.lower().split('chbr')[0].strip().split(' ')[-1].strip()
                if room_count.isnumeric():
                    item_loader.add_value("room_count", room_count)

        square = response.xpath("//tr/td[contains(.,'Surface')]/following-sibling::td/text()").extract_first()
        if square:
            square_meters = math.ceil(float(square.strip("m²")))
            item_loader.add_value("square_meters", square_meters)

        utilities = response.xpath("//tr/td[contains(.,'Charges')]/following-sibling::td/text()").extract_first()
        if utilities :
            item_loader.add_value("utilities",utilities.split("€")[0].strip() )

        floor = response.xpath("//tr/td[contains(.,'Etage')]/following-sibling::td/text()").extract_first()
        if floor :
            if "ème" in floor:
                floor = floor.split("ème")[0]
            item_loader.add_value("floor", floor )
        
        bathroom = response.xpath("//tr/td[contains(.,'Salle d')]/following-sibling::td/text()").extract_first()
        if bathroom :
            item_loader.add_value("bathroom_count", bathroom)
            
        desc = "".join(response.xpath("//div[contains(@class,'fiche-desc')]/p//text()").extract())
        if desc:
            item_loader.add_value("description", re.sub("\s{2,}", " ", desc))
        else:
            desc = "".join(response.xpath("//div[contains(@class,'fiche-desc')]//text()").getall())
            if desc:
                item_loader.add_value("description", re.sub("\s{2,}", " ", desc))

        city = response.xpath("//tr/td[contains(.,'Ville')]/following-sibling::td/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address",city.strip())
       
        energy = response.xpath("//div[@id='curseur_consommation']/div/span[1]/text()").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy)
        
        furnished = response.xpath("//tr/td[contains(.,'Meublé')]/following-sibling::td/text()").extract_first()
        if furnished:
            if "non" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//tr/td[contains(.,'Ascenceur')]/following-sibling::td/text()").extract_first()
        if elevator:
            if "non" in elevator:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        
        garage = response.xpath("//tr/td[contains(.,'Parking') or contains(.,'Garage')]/following-sibling::td/text()").extract_first()
        if garage:
            item_loader.add_value("elevator", True)

        balcony = response.xpath("//tr/td[contains(.,'Balcon')]/following-sibling::td/text()").extract_first()
        if balcony:
            if "non" in balcony:
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        images = [response.urljoin(x) for x in response.xpath("//div[@class='bloc--images']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      

        parking = response.xpath("//tr/td[contains(.,'Garage / Parking')]/following-sibling::td/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        available_date = response.xpath("//tr/td[contains(.,'Date')]/following-sibling::td/text()").get()
        if available_date:
            available_date = available_date.replace("Début","").strip()
            if "immediate" in available_date or "LIBRE" in available_date:
                available_date = datetime.now().strftime("%Y-%m-%d")
                item_loader.add_value("available_date", available_date)
            else:
                date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
        
        item_loader.add_value("landlord_phone", "04 75 44 35 36")
        item_loader.add_value("landlord_email", "nathalie@immojeanselme.com")
        item_loader.add_value("landlord_name", "CABINET JEANSELME")
        yield item_loader.load_item()
def room_cout_calculate(room_value):
    if len(room_value.split(" "))>1:
        add=0
        for x in room_value.split(" "):
            if x.isdigit():
                add+=int(x)
        return add
    else:
        return room_value