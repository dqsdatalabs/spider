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
    name = 'locaprom_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.locaprom.com/fr/listing.html?loc=location&type%5B%5D=appartement&surfacemin=&prixmax=&tri=prix-asc&page=1&coordonnees=&supplementaires=0&prixmin=&surfacemax=&terrain=&numero=&idpers=&options=&telhab=&piecemin=", 
                "property_type": "apartment"
            },
            {
                "url": "https://www.locaprom.com/fr/listing.html?loc=location&type%5B%5D=maison&surfacemin=&prixmax=&tri=prix-asc&page=1&coordonnees=&supplementaires=0&prixmin=&surfacemax=&terrain=&numero=&idpers=&options=&telhab=&piecemin=", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//article[@class='item-listing']/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        if page == 2 or seen:
            follow_url = response.url.replace("&page=" + str(page - 1), "&page=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), 'page': page + 1})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Locaprom_PySpider_france")
        title = "".join(response.xpath("//h1[@class='titre']//text()[normalize-space()]").getall())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        external_id = response.xpath("//li[@class='c_numero']/span[2]/span[@class='champ']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        city = response.xpath("//li[@class='c_ville']//span[@class='champ']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address", city.strip())
     
        description = "".join(response.xpath("//div[@id='descdetail']//div[@class='col-sm-8']/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        room_count = response.xpath("//li[@class='c_chambre']//span[@class='champ']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[@class='c_piece']//span[@class='champ']/text()")

        square_meters = response.xpath("//li[@class='c_surface']//span[@class='champ']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.replace(",",".").strip())))
    
        utilities = response.xpath("//li[@class='c_chargesannuelles']//span[@class='champ']/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        floor = response.xpath("//li[@class='c_etage']//span[@class='champ']/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        deposit = response.xpath("//div[@class='info_prix-hai']/text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1].replace("\xa0",""))
        rent_string = response.xpath("//li[@class='c_prix']//span[@class='champ']/text()").get()
        if rent_string:
            item_loader.add_value("rent_string", rent_string.replace("\xa0",""))
        energy_label = response.xpath("//div[@id='dpedetail']//img/@src[contains(.,'/dpe_')]").get()
        if energy_label:
            energy = energy_label.split("/dpe_")[-1].split(".")[0]
            if energy.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy))
        item_loader.add_value("landlord_name", "LOCAPROM")
        item_loader.add_value("landlord_phone", "05 63 71 81 31")
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label