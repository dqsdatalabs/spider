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

class MySpider(Spider):
    name = 'croissy_immobilier_fr'   
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'  
    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"apartment":"1", "house":"2"}

        for key, value in kwargs.items():
            formdata = {
                "nature": "2",
                "type[]": value,
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "newprogram_delivery_at": "",
                "newprogram_delivery_at_display": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "",
            }
            yield FormRequest("https://www.croissy-immobilier.fr/fr/recherche/",
                            callback=self.parse,
                            formdata=formdata,
                            meta={'property_type': key})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li//a[@class='button']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Croissy_Immobilier_PySpider_france")

        title = "".join(response.xpath("//article/div/h2/text()").getall())
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title",title )
            item_loader.add_value("address", "".join(title.strip().split(" ")[1:]))
            item_loader.add_value("city","".join(title.strip().split(" ")[1:]))

        external_id = response.xpath("//li[contains(.,'Ref.')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip().split(".")[1].strip())

        rent = "".join(response.xpath("//article/div/ul/li[contains(.,'€')]/text()").getall())
        if rent:
            price = rent.replace(" ","").split("€")[0].strip().replace(",",".")
            item_loader.add_value("rent", int(float(price)))
        item_loader.add_value("currency", "EUR")

        deposit = "".join(response.xpath("//div[contains(@class,'legal')]/ul/li[contains(.,'Dépôt de garantie')]/span/text()").getall())
        if deposit:
            dep = deposit.replace(" ","").split("€")[0].strip().replace(",",".")
            item_loader.add_value("deposit", int(float(dep)))

        utilities = "".join(response.xpath("//div[contains(@class,'legal')]/ul/li[contains(.,'Charges')][1]/span/text()").getall())
        if utilities:
            uti = utilities.replace(" ","").split("€")[0].strip().replace(",",".")
            item_loader.add_value("utilities", int(float(uti)))


        square_meters = " ".join(response.xpath("//article/div/ul/li[contains(.,'m²')]").getall()).strip()   
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip())

        bathroom_count = " ".join(response.xpath("//article/div/ul/li[contains(.,'salle de bain')]/text()").getall()).strip()   
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("salle")[0].strip())

        room = ""
        room_count = " ".join(response.xpath("///article/div/ul/li[contains(.,'chambre')]/text()").getall()).strip()   
        if room_count:
            room = room_count.split(" ")[0].strip()
        else:
            room_count = " ".join(response.xpath("//div[contains(@class,'summary')]/ul/li[contains(.,'pièce')]/span/text()").getall()).strip()   
            if room_count:
                room = room_count.split(" ")[0].strip()
        item_loader.add_value("room_count",room )

        description = " ".join(response.xpath("//p[@id='description']/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        images = [ x for x in response.xpath("//div[contains(@class,'show-carousel')]/div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        floor = " ".join(response.xpath("//ul/li[contains(.,'Etage')]/span/text()").getall()).strip()   
        if floor:
            item_loader.add_value("floor", floor.strip().split("/")[0])

        elevator = "".join(response.xpath("//div[contains(@class,'services')]/ul/li[contains(.,'Ascenseur')]/text()").getall())
        if elevator:
            item_loader.add_value("elevator", True)

        Furnished = "".join(response.xpath("//div[contains(@class,'services')]/ul/li[contains(.,'Meublé')]/text()").getall())
        if Furnished:
            item_loader.add_value("furnished", True)

        balcony = "".join(response.xpath("//div[contains(@class,'areas')]/ul/li[contains(.,'Balcon')]/text()").getall())
        if balcony:
            item_loader.add_value("balcony", True)

        parking = "".join(response.xpath("//div[contains(@class,'areas')]/ul/li[contains(.,'Parking')]/text()").getall())
        if parking:
            item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//div[contains(@class,'areas')]/ul/li[contains(.,'Terrasse')]/text()").getall())
        if terrace:
            item_loader.add_value("terrace", True)

        energy_label = " ".join(response.xpath("//div[contains(@class,'diagnostics')]/img[@alt='Énergie - Consommation conventionnelle']/@src").getall()).strip()   
        if energy_label:
            energy_label = energy_label.split("/")[-1]
            if "%" in energy_label:
                energy_label = energy_label.split("%")[0]
            if energy_label > "0":
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        
        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]//text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split("marker_map_2 =")[1]
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(",")[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        zipcode = (response.url).split("-")[-2]
        if zipcode:
            if not zipcode.isdigit():
                zipcode = (response.url).split("-")[-1]
            item_loader.add_value("zipcode",zipcode)

        item_loader.add_value("landlord_phone", "+33 6 62 99 72 13")
        item_loader.add_value("landlord_name", "Sophie POUJOL")
        item_loader.add_value("landlord_email", "poujolsophie@croissy-immobilier.fr")
        

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

