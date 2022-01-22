# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from python_spiders.helper import ItemClear

class MySpider(Spider):
    name = 'immobilier_vauban_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://immobilier-vauban.com/index.php?rubrique=liste-transac&prixmax=1400&type=transac&transac=loc&typedebien=APPARTEMENT&ville=all&proxy=0.01&nbpieces=all",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@id,'startliste')]/div[@class='row']/div/div/a"):
            status = item.xpath("./span/text()").get()
            if status and status.lower() == "loué":
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Immobilier_Vauban_PySpider_france")

        title =" ".join(response.xpath("//h1/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        address = response.xpath("//h1/span/text()").get()
        if address:
            item_loader.add_value("address", address) 
            item_loader.add_value("city", address.split(" - ")[0].strip()) 
      
        room_count = response.xpath("//div[div[.='Nombre de chambres']]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[div[.='Nombre de pièces']]/div[2]/text()")
     
        item_loader.add_xpath("floor", "//div[div[.='Etage']]/div[2]/text()")
        item_loader.add_xpath("external_id", "//div[div[.='Référence']]/div[2]/text()")
        square_meters = response.xpath("//div[div[.='Surf. habitable']]/div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        description = " ".join(response.xpath("//div[@class='content-details']/text()[normalize-space()]").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
            if " salle d" in description:
                bathroom_count = description.split(" salle d")[0].strip().split(" ")[-1].strip()
                if "une" in bathroom_count:
                    item_loader.add_value("bathroom_count", "1")
                elif "deux" in bathroom_count:
                    item_loader.add_value("bathroom_count", "2")
                elif "trois" in bathroom_count:
                    item_loader.add_value("bathroom_count", "3")
        balcony = response.xpath("//div[div[.='Balcon']]/div[2]/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
        terrace = response.xpath("//div[div[.='Terrasse']]/div[2]/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        utilities = response.xpath("//div[div[contains(.,'charges mensuelles')]]/div[2]/text()[normalize-space()]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ",""))       
        deposit = response.xpath("//div[div[.='Dépôt de garantie']]/div[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))

        images = [x.split("url(")[-1].split(")")[0] for x in response.xpath("//div[@class='swiper-container gallery-thumbs']//div[@class='swiper-slide']/@style").getall()]
        if images:
            item_loader.add_value("images", images)
       
        rent =response.xpath("//div[div[.='Loyer']]/div[2]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(" ",""))
        energy_label =response.xpath("//div[contains(@style,'dpe')]/div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label.split(".")[0].strip()))
       
        landlord_name = response.xpath("//div[@class='trombi-details']//p[@class='nom']//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)
            item_loader.add_xpath("landlord_phone", "//div[@class='trombi-details']//p[@class='tel']/a/text()")
      
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