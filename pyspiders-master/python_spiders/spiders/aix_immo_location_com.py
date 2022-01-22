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

class MySpider(Spider):
    name = 'aix_immo_location_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "http://www.aix-immo-location.com/fr/recherche/"
    current_index = 0
    other_prop = ["2"]
    other_type = ["house"]
    def start_requests(self):
        formdata = {
            "nature": "2",
            "type[]": "1",
            "price": "",
            "age": "",
            "tenant_min": "",
            "tenant_max": "",
            "rent_type": "",
            "currency": "EUR",
            "homepage": "",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for url in response.xpath("//li[@class='ad']/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//li[@class='nextpage']/a/@href").get():
            p_url = f"http://www.aix-immo-location.com/fr/recherche/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "nature": "2",
                "type[]": self.other_prop[self.current_index],
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "currency": "EUR",
                "homepage": "",
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
        item_loader.add_value("external_source", "Aix_Immo_Location_PySpider_france")
        property_type = response.meta["property_type"]
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            if "studio" in title.lower():
                property_type = "studio"

        item_loader.add_value("property_type", property_type)
        external_id = response.xpath("//p[@class='comment']//span[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref.")[-1].strip())
      
        city = response.xpath("//div[@class='path']//span/text()").get()
        if city:
            address = " ".join(city.split(" ")[1:])
            item_loader.add_value("city", address.strip())
            item_loader.add_value("address", address.strip())
        floor = response.xpath("//li[text()='Etage ']/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0].strip())
        room_count = response.xpath("//li[contains(.,'Chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Chambre")[0])
        else:
            room_count = response.xpath("//li[text()='Pièces ']/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("pièce")[0])
        bathroom_count = response.xpath("//li[contains(.,'Salle de douche') or contains(.,'Salle de bain')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Salle")[0])
        description = "".join(response.xpath("//p[@class='comment']//text()[normalize-space()]").getall())
        if description:
            item_loader.add_value("description", description.strip())
        square_meters = response.xpath("//li[text()='Surface ']/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
        rent = response.xpath("//h2[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//li[text()='Dépôt de garantie']/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(",")[0].replace(" ",""))
        utilities = response.xpath("//li[text()='Charges']/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        parking=response.xpath("//h3[.='Surfaces']/following-sibling::ul//li/text()").getall()
        for i in parking:
            if "parking" in i.lower():
                item_loader.add_value("parking",True)

        available_date = response.xpath("//li[text()='Disponible le ']/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
       
        energy = response.xpath("//img[@alt='Énergie - Consommation conventionnelle']/@src").get()
        if energy:
            energy_label = energy.split("/")[-1]
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        images = [x for x in response.xpath("//div[contains(@class,'show-carousel owl-carousel')]/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)  
        item_loader.add_value("landlord_name", "AIX IMMO LOCATION")
        item_loader.add_value("landlord_phone", "+33 (0)4 42 21 12 12")

        furnished = response.xpath("//li[.='Meublé']//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        elevator = response.xpath("//li[.='Ascenseur']//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        washing_machine = response.xpath("//li[.='Lave-linge']//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        dishwasher = response.xpath("//li[.='Lave-vaisselle']//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        terrace = response.xpath("//li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
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